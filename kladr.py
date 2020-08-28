import connect as db
import psycopg2
from psycopg2.extras import RealDictCursor
import datetime
import sys

from dataclasses import dataclass

@dataclass
class streetDataClass:
    # rr-ddd-ttt-lll-ssss-aa
    region: str = '00'      # rr
    district: str = '000'   # ddd
    town: str = '000'       # ttt
    locality: str = '000'   # lll
    street: str = '0000'    # ssss
    actuality: str = '00'   # aa
    level: str = '0'        # classification level
    
    def set(self, valueCode):
        """ decomposition code field of street_tbl """
        self.region = valueCode[:2]
        self.district = valueCode[2:5]
        self.town = valueCode[5:8]
        self.locality = valueCode[8:11]
        self.street = valueCode[11:15]
        self.actuality = valueCode[15:]
        self.level = '5'
        return self

    def __str__(self):
        return '-'.join([self.region, self.district, self.town, self.locality,
                         self.street, self.actuality, self.level
                         ])
        
    def getCodeWidth(self):
        return 17
    
    def getRegion(self):
        if self.region == '00':
            info = ['0'] * self.getCodeWidth()
        else:
            info = [self.region]
        return ''.join(info + ['0'] * self.getCodeWidth())[:self.getCodeWidth()]
    
    def getDistrict(self):
        if self.district == '000':
            info = ['0'] * self.getCodeWidth()
        else:
            info = [self.region, self.district]
        return ''.join(info + ['0'] * self.getCodeWidth())[:self.getCodeWidth()]
    
    def getTown(self):
        if self.town == '000':
            info = ['0'] * self.getCodeWidth()
        else:
            info = [self.region, self.district, self.town]
        return ''.join(info + ['0'] * self.getCodeWidth())[:self.getCodeWidth()]
    
    def getLocality(self):
        if self.locality == '000':
            info = ['0'] * self.getCodeWidth()
        else:
            info = [self.region, self.district, self.town, self.locality]
        return ''.join(info + ['0'] * self.getCodeWidth())[:self.getCodeWidth()]
    
    def getStreet(self):
        info = [self.region, self.district, self.town, self.locality, self.street]
        return ''.join(info + ['0'] * self.getCodeWidth())[:self.getCodeWidth()]
    
    def getCode(self):
        return ''.join([self.region, self.district, self.town, self.locality,
                         self.street, self.actuality])
        
    def getCodeWithoutActuality(self):
        return self.getCode()[:-2]
        
    def setValue(self, value, indent):
        if not isinstance(value, int):
            return '<?>'
        if not isinstance(indent, int):
            return '<?>'
        return ''.join(['0000', str(int(value))])[-int(indent):]
    
@dataclass
class kladrDataClass:
    # rr-ddd-ttt-lll-aa
    region: str = '00'      # rr
    district: str = '000'   # ddd
    town: str = '000'       # ttt
    locality: str = '000'   # lll
    actuality: str = '00'   # aa
    level: str = '0'        # classification level
    
    def __str__(self):
        return '-'.join([self.region, self.district, self.town, self.locality,
                         self.actuality, self.level
                         ])
        
    def set(self, valueCode):
        """ decomposition code field of kladr_tbl """
        self.region = valueCode[:2]
        self.district = valueCode[2:5]
        self.town = valueCode[5:8]
        self.locality = valueCode[8:11]
        self.actuality = valueCode[11:]
        self.level = max(['0', '1'][int(self.region) > 0],
                         ['0', '2'][int(self.district) > 0],
                         ['0', '3'][int(self.town) > 0],
                         ['0', '4'][int(self.locality) > 0],
                         )

    def getRegion(self):
        return ''.join([self.region] + ['0']*13)[:13]
    
    def getDistrict(self):
        return ''.join([self.region, self.district] + ['0']*13)[:13]
    
    def getTown(self):
        return ''.join([self.region, self.district, self.town] + ['0']*13
                       )[:13]
    
    def getLocality(self):
        return ''.join([self.region, self.district, self.town, 
                        self.locality] + ['0']*13
                       )[:13]
    
    def getCode(self):
        return ''.join([self.region, self.district, self.town, self.locality,
                         self.actuality
                         ])
    
    def getCodeWithoutActuality(self):
        return self.getCode()[:-2]
        
    def setValue(self, value, indent):
        if not isinstance(value, int):
            return '<?>'
        if not isinstance(indent, int):
            return '<?>'
        return ''.join(['0000', str(int(value))])[-int(indent):]
        return self

def getInfoKladr(testing = False):
    """ query data """
    query = """
        SELECT *
        FROM kladr_tbl
        TABLESAMPLE SYSTEM(1) 
        LIMIT 3
    """
    conn = None
    try:
        params = db.configDbConnection()
        conn = db.psycopg2.connect(**params)
        cur = conn.cursor(cursor_factory=db.RealDictCursor)
        cur.execute(query)
        print("query: ", query)
        print("cursor size: ", cur.rowcount)
        for row in db.iterRow(cur, 10):
            rowValues = [row[desc] for desc in row]
            print(rowValues)
            dataKladr = kladrDataClass()
            print(dataKladr.set(row['code']))
            if testing:
                dataKladr = kladrDataClass()
                title = 'new'
                print(f'{title: <12}: {dataKladr}')
                dataKladr.town = dataKladr.setValue('a', 3)
                title = 'town=22'
                print(f'{title: <12}: {dataKladr}')
                title = 'getCode'
                print(f'{title: <12}: {dataKladr.getCode()}')
                dataKladr.set(row['code'])
                title = 'set'
                print(f'{title: <12}: {dataKladr}')
                title = 'getCode'
                print(f'{title: <12}: {dataKladr.getCode()}')
        cur.close()
    except (Exception, db.psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

def getInfoStreet():
    """ query data """
    query = """
        SELECT *
        FROM street_tbl
        TABLESAMPLE SYSTEM(1) 
        LIMIT 5
    """
    query = """
        SELECT *
        FROM street_tbl
        ORDER BY random() 
        LIMIT 5
    """
    conn = None
    try:
        params = db.configDbConnection()
        conn = db.psycopg2.connect(**params)
        cur = conn.cursor(cursor_factory=db.RealDictCursor)
        cur.execute(query)
        #print("query: ", query)
        #print("cursor size: ", cur.rowcount)
        for row in db.iterRow(cur, 10):
            rowValues = [row[desc] for desc in row]
            print(rowValues)
            dataStreet = streetDataClass()
            print(dataStreet.set(row['code']))
        cur.close()
    except (Exception, db.psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

def codeKladrDecomposition():
    commands = (
        """
        DROP TABLE IF EXISTS kladr_code_tbl
        """,
        """ 
        CREATE TABLE kladr_code_tbl (
        id serial,
        code TEXT, 
        onlycode TEXT DEFAULT '', 
        region TEXT DEFAULT '', 
        district TEXT DEFAULT '', 
        town TEXT DEFAULT '', 
        locality TEXT DEFAULT '', 
        actuality TEXT DEFAULT '', 
        level TEXT DEFAULT ''
        )
        """)
    db.executeCommand(commands)
    """ query data """
    query = """
        SELECT code
        FROM kladr_tbl
        LIMIT 5
    """
    query = """
        SELECT code
        FROM kladr_tbl
        LIMIT 5
    """
    conn = None
    try:
        params = db.configDbConnection()
        conn = db.psycopg2.connect(**params)
        cur = conn.cursor(cursor_factory=db.RealDictCursor)
        cur.execute(query)
        for row in db.iterRow(cur, 10):
            rowValues = [row[desc] for desc in row]
            dataKladr = kladrDataClass()
            dataKladr.set(row['code'])
            valuesToInsert = {
                'code' : row['code'],
                'onlycode' : dataKladr.getCodeWithoutActuality(), 
                'region' : dataKladr.getRegion(), 
                'district' : dataKladr.getDistrict(), 
                'town' : dataKladr.getTown(), 
                'locality' : dataKladr.getLocality(), 
                'actuality' : dataKladr.actuality, 
                'level' : dataKladr.level
            }
            queryInsert = """
                INSERT INTO kladr_code_tbl 
                (code, onlycode, region, district, town, locality, actuality, level)
                VALUES (%(code)s, %(onlycode)s, %(region)s, %(district)s, %(town)s, 
                %(locality)s, %(actuality)s, %(level)s)
            """
            db.executeCommandWithParameters([(queryInsert, valuesToInsert)])
        cur.close()
    except (Exception, db.psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

def createStreetList():
    commands = [
        """
        DROP TABLE IF EXISTS moscow_street_list_tbl
        """,
        """ 
        CREATE TABLE moscow_street_list_tbl (
        id serial,
        code TEXT, 
        street TEXT DEFAULT '', 
        index TEXT DEFAULT '' 
        )
        """,
        """
        DROP TABLE IF EXISTS house_list_tbl
        """,
        """ 
        CREATE TABLE house_list_tbl (
        id serial,
        code TEXT, 
        street TEXT DEFAULT '', 
        houses TEXT DEFAULT '' 
        )
        """]
    db.executeCommand(commands)
    """ query data """
    query = """
        SELECT DISTINCT s.code, 
        k1.socr AS region_s, sb1.socrname AS region_sn, k1.name AS region_name, 
        k2.socr AS district_s, sb2.socrname AS district_sn, k2.name AS district_name, 
        k3.socr AS town_s, sb3.socrname AS town_sn, k3.name AS town_name, 
        k4.socr AS locality_s, sb4.socrname AS locality_sn, k4.name AS locality_name, 
        s1.socr AS street_s, sb5.socrname AS street_sn, s1.name AS street_name, 
        s1.index, d.name
        FROM street_code_tbl AS s
            LEFT JOIN street_tbl AS s1 ON s.code = s1.code 
               LEFT JOIN socrbase_tbl AS sb5 ON s1.socr = sb5.scname AND sb5.level = '5' 
            LEFT JOIN kladr_tbl AS k1 ON s.region = k1.code
               LEFT JOIN socrbase_tbl AS sb1 ON k1.socr = sb1.scname AND sb1.level = '1' 
            LEFT JOIN kladr_tbl AS k2 ON s.district = k2.code
               LEFT JOIN socrbase_tbl AS sb2 ON k2.socr = sb2.scname AND sb2.level = '2' 
            LEFT JOIN kladr_tbl AS k3 ON s.town = k3.code
               LEFT JOIN socrbase_tbl AS sb3 ON k3.socr = sb3.scname AND sb3.level = '3' 
            LEFT JOIN kladr_tbl AS k4 ON s.locality = k4.code
               LEFT JOIN socrbase_tbl AS sb4 ON k4.socr = sb4.scname AND sb4.level = '4'
            LEFT JOIN doma_tbl AS d ON s.code = left(d.code, 17)   
        WHERE d.name != '' AND s.code LIKE '77%'
    """
        #LIMIT 1000
    conn = None
    notFound = 0
    try:
        params = db.configDbConnection()
        conn = db.psycopg2.connect(**params)
        cur = conn.cursor(cursor_factory = db.RealDictCursor)
        cur.execute(query)
        """
        try:
            f = open('streets.txt', 'w')
        finally:
            f.close()
        """
        for row in db.iterRow(cur, 10):
            houses = '' #getHousesByStreet(row['code'])
            if False: #len(houses) == 0:
                notFound += 1
            else:
                queries = []
                street = ', '.join([
                    convertToStr(row['region_sn'], row['region_name']),
                    convertToStr(row['district_sn'], row['district_name']),
                    convertToStr(row['town_sn'], row['town_name']),
                    convertToStr(row['locality_sn'], row['locality_name']),
                    convertToStr(row['street_sn'], row['street_name'])
                    ]).replace(',,', ',').replace(', ,', ',')
                street = street.replace(', ,', ',')
                valuesToInsert = {
                    'code' : row['code'],
                    'street' : street, 
                    'houses' : '', #houses,
                    'index': row['index']
                }
                queryInsert = """
                    INSERT INTO house_list_tbl 
                    (code, street, houses)
                    VALUES (%(code)s, %(street)s, %(houses)s)
                """
                #queries.append((queryInsert, valuesToInsert))
                queryInsert = """
                    INSERT INTO moscow_street_list_tbl 
                    (code, street, index)
                    VALUES (%(code)s, %(street)s, %(index)s)
                """
                queries.append((queryInsert, valuesToInsert))
                db.executeCommandWithParameters(queries)
                #with open('streets.txt', 'a') as f:
                #    f.write(''.join([street, '\n']))
        cur.close()
    except (Exception, db.psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
    print(f'notFound: {notFound}')

def isNumberFound(value):
    for v in list(value):
        if v.isdigit():
            return True
    return False

def convertToStr(valueLow, value):
    resLow = [valueLow, ''][valueLow == None]
    resLow = resLow.lower()
    res = [value, ''][value == None]
    ret = res
    resSplit = res.split()
    if resLow not in resSplit:
        if isNumberFound(res) and len(resSplit) > 1:
            ret = ' '.join([resSplit[1], resSplit[0], resLow])
        elif resLow in ['проезд', 'тупик', 'аллея', 'проспект', 'просек', 'бульвар',
                       'квартал', 'переулок', 'шоссе']:
            ret = ' '.join([res, resLow])
        else:
            ret = ' '.join([resLow, res])
    return ret.strip()
    
def getHousesByStreet(code):
    """ query data """
    query = """
        SELECT * FROM doma_tbl 
        WHERE code LIKE %(code)s 
        LIMIT 50
    """
    queryParameters = {'code': ''.join([code, '%'])}
    conn = None
    res = []
    try:
        params = db.configDbConnection()
        conn = db.psycopg2.connect(**params)
        cur = conn.cursor(cursor_factory = db.RealDictCursor)
        cur.execute(query, queryParameters)
        for row in db.iterRow(cur, 10):
            res += row['name'].split(',')
        cur.close()
    except (Exception, db.psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
    return ','.join(res)
    
def codeStreetDecomposition():
    commands = (
        """
        DROP TABLE IF EXISTS street_code_tbl
        """,
        """ 
        CREATE TABLE street_code_tbl (
        id serial,
        code TEXT, 
        onlycode TEXT DEFAULT '', 
        region TEXT DEFAULT '', 
        district TEXT DEFAULT '', 
        town TEXT DEFAULT '', 
        locality TEXT DEFAULT '', 
        street TEXT DEFAULT '', 
        actuality TEXT DEFAULT '', 
        level TEXT DEFAULT ''
        )
        """)
    db.executeCommand(commands)
    """ query data """
    query = """
        SELECT code
        FROM street_tbl
        LIMIT 5
    """
    query = """
        SELECT code
        FROM street_tbl
    """
        #WHERE code LIKE '77%'
    conn = None
    try:
        params = db.configDbConnection()
        conn = db.psycopg2.connect(**params)
        cur = conn.cursor(cursor_factory = db.RealDictCursor)
        cur.execute(query)
        for row in db.iterRow(cur, 10):
            dataStreet = streetDataClass()
            dataStreet.set(row['code'])
            valuesToInsert = {
                'code' : row['code'],
                'onlycode' : dataStreet.getCodeWithoutActuality(), 
                'region' : dataStreet.getRegion(), 
                'district' : dataStreet.getDistrict(), 
                'town' : dataStreet.getTown(), 
                'locality' : dataStreet.getLocality(), 
                'street' : dataStreet.getStreet(), 
                'actuality' : dataStreet.actuality, 
                'level' : dataStreet.level
            }
            queryInsert = """
                INSERT INTO street_code_tbl 
                (code, onlycode, region, district, town, locality, street, actuality, level)
                VALUES (%(code)s, %(onlycode)s, %(region)s, %(district)s, %(town)s, 
                %(locality)s, %(street)s, %(actuality)s, %(level)s)
            """
            db.executeCommandWithParameters([(queryInsert, valuesToInsert)])
        cur.close()
    except (Exception, db.psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

def createDataTables():
    # create command list
    commands = [
		"""
		CREATE EXTENSION IF NOT EXISTS pg_trgm;
		""",
		"""
		CREATE EXTENSION IF NOT EXISTS rum;
		""",
		"""
		create or replace function phoneme (in_lexeme text)
		returns text
		language plpgsql
		immutable
		as $$
		declare
		res varchar(100) DEFAULT '';
		begin
		res := lower(in_lexeme);
		res := regexp_replace(res,'[ъь]','','g');
		res := regexp_replace(res,'(йо|ио|йе|ие)','и','g');
		res := regexp_replace(res,'[оыя]','а','g');
		res := regexp_replace(res,'[еёэ]','и','g');
		res := regexp_replace(res,'ю','у','g');
		res := regexp_replace(res,'б([псткбвгджзфхцчшщ]|$)','п\1','g');
		res := regexp_replace(res,'з([псткбвгджзфхцчшщ]|$)','с\1','g');
		res := regexp_replace(res,'д([псткбвгджзфхцчшщ]|$)','т\1','g');
		res := regexp_replace(res,'в([псткбвгджзфхцчшщ]|$)','ф\1','g');
		res := regexp_replace(res,'г([псткбвгджзфхцчшщ]|$)','к\1','g');
		res := regexp_replace(res,'дс','ц','g');
		res := regexp_replace(res,'тс','ц','g');
		res := regexp_replace(res,'(.)\1','\1','g');
		return res;
		exception when others then 
		raise notice '%', SQLERRM;
		end;
		$$;
		""",
		"""
		create or replace function metaphone (in_phonemes text)
		returns text
		language plpgsql
		immutable
		as $$
		begin
		return (
			select string_agg(q.lex,' ') from (
			select phoneme(lexeme) as lex from unnest(to_tsvector('simple', in_phonemes))
			order by positions
			) as q
		);
		exception when others then
		raise notice '%', SQLERRM;
		end;
		$$;
		""",
        """ 
        DROP TABLE IF EXISTS street_code_tbl;
        """,
        """
        SELECT code,
			CASE
				WHEN substring(code, 1, 2) = '00' THEN repeat('0', 13)
				ELSE concat(substring(code, 1, 2), repeat('0', 11))
			END 
			AS region,
			CASE
				WHEN substring(code, 3, 3) = '000' THEN repeat('0', 13)
				ELSE concat(substring(code, 1, 5), repeat('0', 8))
			END 
			AS district,
			CASE
				WHEN substring(code, 6, 3) = '000' THEN repeat('0', 13)
				ELSE concat(substring(code, 1, 8), repeat('0', 5))
			END 
			AS town,
			CASE
				WHEN substring(code, 9, 3) = '000' THEN repeat('0', 13)
				ELSE concat(substring(code, 1, 11), repeat('0', 2))
			END 
            AS locality
        INTO street_code_tbl
        FROM street_tbl
        """,
        """ 
        DROP TABLE IF EXISTS t_tbl;
        """,
        """ 
        SELECT DISTINCT s.code, s1.index, d.name AS doma,
        replace(replace(concat_ws(', ', concat_ws(' ', lower(sb1.scname), k1.name), 
        concat_ws(' ', lower(sb2.scname), k2.name), 
        concat_ws(' ', lower(sb3.scname), k3.name), 
        concat_ws(' ', lower(sb4.scname), k4.name), 
        concat_ws(' ', lower(sb5.scname), s1.name)), ', ,', ','), ', ,', ',') AS street_full, 
        lower(replace(replace(concat_ws(' ', k1.name, k2.name, k3.name, k4.name, s1.name),
		'  ', ' '), '  ', ' ')) AS street_shot,
        lower(sb1.socrname) AS reg_pr, k1.name AS region, 
        lower(sb2.socrname) AS dis_pr, k2.name AS district, 
        lower(sb3.socrname) AS tow_pr, k3.name AS town, 
        lower(sb4.socrname) AS loc_pr, k4.name AS locality, 
        lower(sb5.socrname) AS str_pr, s1.name AS street,
		metaphone(lower(replace(replace(concat_ws(' ', k1.name, k2.name, k3.name, k4.name, s1.name),
		'  ', ' '), '  ', ' '))) AS street_metaphone
        INTO t_tbl                       
        FROM street_code_tbl AS s
            LEFT JOIN street_tbl AS s1 ON s.code = s1.code 
                LEFT JOIN socrbase_tbl AS sb5 ON s1.socr = sb5.scname AND sb5.level = '5' 
            LEFT JOIN kladr_tbl AS k1 ON s.region = k1.code
                LEFT JOIN socrbase_tbl AS sb1 ON k1.socr = sb1.scname AND sb1.level = '1' 
            LEFT JOIN kladr_tbl AS k2 ON s.district = k2.code
                LEFT JOIN socrbase_tbl AS sb2 ON k2.socr = sb2.scname AND sb2.level = '2' 
            LEFT JOIN kladr_tbl AS k3 ON s.town = k3.code
                LEFT JOIN socrbase_tbl AS sb3 ON k3.socr = sb3.scname AND sb3.level = '3' 
            LEFT JOIN kladr_tbl AS k4 ON s.locality = k4.code
                LEFT JOIN socrbase_tbl AS sb4 ON k4.socr = sb4.scname AND sb4.level = '4'
            LEFT JOIN doma_tbl AS d ON s.code = left(d.code, 17)   
        WHERE d.name != '';
        """,
        """ 
        DROP TABLE IF EXISTS street_code_tbl;
        """,
        """
        DROP TABLE IF EXISTS rus_shot_tbl;
        """,
        """
        SELECT DISTINCT code, 
        index, street_full, street_shot, street_metaphone, reg_pr, region, dis_pr, district,
        tow_pr, town, loc_pr, locality, str_pr, street 
        INTO rus_shot_tbl 
        FROM t_tbl;
        """,
        """
        DROP TABLE IF EXISTS rus_shot_region_tbl;
        """,
           """
           SELECT DISTINCT region
           INTO rus_shot_region_tbl 
           FROM t_tbl;
           """,
              """
              CREATE INDEX trgm_region_idx 
              ON rus_shot_region_tbl 
              USING gin (region gin_trgm_ops);
              """,
        """
        DROP TABLE IF EXISTS rus_shot_district_tbl;
        """,
           """
           SELECT DISTINCT district
           INTO rus_shot_district_tbl 
           FROM t_tbl;
           """,
              """
              CREATE INDEX trgm_district_idx 
              ON rus_shot_district_tbl 
              USING gin (district gin_trgm_ops);
              """,
        """
        DROP TABLE IF EXISTS rus_shot_town_tbl;
        """,
           """
           SELECT DISTINCT town
           INTO rus_shot_town_tbl 
           FROM t_tbl;
           """,
              """
              CREATE INDEX trgm_town_idx 
              ON rus_shot_town_tbl 
              USING gin (town gin_trgm_ops);
              """,
        """
        DROP TABLE IF EXISTS rus_shot_locality_tbl;
        """,
           """
           SELECT DISTINCT locality
           INTO rus_shot_locality_tbl 
           FROM t_tbl;
           """,
              """
              CREATE INDEX trgm_locality_idx 
              ON rus_shot_locality_tbl 
              USING gin (locality gin_trgm_ops);
              """,
        """
        DROP TABLE IF EXISTS rus_shot_street_tbl;
        """,
           """
           SELECT DISTINCT street
           INTO rus_shot_street_tbl 
           FROM t_tbl;
           """,
              """
              CREATE INDEX trgm_street_idx 
              ON rus_shot_street_tbl 
              USING gin (street gin_trgm_ops);
              """,
        """
        DROP TABLE IF EXISTS t_tbl;
        """,
        """
        ALTER TABLE rus_shot_tbl 
        ADD COLUMN tsv tsvector;
        """,
        """
        UPDATE rus_shot_tbl 
        SET tsv = to_tsvector(street_shot);
        """,
		"""
		CREATE INDEX rus_shot_code_idx
		ON rus_shot_tbl (code);
		""",
        """
        CREATE INDEX trgm_idx 
        ON rus_shot_tbl 
        USING gin (street_shot gin_trgm_ops);
        """,
        """
        CREATE INDEX metaphone_trgm_idx 
        ON rus_shot_tbl 
        USING gin (metaphone(street_shot) gin_trgm_ops);
        """,
        """
        CREATE INDEX rum_idx 
        ON rus_shot_tbl
        USING rum (to_tsvector('simple'::regconfig, street_shot) rum_tsvector_ops);
        """,
		"""
		DROP TABLE IF EXISTS td_tbl;
		""",
		"""
		SELECT name AS houses, substring(code, 1, 17) AS code, index
		INTO td_tbl
		FROM doma_tbl;
		""",
		"""
		CREATE INDEX td_code_idx
		ON td_tbl (code);
		""",
		"""
		DROP TABLE IF EXISTS msk_shot_tbl;
		""",
		"""
		SELECT *
		INTO msk_shot_tbl
		FROM rus_shot_tbl
		WHERE region = 'Москва';
		""",
		"""
		CREATE INDEX msk_trgm_idx 
		ON msk_shot_tbl 
		USING gin (street_shot gin_trgm_ops);
		""",
    ]
    # execute command list
    db.executeCommand(commands)
    
if __name__ == '__main__':
    query = """
        SELECT *
        FROM kladr_tbl
        WHERE code LIKE '%00000000000'
        LIMIT 20
    """
    query = """
        SELECT N.socrname, K.name, K.code
        FROM kladr_tbl AS K
        INNER JOIN socrbase_tbl AS N on N.scname = K.socr
        WHERE N.level = '1'
        LIMIT 10
    """
    timeStart = datetime.datetime.today()
    createDataTables()
    print(f'execution time: {datetime.datetime.today() - timeStart}')
    