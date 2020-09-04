import connect as db
import psycopg2
from psycopg2.extras import RealDictCursor
import datetime
import sys
import pprint

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
		res := regexp_replace(res,'б','п','g');
		res := regexp_replace(res,'з','с','g');
		res := regexp_replace(res,'д','т','g');
		res := regexp_replace(res,'в','ф','g');
		res := regexp_replace(res,'г','к','g');
		res := regexp_replace(res,'дс','ц','g');
		res := regexp_replace(res,'тс','ц','g');
		res := regexp_replace(res,'(.)\\1','\\1','g');
		return res;
		exception
		when others then raise exception '%', sqlerrm;
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
        SELECT DISTINCT s.code, s1.index, d.name AS doma, e.sort_order AS sort,
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
            LEFT JOIN estimates_tbl AS e ON e.region_code = left(s.code, 2)
        WHERE d.name != ''
		ORDER BY e.sort_order, street;
        """,
        """ 
        DROP TABLE IF EXISTS street_code_tbl;
        """,
        """
        DROP TABLE IF EXISTS rus_shot_tbl;
        """,
        """
        SELECT DISTINCT code, index, street, street_full, street_shot, street_metaphone, sort
        INTO rus_shot_tbl 
        FROM t_tbl
		WHERE street_shot NOT LIKE '%автодорог%'
		ORDER BY sort, street;
        """,
        """
        DROP TABLE IF EXISTS rus_shot_region_tbl;
        """,
           """
           SELECT DISTINCT region, substring(code, 1, 2) as code
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
        SET tsv = to_tsvector(street_metaphone);
        """,
		"""
		CREATE INDEX rus_shot_code_idx
		ON rus_shot_tbl (code);
		""",
        """
        CREATE INDEX metaphone_trgm_idx 
        ON rus_shot_tbl 
        USING gin (street_metaphone gin_trgm_ops);
        """,
        """
        CREATE INDEX rum_idx 
        ON rus_shot_tbl
        USING rum (to_tsvector('simple'::regconfig, street_metaphone) rum_tsvector_ops);
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
		WHERE left(code, 2) = '77';
		""",
		"""
		CREATE INDEX msk_trgm_idx 
		ON msk_shot_tbl 
		USING gin (street_shot gin_trgm_ops);
		""",
    ]
    # execute command list
    db.executeCommand(commands)
    
def mostRecentEstimates():
	query = """
		SELECT DISTINCT r.region, r.code
		FROM rus_shot_region_tbl AS r
		LEFT JOIN rus_shot_tbl AS s
		ON substring(s.code, 1, 2) = r.code;
	"""
	queryParameters = {}
	rg = [{'rg': 'Москва', 'value': 12678079},
		  {'rg': 'Московская область', 'value': 7690863},
		  {'rg': 'Краснодарский край', 'value': 5675462},
		  {'rg': 'Санкт-Петербург', 'value': 5398064},
		  {'rg': 'Свердловская область', 'value': 4310681},
		  {'rg': 'Ростовская область', 'value': 4197821},
		  {'rg': 'Республика Башкортостан', 'value': 4038151},
		  {'rg': 'Республика Татарстан', 'value': 3902642},
		  {'rg': 'Тюменская область(с ХМАО и ЯНАО)', 'value': 3756536},
		  {'rg': 'Челябинская область', 'value': 3466369},
		  {'rg': 'Нижегородская область', 'value': 3202946},
		  {'rg': 'Самарская область', 'value': 3179532},
		  {'rg': 'Республика Дагестан', 'value': 3110858},
		  {'rg': 'Красноярский край', 'value': 2866255},
		  {'rg': 'Ставропольский край', 'value': 2803573},
		  {'rg': 'Новосибирская область', 'value': 2798170},
		  {'rg': 'Кемеровская область', 'value': 2657854},
		  {'rg': 'Пермский край', 'value': 2599260},
		  {'rg': 'Волгоградская область', 'value': 2491036},
		  {'rg': 'Саратовская область', 'value': 2421895},
		  {'rg': 'Иркутская область', 'value': 2391193},
		  {'rg': 'Воронежская область', 'value': 2324205},
		  {'rg': 'Алтайский край', 'value': 2317153},
		  {'rg': 'Оренбургская область', 'value': 1956835},
		  {'rg': 'Омская область', 'value': 1926665},
		  {'rg': 'Республика Крым', 'value': 1912622},
		  {'rg': 'Приморский край', 'value': 1895868},
		  {'rg': 'Ленинградская область', 'value': 1875872},
		  {'rg': 'Ханты-Мансийский Автономный округ - Югра', 'value': 1674676},
		  {'rg': 'Белгородская область', 'value': 1549151},
		  {'rg': 'Удмуртская Республика', 'value': 1500955},
		  {'rg': 'Чеченская Республика', 'value': 1478726},
		  {'rg': 'Тульская область', 'value': 1466127},
		  {'rg': 'Владимирская область', 'value': 1358416},
		  {'rg': 'Хабаровский край', 'value': 1315643},
		  {'rg': 'Пензенская область', 'value': 1305563},
		  {'rg': 'Кировская область', 'value': 1262402},
		  {'rg': 'Тверская область', 'value': 1260379},
		  {'rg': 'Ярославская область', 'value': 1253389},
		  {'rg': 'Ульяновская область', 'value': 1229824},
		  {'rg': 'Чувашская Республика', 'value': 1217818},
		  {'rg': 'Брянская область', 'value': 1192491},
		  {'rg': 'Вологодская область', 'value': 1160445},
		  {'rg': 'Липецкая область', 'value': 1139371},
		  {'rg': 'Архангельская область (с НАО)', 'value': 1136535},
		  {'rg': 'Рязанская область', 'value': 1108847},
		  {'rg': 'Курская область', 'value': 1104008},
		  {'rg': 'Томская область', 'value': 1079271},
		  {'rg': 'Забайкальский край', 'value': 1059700},
		  {'rg': 'Калининградская область', 'value': 1012512},
		  {'rg': 'Тамбовская область', 'value': 1006748},
		  {'rg': 'Астраханская область', 'value': 1005782},
		  {'rg': 'Калужская область', 'value': 1002575},
		  {'rg': 'Ивановская область', 'value': 997135},
		  {'rg': 'Республика Бурятия', 'value': 985937},
		  {'rg': 'Республика Саха (Якутия)', 'value': 971996},
		  {'rg': 'Смоленская область', 'value': 934889},
		  {'rg': 'Кабардино-Балкарская Республика', 'value': 868350},
		  {'rg': 'Курганская область', 'value': 827166},
		  {'rg': 'Республика Коми', 'value': 820473},
		  {'rg': 'Республика Мордовия', 'value': 790197},
		  {'rg': 'Амурская область', 'value': 790044},
		  {'rg': 'Мурманская область', 'value': 741404},
		  {'rg': 'Орловская область', 'value': 733498},
		  {'rg': 'Республика Северная Осетия — Алания', 'value': 696837},
		  {'rg': 'Республика Марий Эл', 'value': 679417},
		  {'rg': 'Костромская область', 'value': 633385},
		  {'rg': 'Псковская область', 'value': 626115},
		  {'rg': 'Республика Карелия', 'value': 614064},
		  {'rg': 'Новгородская область', 'value': 596508},
		  {'rg': 'Ямало-Ненецкий автономный округ', 'value': 544444},
		  {'rg': 'Республика Хакасия', 'value': 534262},
		  {'rg': 'Республика Ингушетия', 'value': 507061},
		  {'rg': 'Сахалинская область', 'value': 488257},
		  {'rg': 'Карачаево-Черкесская Республика', 'value': 465528},
		  {'rg': 'Республика Адыгея', 'value': 463088},
		  {'rg': 'Севастополь', 'value': 449138},
		  {'rg': 'Республика Тыва', 'value': 327383},
		  {'rg': 'Камчатский край', 'value': 313016},
		  {'rg': 'Республика Калмыкия', 'value': 271135},
		  {'rg': 'Республика Алтай', 'value': 220181},
		  {'rg': 'Еврейская автономная область', 'value': 158305},
		  {'rg': 'Магаданская область', 'value': 140149},
		  {'rg': 'Архангельская область без НАО', 'value': 92424},
		  {'rg': 'Тюменская область без ХМАО и ЯНАО', 'value': 53746},
		  {'rg': 'Чукотский автономный округ', 'value': 50288},
		  {'rg': 'Ненецкий автономный округ', 'value': 44111}
		  ]
	conn = None
	res = []
	try:
		params = db.configDbConnection()
		conn = db.psycopg2.connect(**params)
		cur = conn.cursor(cursor_factory = db.RealDictCursor)
		cur.execute(query, queryParameters)
		#regions = [v['rg'] for v in rg]
		for i, row in enumerate(db.iterRow(cur, 10)):
			region = row['region'].split(' - ')[0].split(' -')[0].split(' /')[0]
			found = False
			for v in rg:
				if v['rg'].find(region) != -1:
					res.append({'region': region,
								'sortOrder': 0,
								'recentEstimates': v['value'],
								'regionCode': row['code']
								})
					found = True
			if not found:
				print(f'not found {region}')
		res.sort(key = lambda x: (-x['recentEstimates']))
		with open('most_recent_estimates.txt', 'w') as f:
			for i, v in enumerate(res):
				v['sortOrder'] = i + 1
				r = v['region']
				s = v['sortOrder']
				e = v['recentEstimates']
				c = v['regionCode']
				f.write(f'{r},{s},{e},{c}\n')
		cur.close()
	except (Exception, db.psycopg2.DatabaseError) as error:
		print(error)
	finally:
		if conn is not None:
			conn.close()
	return res

def getMostRecentEstimates():
	# create command list
	commands = [
		"""
		DROP TABLE IF EXISTS estimates_tbl;
		""",
		"""
		CREATE TABLE estimates_tbl (
			region            varchar(100) NOT NULL,
			sort_order        integer NOT NULL,
			recent_estimates  integer NOT NULL,
			region_code       char(2) NOT NULL
		);
		""",
	]
	# execute command list
	db.executeCommand(commands)
	conn = None
	try:
		# read the connection parameters
		params = db.configDbConnection()
		# connect to the PostgreSQL server
		conn = db.psycopg2.connect(**params)
		cur = conn.cursor()
		# store data from file to database
		with open('most_recent_estimates.txt', 'r') as f:
			cur.copy_from(f, 'estimates_tbl', sep=',',
						  columns=('region', 'sort_order', 'recent_estimates', 'region_code'))
		# close communication with the PostgreSQL database server
		cur.close()
		# commit the changes
		conn.commit()
	except (Exception, db.psycopg2.DatabaseError) as error:
		print(error)
	finally:
		if conn is not None:
			conn.close()

if __name__ == '__main__':
	timeStart = datetime.datetime.today()
	# mostRecentEstimates()
	getMostRecentEstimates()
	createDataTables()
	print(f'execution time: {datetime.datetime.today() - timeStart}')
    