import connect as db

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
        
    def getCode(self):
        return ''.join([self.region, self.district, self.town, self.locality,
                         self.street, self.actuality
                         ])
        
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
        
    def getRegion(self):
        return ''.join([self.region] + ['0']*15)[:13]
    
    def getDistrict(self):
        return ''.join([self.region, self.district] + ['0']*15)[:13]
    
    def getTown(self):
        return ''.join([self.region, self.district, 
                        self.town] + ['0']*15
                       )[:13]
    
    def getLocality(self):
        return ''.join([self.region, self.district, self.town, 
                        self.locality] + ['0']*15
                       )[:13]
    
    def getOnlyCode(self):
        return ''.join([self.region, self.district, self.town, 
                        self.locality]
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
        params = db.config()
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
        params = db.config()
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
        code_id serial,
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
    """
    conn = None
    try:
        params = db.config()
        conn = db.psycopg2.connect(**params)
        cur = conn.cursor(cursor_factory=db.RealDictCursor)
        cur.execute(query)
        for row in db.iterRow(cur, 10):
            rowValues = [row[desc] for desc in row]
            dataKladr = kladrDataClass()
            dataKladr.set(row['code'])
            valuesToInsert = [row['code'], 
                              dataKladr.region, dataKladr.district, 
                              dataKladr.town, dataKladr.locality, 
                              dataKladr.actuality, dataKladr.level]
            valuesToInsert = {
                'code' : row['code'],
                'onlycode' : dataKladr.getOnlyCode(), 
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
    #db.getInfo(query)
    #getInfoKladr()
    #getInfoStreet()
    codeKladrDecomposition()