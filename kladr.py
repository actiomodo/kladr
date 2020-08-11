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
    
    def set(self, value):
        """ decomposition code field of street_tbl """
        self.region = value[:2]
        self.district = value[2:5]
        self.town = value[5:8]
        self.locality = value[8:11]
        self.street = value[11:15]
        self.actuality = value[15:]
        self.level = '5'
        return self
    
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
        
    def getCode(self):
        return ''.join([self.region, self.district, self.town, self.locality,
                         self.actuality
                         ])
        
    def set(self, value):
        """ decomposition code field of kladr_tbl """
        self.region = value[:2]
        self.district = value[2:5]
        self.town = value[5:8]
        self.locality = value[8:11]
        self.actuality = value[11:]
        self.level = max(['0', '1'][int(self.region) > 0],
                         ['0', '2'][int(self.district) > 0],
                         ['0', '3'][int(self.town) > 0],
                         ['0', '4'][int(self.locality) > 0],
                         )
        return self

def getInfoKladr():
    """ query data """
    query = """
        SELECT *
        FROM kladr_tbl
        LIMIT 1
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
            print(storeDataKladr(row['code']))
            dataKladr = kladrDataClass()
            dataKladr.town='111'
            print(dataKladr)
            print(dataKladr.getCode())
            dataKladr.set(row['code'])
            print(dataKladr)
            print(dataKladr.getCode())
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
        LIMIT 5
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
            print(storeDataStreet(row['code']))
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
                'region' : dataKladr.region, 
                'district' : dataKladr.district, 
                'town' : dataKladr.town, 
                'locality' : dataKladr.locality, 
                'actuality' : dataKladr.actuality, 
                'level' : dataKladr.level
            }
            queryInsert = """
                INSERT INTO kladr_code_tbl 
                (code, region, district, town, locality, actuality, level)
                VALUES (%(code)s, %(region)s, %(district)s, %(town)s, 
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
    getInfoKladr()
    #getInfoStreet()
    #codeKladrDecomposition()