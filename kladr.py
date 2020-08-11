import connect as db

from dataclasses import dataclass

@dataclass
class kladrDataClass:
    # rr-ddd-ttt-lll-aa
    region: str     # rr
    district: str   # ddd
    town: str       # ttt
    locality: str   # lll
    actuality: str  # aa
    level: str      # classification level

@dataclass
class streetDataClass:
    # rr-ddd-ttt-lll-ssss-aa
    region: str     # rr
    district: str   # ddd
    town: str       # ttt
    locality: str   # lll
    street: str     # ssss
    actuality: str  # aa
    level: str      # classification level

def storeDataKladr(value):
    """ decomposition code field of kladr_tbl """
    data = kladrDataClass('', '', '', '', '', '')
    data.region = value[:2]
    data.district = value[2:5]
    data.town = value[5:8]
    data.locality = value[8:11]
    data.actuality = value[11:]
    data.level = max(['0', '1'][int(data.region) > 0],
                     ['0', '2'][int(data.district) > 0],
                     ['0', '3'][int(data.town) > 0],
                     ['0', '4'][int(data.locality) > 0],
                     )
    return data

def storeDataStreet(value):
    """ decomposition code field of street_tbl """
    data = streetDataClass('', '', '', '', '', '', '')
    data.region = value[:2]
    data.district = value[2:5]
    data.town = value[5:8]
    data.locality = value[8:11]
    data.street = value[11:15]
    data.actuality = value[15:]
    data.level = '5'
    return data


def getInfoKladr():
    """ query data """
    query = """
        SELECT *
        FROM kladr_tbl
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
            print(storeDataKladr(row['code']))
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
            dataKladr = storeDataKladr(row['code'])
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
    #getInfoKladr()
    #getInfoStreet()
    codeKladrDecomposition()