import connect as db

from dataclasses import dataclass

@dataclass
class kladrDataClass:
    region: str     # rr
    district: str   # ddd
    town: str       # ttt
    locality: str   # lll
    updateCode: str # uu
    level: str      # classification level

@dataclass
class streetDataClass:
    region: str     # rr
    district: str   # ddd
    town: str       # ttt
    locality: str   # lll
    street: str     # ssss
    updateCode: str # uu
    level: str      # classification level

def storeDataKladr(value):
    """ decomposition code field of kladr_tbl """
    data = kladrDataClass('', '', '', '', '', '')
    data.region = value[:2]
    data.district = value[2:5]
    data.town = value[5:8]
    data.locality = value[8:11]
    data.updateCode = value[11:]
    data.level = max([0, 1][int(data.region) > 0],
                     [0, 2][int(data.district) > 0],
                     [0, 3][int(data.town) > 0],
                     [0, 4][int(data.locality) > 0],
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
    data.updateCode = value[15:]
    data.level = 5
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
    getInfoStreet()