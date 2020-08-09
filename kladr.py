import connect as db

if __name__ == '__main__':
    db.connect()
    query = """
        SELECT *
        FROM kladr_tbl
        WHERE code LIKE '%00000000000'
        LIMIT 20
    """
    db.get_info(query)