import psycopg2
from psycopg2.extras import RealDictCursor

from config import configDbConnection
import pysnooper

import logging
import logging.config
import yaml

with open('l_config.yaml', 'r') as f:
    config = yaml.safe_load(f.read())
    logging.config.dictConfig(config)

logger = logging.getLogger(__name__)

"""
import psycopg2.extensions
class LoggingCursor(psycopg2.extensions.cursor):
    def execute(self, sql, args=None):
        logger = logging.getLogger('sql_debug')
        logger.info(self.mogrify(sql, args))
        try:
            psycopg2.extensions.cursor.execute(self, sql, args)
        except Exception as exc:
            logger.error("%s: %s" % (exc.__class__.__name__, exc))
            raise
conn = psycopg2.connect(DSN)
cur = conn.cursor(cursor_factory = LoggingCursor)
"""

def executeCommandWithParameters(commands):
    """ execute command """
    conn = None
    try:
        # read the connection parameters
        params = configDbConnection()
        # connect to the PostgreSQL server
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        # create table one by one
        for command in commands:
            cur.execute(command[0], command[1])
        # close communication with the PostgreSQL database server
        cur.close()
        # commit the changes
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

#@pysnooper.snoop()
def createLofInfo(command):
    command = command.strip()
    showInfo = command[:40]
    commandParts = command.split('DROP TABLE IF EXISTS')
    if len(commandParts) > 1:
        showInfo = ''.join(['DROP TABLE', commandParts[1]])
    commandParts = command.split('CREATE INDEX')
    if len(commandParts) > 1:
        showInfo = ' '.join(['CREATE INDEX', commandParts[1].split()[0]])
    commandParts = command.split('CREATE TABLE')
    if len(commandParts) > 1:
        showInfo = ' '.join(['CREATE TABLE', commandParts[1].split()[0].strip()])
    commandParts = command.split('ALTER TABLE')
    if len(commandParts) > 1:
        showInfo = ' '.join([command.replace('\n','').split('ADD')[0].strip(), 
                             'ADD', command.split('ADD')[1].strip()])
    commandParts = command.split('UPDATE')
    if len(commandParts) > 1:
        showInfo = ' '.join([command.replace('\n','').split('SET')[0].strip(), 
                             'SET', command.split('SET')[1].strip()])
    commandParts = command.split('SELECT')
    if len(commandParts) > 1:
        distinct = ['DISTINCT', ''][command.find('DISTINCT') == -1]
        if command.find('INTO') != -1:
            into = ' '.join(['INTO', command.split('INTO')[1].split()[0].strip()])
        into = [into, ''][command.find('INTO') == -1]
        showInfo = ' '.join(['SELECT', distinct, '...', into, 
                             'FROM', command.split('FROM')[1].split()[0].strip()])
    return showInfo
    
#@pysnooper.snoop()
def executeCommand(commands):
    """ execute command """
    conn = None
    try:
        # read the connection parameters
        params = configDbConnection()
        # connect to the PostgreSQL server
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        # create table one by one
        for command in commands:
            logger.debug(f'{createLofInfo(command)}')
            cur.execute(command)
            logger.debug(f'{command.split()[0]} - Ok')
        # close communication with the PostgreSQL database server
        cur.close()
        # commit the changes
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            
def connect():
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # read connection parameters
        params = configDbConnection()

        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)

        # create a cursor
        cur = conn.cursor()

        # execute a statement
        print('PostgreSQL database version:')
        cur.execute('SELECT version()')
        
        # custom query
        """
        cur.execute('SELECT count(*) FROM street_tbl LIMIT 5')
        """

        # display the PostgreSQL database server version
        db_version = cur.fetchone()
        print(db_version)

        # close the communication with the PostgreSQL
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')

def iterRow(cursor, size=10):
    while True:
        rows = cursor.fetchmany(size)
        if not rows:
            break
        for row in rows:
            yield row

    
def getInfo(query):
    """ query data """
    conn = None
    try:
        params = configDbConnection()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        cur.execute(query)
        print("query: ", query)
        print("cursor size: ", cur.rowcount)
        for row in iterRow(cur, 10):
            print(row)
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            
if __name__ == '__main__':
    connect()
    #getInfo(query)