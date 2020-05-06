import configparser
import psycopg2
from  sql_create_table import drop_table_queries,create_table_queries
import logging
import sys

logger = logging.getLogger('create_tables')

def drop_tables(cur, conn):
    """
    Drop all tables in AWS Redshift if they existed previously.
    
    :param cur: Database cursor
    :param conn: Database connection
    """
    logger.debug("Drop tables ...")
    for query in drop_table_queries:
        logger.debug(f'RUN query: {query}')
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Create all tables in AWS Redshift.
    
    :param cur: Database cursor
    :param conn: Database connection
    """
    logger.debug("Create tables ...")
    for query in create_table_queries:
        logger.debug(f'RUN query: {query}')
        cur.execute(query)
        conn.commit()


def main():
    """
    Main program.
    """
    handler = logging.StreamHandler(sys.stdout)
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)
    
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    try:
        drop_tables(cur, conn)
        create_tables(cur, conn)
    except Exception as ex:
        print ("Oops! An exception has occured:", ex)
        print ("Exception TYPE:", type(ex))
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    main()