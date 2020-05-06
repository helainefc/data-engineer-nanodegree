import configparser
import psycopg2
from sql_insert_tables import copy_table_queries,insert_table_queries
import logging
import sys

logger = logging.getLogger('etl')

def load_staging_tables(cur, conn):
    """
    Load data into the AWS Redshift staging tables.
    
    :param cur: Database cursor
    :param conn: Database connection
    """
    logger.debug("Load staging tables ...")
    for query in copy_table_queries:
        logger.debug(f'RUN query: {query}')
        cur.execute(query)
        conn.commit()

    print('All files COPIED OK.')

def insert_tables(cur, conn):
    """
    Insert data from staging tables 

    :param cur: Database cursor
    :param conn: Database connection
    """
    logger.debug("Insert tables ...")
    for query in insert_table_queries:
        logger.debug(f'RUN query: {query}')
        cur.execute(query)
        conn.commit()

    print('All files INSERTED OK.')

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
        load_staging_tables(cur, conn)
        insert_tables(cur, conn)
    except Exception as ex:
        print ("Oops! An exception has occured:", ex)
        print ("Exception TYPE:", type(ex))
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    main()