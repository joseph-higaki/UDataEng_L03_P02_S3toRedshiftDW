import configparser
import psycopg2
import sql_queries
from sql_queries import execute_query_list


def main():
    """Entry point for DML scripts to load data into the database
    """    
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect(f"host={config.get('CLUSTER','HOST')} dbname={config.get('CLUSTER','DB_NAME')} user={config.get('CLUSTER','DB_USER')} password={config.get('CLUSTER','DB_PASSWORD')} port={config.get('CLUSTER','DB_PORT')}")
    cur = conn.cursor()
    try:
        # Load Raw Staging Tables
        execute_query_list(cur, conn, sql_queries.copy_table_queries)

        # Load Intermediate Staging Tables
        execute_query_list(cur, conn, sql_queries.insert_intermediate_staging_table_queries)

        # Load DWH Tables
        execute_query_list(cur, conn, sql_queries.insert_dwh_table_queries)
    finally:    
        conn.close()


if __name__ == "__main__":
    main()