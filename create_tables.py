import configparser
import psycopg2
import sql_queries 
from sql_queries import execute_query_list

def main():
    """Entry point for DDL scripts
    """    
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect(f"host={config.get('CLUSTER','HOST')} dbname={config.get('CLUSTER','DB_NAME')} user={config.get('CLUSTER','DB_USER')} password={config.get('CLUSTER','DB_PASSWORD')} port={config.get('CLUSTER','DB_PORT')}")
    cur = conn.cursor()
    try:
        #Create Raw Staging Tables
        # Comment this line if COPY from S3 to Redshift is not needed
        # sql_queries.execute_commit_query_list(cur, conn, sql_queries.drop_raw_staging_table_queries)
        execute_query_list(cur, conn, [*sql_queries.drop_raw_staging_table_queries, *sql_queries.create_raw_staging_table_queries])

        #Create Intermediate Staging Tables
        execute_query_list(cur, conn, [*sql_queries.drop_intermediate_staging_table_queries, *sql_queries.create_intermediate_staging_table_queries])

        #Create Data Warehouse Tables
        execute_query_list(cur, conn, [*sql_queries.drop_dwh_table_queries, *sql_queries.create_dwh_table_queries])
    finally:
        conn.close()

if __name__ == "__main__":
    main()