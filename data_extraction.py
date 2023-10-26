from database_utils import DatabaseConnector
import psycopg2
import tabula
import pandas as pd
from sqlalchemy import inspect, text


class DataExtractor:
    """
    This class will work as a utility class, in it you will be creating methods that help extract data from different data sources.
    The methods contained will be fit to extract data from a particular data source, 
    these sources will include CSV files, an API and an S3 bucket.
    """
    def __init__(self):
    
        print('inside data extractor')
        self.db_conn = DatabaseConnector()
        self.conn = self.db_conn.init_db_engine()

    def list_db_tables(self):
        """
        to list all the tables in the database so you know which tables you can extract data from
        """
        tables = list()
        with self.conn.cursor() as cur:
            cur.execute("""SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'""")
            for tab in cur.fetchall():
                table = [table for table in tab]
                tables.extend(table)
            return tables

    def read_rds_table(self):
        """
        extract the database table to a pandas DataFrame
        """
        tables = self.list_db_tables()
        # table named will be returned as a dataframe.
        tables_df = dict()
        for table in tables:
            with self.conn.cursor() as cur:
                cur.execute(f"SELECT * FROM {table};")
                data = cur.fetchall()
                cur.close()
                
                # create a dataframe
                cols = list()
                for elt in cur.description:
                    col_name = elt[0]
                    cols.append(col_name)
                df = pd.DataFrame(data=data, columns=cols)
                tables_df[table] = df

        return tables_df

    def retrieve_pdf_data(self,pdf_path):
        # read pdf returns list of Dataframe
        tables = tabula.read_pdf(pdf_path, pages="all", multiple_tables=True)
        return tables


# data = DataExtractor()
# file_path = './card_details.pdf'
# print(data.retrieve_pdf_data(file_path))