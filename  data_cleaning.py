import psycopg2
import pandas as pd
from data_extraction import DataExtractor
from database_utils import DatabaseConnector

class DataCleaning:
    """
    to clean data from each of the data sources.
    """
    def __init__(self):
        self.datas = DataExtractor()
        self.db_connector = DatabaseConnector()
        self.conn = self.db_connector.init_db_engine()
        self.upload_data = self.db_connector.upload_to_db

    def clean_user_data(self):
        tables = self.datas.read_rds_table()
        print("inside clean user_data")
        print(type(tables))
        user_data_df = None
        
        for table_name, table_data in tables.items():
            if table_name == 'legacy_users': 
                print("check type of table data")
                print(type(table_data))
                # check if any date format error
                table_data['date_of_birth'] = pd.to_datetime(table_data['date_of_birth'], errors='coerce')
                table_data['join_date'] = pd.to_datetime(table_data['join_date'], errors='coerce')
                
                # find any null values in every columns
                columns = table_data.columns
                for col in columns:
                    find_null = table_data[col].isnull().sum().sum()
                    # print(f'{col} has {find_null} of NaN values')

                # Replace null values with 0 in the 'legacy_users' table
                if 'legacy_users' in table_name:
                    table_data.fillna(0, inplace=True) 
                      
                #  Remove duplicates DataFrame
                table_data.drop_duplicates(keep='first')
        
            self.upload_data(table_data,"dim_users")
        
               
data = DataCleaning()
print(data.clean_user_data())