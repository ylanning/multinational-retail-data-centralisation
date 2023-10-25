import psycopg2
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

    def clean_user_data(self):
        print('inside clean user data')
        tables = self.datas.read_rds_table()
        print('TABLES')
        for name, table in tables.items():
            print('TABLE :', name)
            print(table.info())
        # for table in tables.values():
        #     columns = table.columns
        #     for col in columns:
        #         missing_value = table[col].isna().any()
        #         if missing_value:
        #             print(table[col])
        #             table[col].fillna(0)

            
data = DataCleaning()
print(data.clean_user_data())