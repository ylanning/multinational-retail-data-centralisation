import psycopg2
import yaml
import json

from sqlalchemy import create_engine

util_file = './ db_creds.yaml'

class DatabaseConnector:
    """
    will use to connect with and upload data to the database.
    """         
    def read_db_creds(self):
        # this will read the credentials yaml file and return a dictionary of the credentials.
        with open(util_file, 'r') as stream:
            try:
                data_loaded = yaml.safe_load(stream)
            except yaml.YAMLError as exc:
                print(exc)        
            return data_loaded

    def init_db_engine(self):
        # Initialise credentials details
        credentials = self.read_db_creds()

        if credentials:
            try:
                HOST = credentials['RDS_HOST']
                PASSWORD = credentials['RDS_PASSWORD']
                USER = credentials['RDS_USER']
                DATABASE = credentials['RDS_DATABASE']
                PORT = credentials['RDS_PORT']

                # Establish Connections
                conn = psycopg2.connect(
                    host = HOST,
                    port = PORT,
                    database = DATABASE,
                    user = USER,
                    password = PASSWORD
                )
                print('its connected')            
                
            except Exception as e:
                print(e)         
        return conn

    def upload_to_db(self):
        # take in a Pandas DataFrame and table name to upload to as an argument.
        # Once extracted and cleaned use the upload_to_db method to store the data in your sales_data database in a table named dim_users
        pass

if  __name__ == "__main__": 
    print("Calling a function from a Class")
    utils_one = DatabaseConnector()
    print(utils_one.init_db_engine())



            