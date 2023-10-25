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

    def upload_to_db(self,dataframe,name):
        print('inside upload to db')
        print(dataframe)
        print(type(dataframe))
        print(name)
        conn = self.init_db_engine()
        df = dataframe
        # Specify the table name where to load the data
        table_name = name
        # Write the DataFrame to the PostgreSQL table
        df.to_sql(name=table_name, con=conn, index=False )      
        # Commit and close the connection
        conn.commit()
        conn.close()

if  __name__ == "__main__": 
    print("Calling a function from a Class")
    utils_one = DatabaseConnector()
    print(utils_one.init_db_engine())



            