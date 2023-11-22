import psycopg2
import pandas as pd
import yaml
from sqlalchemy import create_engine, text

util_file = './db_creds.yaml'
db_file = './sales_data_env.yaml'

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

    def db_params(self):
        with open(db_file,'r') as stream:
            try:
                db_env= yaml.safe_load(stream)
            except yaml.YAMLError as exc:
                print(exc)
            return db_env

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

    def upload_to_db(self,table_name,dataframe):
        print('inside upload to db')
        # initialise db params
        db_params = self.db_params()

        if db_params:
            host = 'localhost'
            database = db_params['database']
            user = db_params['user']
            password = db_params['password']
            port = db_params['port']
            db_type =  db_params['db_type']
            dbapi = db_params['dbapi']
            
            # Create a connection to the PostgreSQL server
            try:
                conn = psycopg2.connect(    
                    host=host,
                    database=database,
                    user=user,
                    password=password,
                    port=port
                )
                print(conn)
                # Create a cursor object
                cur = conn.cursor()

                # Set automatic commit to be true, so that each action is committed without having to call conn.committ() after each command
                conn.set_session(autocommit=True)     
                print("it's connected")
                        
            except Exception as e:
                print("failed to make connection")
                print(e)               

            try:
                # Connect to the user_data database
                engine = create_engine(f"{db_type}+{dbapi}://{user}:{password}@{host}:{port}/{database}")
                
                with engine.begin() as connection:
                    dataframe.to_sql(table_name, engine, if_exists='replace', index_label='id')
            except Exception as e:
                print("Uploading table to database is failed")
                print(e)
                
            # # Commit the changes and close the connection to the default database
            conn.commit()
            cur.close()
            conn.close()

    def connect_to_db(self):
        # initialise db params
        db_params = self.db_params()

        if db_params:
            host = 'localhost'
            database = db_params['database']
            user = db_params['user']
            password = db_params['password']
            port = db_params['port']
            
            # Create a connection to the PostgreSQL server
            try:
                conn = psycopg2.connect(    
                    host=host,
                    database=database,
                    user=user,
                    password=password,
                    port=port
                )
                print(conn)
                print("it's connected")
                # Create a cursor object
                cur = conn.cursor()
                # Set automatic commit to be true, so that each action is committed without having to call conn.commit() after each command
                conn.set_session(autocommit=True)     

                # # Close cursor and communication with the database
                # cur.close()
                # conn.close()
                 
            except Exception as e:
                print("failed to make connection")
                print(e)  
                
        return cur
        

if  __name__ == "__main__": 
    print("Calling a function from a Class")
    utils_one = DatabaseConnector()
    print(utils_one.init_db_engine())



            