import tabula
import pandas as pd
import requests
import boto3

from database_utils import DatabaseConnector
from sqlalchemy import inspect, text
from io import StringIO


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
        extract the database table to a pandas DataFramev  
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

    def list_number_of_stores(self, headers):
        #  returns the number of stores to extract. It should take 
        #  in the number of stores endpoint and header dictionary as an argument.
        
        # API endpoint url
        api_url = " https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores"
        response = requests.get(api_url, headers=headers)
       
        # Send a GET request to get retrieve information about the number of stores
        if response.status_code == 200:

            # Check if the request was succesfull (status code 200)
            data = response.json()

            # Extract and print the number_of_stores
            num_stores = data['number_stores']
            print(num_stores)

        else:
            print(f"Request failed with status code: {response.status_code}")
            print(f"Response Text: {response.text}")

        return num_stores

    def retrieve_stores_data(self, headers):
        # returns the number of stores to extract.
        store_numbers = self.list_number_of_stores(headers)
        stores_data = list()

        for num in range(store_numbers):
            # API endpoint URL
            api_url = f"https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{num}"

            # make a GET requests to the API
            response = requests.get(api_url, headers=headers)

            # check the response status code
            if response.status_code == 200:
                data = response.json()
                stores_data.append(data)
            else:
                print(f"Request failed with status code: {response.status_code}")
                print(f"Response Text: {response.text}")
        return stores_data

    def extract_from_s3(self, s3_address):
        s3 = boto3.client('s3')

        # Extract the bucket name and object key from the S3 address
        bucket, key = s3_address.replace('s3://', '').split('/', 1)
        print("bucket :", bucket)
        print("key :", key)

        try:
            # Download the object from S3
            response = s3.get_object(Bucket=bucket, Key=key)
            content = response['Body'].read().decode('utf-8')

            # convert the csv content to a Pandas DataFrame
            df = pd.read_csv(StringIO(content))
            
            return df
        except Exception as e:
            print(f"Error: {e}")
            return None

    def extract_from_s3_json(self, s3_url):
      
        # Split the S3 URL into its components
        s3_parts = s3_url.split('/')
        bucket_name = s3_parts[2].split('.')[0]  # 'data-handling-public'
        aws_region = s3_parts[2].split('.')[2] # 'eu-west-1'
        file_key = s3_parts[-1] # 'date_details.json'
        print("Bucket Name:", bucket_name)
        print("AWS Region:", aws_region)
        print("File Key:", file_key)
        
        s3 = boto3.client('s3', region_name=aws_region)
         # # Download the file from S3
        s3.download_file(bucket_name, file_key, "date_details.json")

        # # Read the downloaded JSON file into a pandas DataFrame
        df = pd.read_json("date_details.json")

        return df  
    
        

if __name__ == "__main__":
    extractor = DataExtractor()
    data_frame = extractor.extract_from_s3()

    if data_frame is not None:
        print(data_frame.info())
        print(data_frame['weight'].head(200))
    else:
        print("Failed to extract data from s3.")