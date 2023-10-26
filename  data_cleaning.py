import psycopg2
import pandas as pd
import re
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

    # Define a custom function to convert date formats conditionally
    def convert_date(self,date_str):
        try:
            date_str = pd.to_datetime(date_str)
            # Try converting using the first format
            return pd.to_datetime(date_str, format='%Y-%m-%d')
        except ValueError:
            try:
                # If the first format fails, try the second format
                return pd.to_datetime(date_str, format='%Y %B %d')
            except ValueError:
                # If both formats fail, return the original value
                return pd.to_datetime(date_str, errors='coerce')

    def clean_phone_number(self,phone):
        # Remove non-numeric characters (including '+') using regex
        phone = re.sub(r'[^0-9]', '', phone)
        # Remove leading '0' after country code if it exists
        phone = re.sub(r'^\+0', '', phone)
        return phone
            
    def clean_user_data(self):
        tables = self.datas.read_rds_table()
        print("inside clean user_data")
        print(tables.keys())
        
        for table_name, table_data in tables.items():
            if table_name == 'legacy_users': 
                # convert date to pandas datetime and clean
                table_data['date_of_birth'] = table_data['date_of_birth'].apply(self.convert_date)
                table_data['join_date'] = table_data['join_date'].apply(self.convert_date)

                # uniform phone number format
                table_data['phone_number'] = table_data['phone_number'].apply(self.clean_phone_number)
                
                # Remove duplicate rows based on all columns
                nodup_table_data = table_data.drop_duplicates()
     
                # Replace NaN values with 0
                cleaned_table_data = nodup_table_data.dropna()
            
                self.upload_data("dim_users", cleaned_table_data)

    def clean_card_data(self):
        file_path = './card_details.pdf'
        print("inside clean card data")
        tables = self.datas.retrieve_pdf_data(file_path)
        datas = list()
  
        for df in tables:
            df['date_payment_confirmed'] = df['date_payment_confirmed'].apply(self.convert_date)
            df.drop_duplicates()
            # Append the current DataFrame to the datas DataFrame
            datas.append(df)

        # Concatenate all DataFrames in the list into one DataFrame
        final_data = pd.concat(datas, ignore_index=True)
        final_data.reset_index(drop=True, inplace=True)
        
        self.upload_data("dim_card_details", final_data)
                      
data = DataCleaning()

print(data.clean_card_data())