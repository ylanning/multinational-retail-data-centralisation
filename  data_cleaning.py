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
            return pd.to_datetime(date_str, format='%Y-%m-%d', errors='coerce')
        
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

    def clean_duplicates(self,data):
        clean_data = data.drop_duplicates()
        return clean_data

    def check_null_string(self,data):
        # check if any null value in a series
        is_null = data.isnull().values.any()
        
        if is_null:
            invalid_string = ['NaN','N/A','null','NULL']
            df = data.replace(invalid_string, pd.NA)
            return(df)
                
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

    def called_clean_store_data(self):
        # cleans the data retrieve from the API and 
        # returns a pandas DataFrame
        headers = {
            "x-api-key" : "yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"
            }
        # List of stores
        store_datas = self.datas.retrieve_stores_data(headers)
        # Total number of stores
        store_numbers = self.datas.list_number_of_stores(headers)
  
        # all dataframes placeholder
        stores = list()

        # specify an index for all stores
        index = range(store_numbers)
        
        for store in store_datas:
            df = pd.DataFrame(store, index=index)
            print(df.info())
            
            # delete row if the opening_date column value is null
            cleaned_df = df.drop("lat", axis='columns')

            # cleaned any missing values
            filtered_df = cleaned_df.replace(['NaN','N/A','null','NULL'], pd.NA)
            new_data = filtered_df.dropna(how='any')
     
            # convert date to pandas datetime and clean
            new_data['opening_date'] = new_data['opening_date'].apply(self.convert_date)
            cleaned_date = new_data.dropna(subset=['opening_date'])

            # rename invalid value in continent values
            replacement_values = {'eeEurope':'Europe', 'eeAmerica':'America'}
            cleaned_date['continent'] = cleaned_date['continent'].replace(replacement_values)
            
            # remove duplicates
            col_cleaned = self.clean_duplicates(cleaned_date)

            # change columns datatype
            col_cleaned['longitude'] = col_cleaned['longitude'].astype('float64')
            col_cleaned['latitude'] = col_cleaned['latitude'].astype('float64')
            
            print(col_cleaned.info())
            
            stores.append(col_cleaned)

        final_stores_data = pd.concat(stores, axis=0)
        self.upload_data('dim_store_details', final_stores_data)
                      
data = DataCleaning()
print(data.called_clean_store_data())