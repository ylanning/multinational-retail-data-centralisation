import psycopg2
import pandas as pd
import re
import boto3
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

    def convert_product_weight_and_float(self, datas):
        """
        method to clean up the weight column and remove all excess characters then represent the weights as a float.
        """
        if 'g' in datas or 'ml' in datas :
            # Convert from grams to kilograms ( 1g = 0.001 kg)
            converted_data = float(re.sub(r'\D', '', datas)) / 1000
            return converted_data
        else:
            return float(re.sub(r'\D', '', datas))

    def clean_non_char(self,dataframe):
        cleaned_non_char = re.sub(r'\W', ' ', dataframe)
        return cleaned_non_char
            
    def clean_product_data(self):
        s3_address = "s3://data-handling-public/products.csv"
        df = self.datas.extract_from_s3(s3_address)
        
        # cleaned any missing values
        filtered_df = df.replace(['NaN','N/A','null','NULL'], pd.NA)
        new_df = filtered_df.dropna(how='any')
        
        new_df['weight_in_kg'] = new_df['weight'].apply(self.convert_product_weight_and_float)
        new_df['product_price_in_£'] = new_df['product_price'].apply(self.convert_product_weight_and_float)
        new_df['category'] = new_df['category'].apply(self.clean_non_char)
        new_df['date_added'] = new_df['date_added'].apply(self.convert_date)

        self.upload_data('dim_products', new_df)

    def clean_orders_data(self):
        tables = self.datas.read_rds_table()

        if 'orders_table' in tables:
            table_data = tables['orders_table']

            # columns that need to be removed
            columns_to_remove = ['first_name','last_name', '1']

            # Drop the specified columns
            table_data.drop(columns=columns_to_remove, axis=1, inplace=True)

            # Upload the cleaned data
            self.upload_data('orders_table', table_data)

        return table_data

    def clean_date_times(self):
        s3_address = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json'
        date_times = self.datas.extract_from_s3_json(s3_address)
        self.upload_data('dim_date_times', date_times)
        return date_times

    
if __name__ == "__main__":                      
    data = DataCleaning()
    products = data.clean_date_times()

    if products is not None:
        # print(products)
        print(products.head())
        print(products.info())
    else:
        print("Failed to extract data")