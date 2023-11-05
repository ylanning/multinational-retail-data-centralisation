import psycopg2
import pandas as pd
import re
import uuid
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
        
    def convert_product_weight_and_float(self, datas):
        """
        method to clean up the weight column and remove all excess characters then represent the weights as a float.
        """
        if 'x' in datas:
            num, _, gram = datas.split()
            return float(num) * float(gram.replace('g', '')) / 1000
   
        if 'oz' in datas:
            oz = datas.replace('oz', '')
            oz_to_kg = float(oz)/35.27396195
            return round(oz_to_kg, 3)
            
        if 'kg' in datas:
            in_kg = datas.replace('kg', '')
            if '.' in in_kg:
                num = in_kg.split('.')[0]
                count = 1
                while count <= len(num):
                    return float(in_kg)
                    count += 1
            elif len(in_kg) == 2:
                return float(in_kg) / 10
            elif len(in_kg) == 3:
                return float(in_kg) / 100
            elif len(in_kg) == 4:
                return float(in_kg) / 1000
            elif len(in_kg) == 5:
                return float(in_kg) / 10000
            else:
                return float(in_kg) 
        else:
            # Convert from grams to kilograms ( 1g = 0.001 kg)
            converted_data = float(re.sub(r'\D', '', datas)) / 1000
            return converted_data
         
    def convert_to_float(self,datas):
        return float(re.sub(r'\D', '', datas))/100
        
    def clean_non_char(self,dataframe):
        cleaned_non_char = re.sub(r'\W', ' ', dataframe)
        return cleaned_non_char

    def clean_non_numeric(self, dataframe):
        cleaned_non_num = re.sub(r'\D','', dataframe)
        return cleaned_non_num

    def is_valid_uuid(self, dataframe):
        try:
            uuid.UUID(dataframe)
            return dataframe
        except:
            return None
                      
    def clean_user_data(self):
        tables = self.datas.read_rds_table()
        print("tables :", tables)

        if 'legacy_users' in tables:
            table_data = tables['legacy_users']

            # convert date to pandas datetime and clean
            table_data['date_of_birth'] = table_data['date_of_birth'].apply(self.convert_date)
            table_data['join_date'] = table_data['join_date'].apply(self.convert_date)

            # uniform phone number format
            table_data['phone_number'] = table_data['phone_number'].apply(self.clean_phone_number)
            
            # Remove duplicate rows based on all columns
            nodup_table_data = table_data.drop_duplicates()
            
            # # Replace NaN values with 0
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

    def clean_store_data(self):
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
            
            # delete row if the opening_date column value is null
            cleaned_df = df.drop("lat", axis='columns')

            # cleaned any missing values
            filtered_df = cleaned_df.replace(['NaN','N/A','null','NULL'], pd.NA)
            new_data = filtered_df.dropna(how='any')
     
            # convert date to pandas datetime and clean
            new_data['opening_date'] = new_data['opening_date'].apply(self.convert_date)
            # delete row with null values in opening date column
            cleaned_date = new_data.dropna(subset=['opening_date'])

            # rename invalid value in continent values
            replacement_values = {'eeEurope':'Europe', 'eeAmerica':'America'}
            cleaned_date['continent'] = cleaned_date['continent'].replace(replacement_values)

            # remove any non-numeric valiue in store_code
            cleaned_date['staff_numbers'] = cleaned_date['staff_numbers'].apply(self.clean_non_numeric)
            
            # remove duplicates
            col_cleaned = self.clean_duplicates(cleaned_date)

            # change columns datatype
            col_cleaned['longitude'] = col_cleaned['longitude'].astype('float64')
            col_cleaned['latitude'] = col_cleaned['latitude'].astype('float64')
            
            stores.append(col_cleaned)

        final_stores_data = pd.concat(stores, axis=0)
        self.upload_data('dim_store_details', final_stores_data)

                
    def clean_product_data(self):
        s3_address = "s3://data-handling-public/products.csv"
        df = self.datas.extract_from_s3(s3_address)
        
        # cleaned any missing values
        filtered_df = df.replace(['NaN','N/A','null','NULL'], pd.NA)
        new_df = filtered_df.dropna(how='any')
        
        new_df['weight_in_kg'] = new_df['weight'].apply(self.convert_product_weight_and_float)
        new_df['product_price_in_£'] = new_df['product_price'].apply(self.convert_to_float)
        new_df['category'] = new_df['category'].apply(self.clean_non_char)
        new_df['date_added'] = new_df['date_added'].apply(self.convert_date)
        new_df['uuid'] = new_df['uuid'].apply(self.is_valid_uuid)
        new_df = new_df.dropna(subset=['uuid'])

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
        date_times['date_uuid'] = date_times['date_uuid'].apply(self.is_valid_uuid)
        date_times = date_times.dropna(subset=['date_uuid'])
        
        self.upload_data('dim_date_times', date_times)
        
        return date_times

    def change_dtype(self, table, column, type):
        cur = self.db_connector.connect_to_db()
        try:
            cur.execute(cur.execute(f"""ALTER TABLE {table} 
                                        ALTER COLUMN {column} 
                                        TYPE {type}
                                        USING {column}::{type}"""))
        except Exception as e:
            print(f"Failed to execute {e}")  
        cur.close()
        
    def alter_order_table_dtype(self):
        self.change_dtype('orders_table','date_uuid','UUID')
        self.change_dtype('orders_table','user_uuid','UUID')
        self.change_dtype('orders_table','card_number','VARCHAR(16)')
        self.change_dtype('orders_table','store_code','VARCHAR(16)')
        self.change_dtype('orders_table','product_code','VARCHAR(16)')
        self.change_dtype('orders_table','product_quantity','SMALLINT')
            
    def alter_dim_users_table_dtype(self):
        self.change_dtype('dim_users','first_name','VARCHAR(255')
        self.change_dtype('dim_users','last_name','VARCHAR(255')
        self.change_dtype('dim_users','date_of_birth','DATE')
        self.change_dtype('dim_users','country_code','VARCHAR(2)')
        self.change_dtype('dim_users','user_uuid','UUID')
        self.change_dtype('dim_users','join_date','DATE')
        
    def alter_store_details_dtype(self):
        self.change_dtype('dim_store_details','longitude','FLOAT')
        self.change_dtype('dim_store_details','locality','VARCHAR(255)')
        self.change_dtype('dim_store_details','store_code','VARCHAR(16)')
        self.change_dtype('dim_store_details','staff_numbers','SMALLINT')
        self.change_dtype('dim_store_details','opening_date','DATE')
        self.change_dtype('dim_store_details','store_type','VARCHAR(255)') 
        self.change_dtype('dim_store_details','latitude','FLOAT') 
        self.change_dtype('dim_store_details','country_code','VARCHAR(2)') 
        self.change_dtype('dim_store_details','continent','VARCHAR(255)') 
         
    def alter_dim_products_table(self):
        cur = self.db_connector.connect_to_db()
        try:
            # create a new weight_class column in the table
            cur.execute("""ALTER TABLE dim_products
                           ADD COLUMN weight_class VARCHAR(20);
                        """)

            # upate new column with human-readable values based on the 
            # weight range of the product 
            cur.execute("""UPDATE dim_products 
                        SET weight_class = 
                        CASE 
                            WHEN weight_in_kg < 2 THEN 'Light'
                            WHEN weight_in_kg BETWEEN 2 AND 39 THEN 'Mid_Sized'
                            WHEN weight_in_kg BETWEEN 40 AND 139 THEN 'Heavy' 
                            ELSE 'Truck_Required' 
                        END;""") 

            # Create still_available column 
            cur.execute("""ALTER TABLE dim_products
                    ADD COLUMN still_available BOOL;""")

            # Update the values in the renamed column
            cur.execute("""UPDATE dim_products
                            SET still_available = CASE 
                            WHEN removed = 'Removed' THEN false 
                            ELSE true
                            END;""")  

            # Delete column removed
            cur.execute("""ALTER TABLE dim_products
                            DROP COLUMN removed;""")
            
            # Changing its data type
            self.change_dtype('dim_products','product_price_in_£','FLOAT')
            self.change_dtype('dim_products','weight_in_kg','FLOAT')
            # self.change_dtype('dim_products','EAN','VARCHAR(16)')
            self.change_dtype('dim_products','product_code','VARCHAR(16)')
            self.change_dtype('dim_products','date_added','DATE')
            self.change_dtype('dim_products','date_uuid','UUID')
            
        except Exception as e:
            print(f"Failed to execute {e}")  
        cur.close()

    def alter_dim_date_times_dtype(self):
        self.change_dtype('dim_date_times','month','VARCHAR(2)')
        self.change_dtype('dim_date_times','year','VARCHAR(4)')
        self.change_dtype('dim_date_times','day','VARCHAR(2)')
        self.change_dtype('dim_date_times','time_period','VARCHAR(12)')
        self.change_dtype('dim_date_times','date_uuid','UUID')

    def alter_dim_card_details_dtype(self):
        self.change_dtype('dim_card_details','card_number','VARCHAR(18)')
        self.change_dtype('dim_card_details','expiry_date','VARCHAR(5)')
        self.change_dtype('dim_card_details','date_payment_confirmed','DATE')

        
    def find_disperancies_data(self,table,column):
        cur = self.db_connector.connect_to_db()
        missing_vals = list()
        # Find any disperancies between the 2 tables
        cur.execute(f"""SELECT DISTINCT orders_table.{column}
                        FROM orders_table
                        LEFT JOIN {table}
                        ON orders_table.{column} = {table}.{column}
                        WHERE {table}.{column} IS NULL """)
        
        # Add all the missing values into lists
        for tab in cur.fetchall():
            value = [value for value in tab]
            missing_vals.extend(value)  
            
        # Insert missing value into the table  
        if missing_vals:
            for val in missing_vals:
                cur.execute(f"INSERT INTO {table}({column}) VALUES ('{val}')")

    def add_primary_key_constraint(self,table,column):
        cur = self.db_connector.connect_to_db()
        try:
            cur.execute(f""" SELECT DISTINCT orders_table.{column}
                            FROM orders_table
                            LEFT JOIN {table}
                            ON orders_table.{column} = {table}.{column}
                            WHERE {table}.{column} IS NULL""")

        except Exception as e:
            print(f"Failed to execute {e}")  
        cur.close() 

                          
    def update_tables_primary_key(self):
        cur = self.db_connector.connect_to_db()
        tables = list()
        
        try:
            cur.execute("""SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';""")
            for tab in cur.fetchall():
                table = [table for table in tab]
                tables.extend(table)

            for table in tables:
                if 'dim_card_details' in table:
                    self.find_disperancies_data(table[0],'card_number')
                    self.add_primary_key_constraint(table[0],'card_number')
    
                if 'dim_users' in table:
                    print('table :', table)
                    self.find_disperancies_data(table[0],'user_uuid')
                    self.add_primary_key_constraint(table[0],'user_uuid')
                    
                if 'dim_date_times' in table:
                    self.find_disperancies_data(table[0], 'date_uuid')
                    self.add_primary_key_constraint(table[0], 'date_uuid')
                    
                if 'dim_products' in table:
                    self.find_disperancies_data(table[0],'product_code')
                    self.add_primary_key_constraint(table[0],'product_code')
     
                if 'dim_store_details' in table:
                    self.find_disperancies_data(table[0], 'store_code')
                    self.add_primary_key_constraint(table[0], 'store_code')

        except Exception as e:
            print(f"Failed to execute {e}")  
        cur.close()                 
                
if __name__ == "__main__":                      
    data = DataCleaning()
    # clean = data.clean_orders_data()
    products = data.create_dim_tables_primary_key()

    # if products is not None:
    #     print(products)
    #     print(products.head())
    #     # print(products['card_number'])
    #     # print(products['store_code'])
    # else:
    #     print("Failed to extract data")