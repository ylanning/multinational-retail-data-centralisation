from database_utils import DatabaseConnector

db_connector = DatabaseConnector()
conn = db_connector.init_db_engine()
cur = db_connector.connect_to_db()

def task_one():
    # How many stores does the business have and in which countries
    try:
        cur.execute("""SELECT country_code AS country,
                              COUNT(store_code) AS total_no_stores
                        FROM dim_store_details
                        WHERE country_code IN ('GB', 'DE', 'US')
                        GROUP BY country_code
                        ORDER BY total_no_stores DESC; """)
        
        for data in cur.fetchall():
            print(data)
            
    except Exception as e:
        print(e)

def task_two():
    # How many stores does the business have and in which countries
    try:
        cur.execute("""SELECT locality,
                        COUNT(store_code) AS total_no_stores
                        FROM dim_store_details
                        GROUP BY locality
                        HAVING COUNT(store_code) >= 10
                        ORDER BY total_no_stores DESC; """)
        
        for data in cur.fetchall():
            print(data)
            
    except Exception as e:
        print(e)

def task_three():
    # which month produced the largest amount of sales
    try:
        cur.execute(""" SELECT SUM(products.product_price_in_£ * orders.product_quantity) AS total_sales,
                        dates.month
                        FROM orders_table AS orders 
                        INNER JOIN dim_date_times AS dates 
                        ON orders.date_uuid = dates.date_uuid
                        INNER JOIN dim_products AS products
                        ON orders.product_code = products.product_code
                        GROUP BY month
                        ORDER BY total_sales DESC;""")
        
        for data in cur.fetchall():
            print(data)
            
    except Exception as e:
        print(e)

def task_four():
    # how many sales are coming from online

    # Note: I can't find any values that contain web / offline,
    #       could it be the issue during cleaning?
    
    pass

def task_five():
    # What percentage of sales come through each type of store
    try:
        cur.execute("""SELECT stores.store_type, 
                              SUM(products.product_price_in_£ * orders.product_quantity) AS total_sales
                        FROM dim_products AS products
                        INNER JOIN orders_table AS orders
                        ON products.product_code = orders.product_code
                        INNER JOIN dim_store_details AS stores
                        ON stores.store_code = orders.store_code
                        GROUP BY stores.store_type
                        ORDER BY total_sales DESC """)
        
        for data in cur.fetchall():
            print(data)
            
    except Exception as e:
        print(e)

def task_six():
    # Which month in each year produced the highest cost of sales
    try:
        cur.execute(""" SELECT  SUM(products.product_price_in_£ * orders.product_quantity) AS total_sales,
                                dates.year,
                                dates.month
                        FROM dim_products AS products
                        INNER JOIN orders_table AS orders
                        ON products.product_code = orders.product_code
                        INNER JOIN dim_date_times AS dates 
                        ON dates.date_uuid = orders.date_uuid
                        GROUP BY dates.year, dates.month
                        ORDER BY total_sales DESC; """)
        
        for data in cur.fetchall():
            print(data)    
    except Exception as e:
        print(e)

def task_seven():
    # staff headcount
    try:
        cur.execute(""" SELECT sum(staff_numbers) AS total_staff_numbers,
                        country_code
                        FROM dim_store_details
                        GROUP BY country_code
                        ORDER BY total_staff_numbers DESC; """)
        
        for data in cur.fetchall():
            print(data)    
    except Exception as e:
        print(e)

def task_eight():
    # Which german store type is selling the most
    try:
        cur.execute(""" SELECT SUM(product_price_in_£ * orders.product_quantity) AS total_sales,
                        stores.store_type,
                        stores.country_code
                        FROM dim_products AS products
                        INNER JOIN orders_table AS orders
                        ON products.product_code = orders.product_code
                        INNER JOIN dim_store_details AS stores 
                        ON stores.store_code = orders.store_code
                        WHERE stores.country_code = 'DE'
                        GROUP BY stores.store_type, stores.country_code
                        ORDER BY total_sales; """)
    
        for data in cur.fetchall():
            print(data)    
    except Exception as e:
        print(e)

def task_nine():
    # How quickly is the company making sales

    # Submitted due to ran out of time
    # I stuck working on this task and not sure where to start
    pass 

if __name__ == "__main__":                      
    pass