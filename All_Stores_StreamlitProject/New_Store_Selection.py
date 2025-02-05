import requests
import json
import math
import time
import mysql.connector
from mysql.connector import Error
import pandas as pd
from itertools import chain
import concurrent.futures
import os

# Define the path
path = r'D:\\Vinita\\Project\\Final_streamlit_Project\\data\\'
files = os.listdir(path)
for file in files:
    file_path = os.path.join(path, file)
    if os.path.isfile(file_path):
        os.remove(file_path)

# ---------------------- STEP 1: Get Store Domains ----------------------
def get_store_domains():
    try:
        connection = mysql.connector.connect(
            host='qeappsdbrds.cjwmwom4ez5e.us-west-1.rds.amazonaws.com',
            user='ecom_demand_forecasting_prod',
            password='bxZ#3#g!kBz@7?b$!h3?',
            database='ecom_demand_forecasting_prod'
        )
        if connection.is_connected():
            cursor = connection.cursor()
            # query = "SELECT DISTINCT domain FROM ecom_demand_forecasting_prod.store_events LIMIT 10;"
            # query = "SELECT DISTINCT domain FROM ecom_demand_forecasting_prod.store_events ORDER BY domain ASC LIMIT 2;"
            query = "SELECT DISTINCT domain FROM ecom_demand_forecasting_prod.store_events where domain='dyori.myshopify.com'"
            # query = "SELECT DISTINCT domain FROM ecom_demand_forecasting_prod.store_events WHERE domain IN ('dyori.myshopify.com', 'select-interiors-gifts.myshopify.com', 'iamjuliedu.myshopify.com');"
            # query="SELECT DISTINCT domain FROM ecom_demand_forecasting_prod.store_events LIMIT 5 OFFSET 50;"
            cursor.execute(query)
            result = cursor.fetchall()
            return [row[0] for row in result]
    except Error as e:
        print(f"Error: {e}")
        return []
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()


# ---------------------- STEP 2: Fetch Data from API ----------------------
# def fetch_all_data(api_url, store_domain, store_name, file_suffix):
#     limit = 250
#     response = requests.get(f"{api_url}?page=1&limit={limit}&domain={store_domain}")
#     if response.status_code != 200:
#         print(f"Failed to fetch data for {store_domain}.")
#         return
#     data = response.json()
#     total_records = data.get("totalRecords", 0)
#     total_pages = 2  # Adjust as needed
#     print(f"{store_name}: Total Records: {total_records}, Total Pages: {total_pages}")
#     all_data = []
#
#     for page in range(1, total_pages + 1):
#         print(f"Fetching page {page}/{total_pages} for {store_name}...")
#         response = requests.get(f"{api_url}?page={page}&limit={limit}&domain={store_domain}")
#         if response.status_code == 200:
#             data = response.json()  # Parse the response JSON
#             if not data.get("success", True) and "Store not found for the given domain" in data.get("message", ""):
#                 print(f"Error: {data.get('message')}")
#                 return  #
#             else:
#                 page_data = response.json().get("data", [])
#                 all_data.extend(page_data)
#                 print("Data fetched successfully")
#         else:
#             print(f"Failed to fetch data. Status code: {response.status_code}")
#
#         if page < total_pages:
#             print("Waiting for 10 seconds before fetching the next page...")
#             time.sleep(10)
#
#     file_name = f"{store_name}_{file_suffix}.json"
#     with open(file_name, "w") as json_file:
#         json.dump(all_data, json_file, indent=4)
#     print(f"All data collected and saved to '{file_name}' for {store_name}.")

def fetch_all_data(api_url, store_domain, store_name, file_suffix):
    limit = 250
    retries = 5
    retry_delay = 2

    def get_with_retry(url, retries, delay):
        attempt = 0
        while attempt < retries:
            response = requests.get(url)
            if response.status_code == 200:
                return response
            else:
                print(f"Attempt {attempt + 1}: Failed to fetch data. Status code: {response.status_code}")
                attempt += 1
                if attempt < retries:
                    print(f"Retrying in {delay} seconds...")
                    time.sleep(delay)
                    delay *= 2  # Exponential backoff
        print(f"Failed to fetch data after {retries} attempts.")
        return None

    response = get_with_retry(f"{api_url}?page=1&limit={limit}&domain={store_domain}", retries, retry_delay)

    if not response:
        print(f"Failed to fetch data for {store_domain}. Giving up.")
        return

    data = response.json()
    total_records = data.get("totalRecords", 0)
    total_pages = 2
    print(f"{store_name}: Total Records: {total_records}, Total Pages: {total_pages}")
    all_data = []
    for page in range(1, total_pages + 1):
        print(f"Fetching page {page}/{total_pages} for {store_name}...")
        response = get_with_retry(f"{api_url}?page={page}&limit={limit}&domain={store_domain}", retries, retry_delay)
        if not response:
            print(f"Failed to fetch data for page {page}. Skipping...")
            continue

        data = response.json()
        if not data.get("success", True) and "Store not found for the given domain" in data.get("message", ""):
            print(f"Error: {data.get('message')}")
            return
        else:
            page_data = data.get("data", [])
            all_data.extend(page_data)
            print("Data fetched successfully")

        if page < total_pages:
            print("Waiting for 10 seconds before fetching the next page...")
            time.sleep(5)

    file_name = f"{store_name}_{file_suffix}.json"
    with open(file_name, "w") as json_file:
        json.dump(all_data, json_file, indent=4)
    print(f"All data collected and saved to '{file_name}' for {store_name}.")


# ---------------------- STEP 3: Define API Endpoints ----------------------
api_endpoints = {
    "https://apps.qeapps.com/ecom_apps_n/production/demand-forecasting/api/v1/getAllGraphqlOrders": "orders",
    "https://apps.qeapps.com/ecom_apps_n/production/demand-forecasting/api/v1/getGraphqlCustomers": "customers",
    "https://apps.qeapps.com/ecom_apps_n/production/demand-forecasting/api/v1/getGraphqlProducts": "products",
    "https://apps.qeapps.com/ecom_apps_n/production/demand-forecasting/api/v1/getGraphqlAbandonOrders": "abandoned_checkouts"
}


# ---------------------- STEP 4: Fetch Data for Each Store ----------------------
def fetch_data_for_store(store_domain):
    store_name = store_domain.split(".myshopify")[0]
    for api_url, file_suffix in api_endpoints.items():
        fetch_all_data(api_url, store_domain, store_name, file_suffix)


# ---------------------- STEP 5: Fetch and Process Customer Journey Data ----------------------
def fetch_customer_journey(domain_filter, store_name):
    try:
        connection = mysql.connector.connect(
            host='qeappsdbrds.cjwmwom4ez5e.us-west-1.rds.amazonaws.com',
            user='ecom_demand_forecasting_prod',
            password='bxZ#3#g!kBz@7?b$!h3?',
            database='ecom_demand_forecasting_prod'
        )
        if connection.is_connected():
            cursor = connection.cursor()
            query = "SELECT data FROM ecom_demand_forecasting_prod.store_events WHERE domain = %s"
            cursor.execute(query, (domain_filter,))
            result = cursor.fetchall()
            all_data = [{"Data": row[0]} for row in result]
            file_name = f"{store_name}_CustomerJourney_store.json"
            with open(file_name, 'w') as json_file:
                json.dump(all_data, json_file, indent=4)
            print(f"Data for domain '{domain_filter}' has been saved to {file_name}")

    except Error as e:
        print(f"Error: {e}")
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()


def fetch_customer_journey_for_store(store_domain):
    store_name = store_domain.split(".myshopify")[0]
    fetch_customer_journey(store_domain, store_name)

store_domains = get_store_domains()

# with concurrent.futures.ThreadPoolExecutor() as executor:
#     futures = []
#     for store_domain in store_domains:
#         futures.append(executor.submit(fetch_data_for_store, store_domain))
#     for store_domain in store_domains:
#         futures.append(executor.submit(fetch_customer_journey_for_store, store_domain))
#     for future in concurrent.futures.as_completed(futures):
#         future.result()

with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
    futures = []
    for store_domain in store_domains:
        futures.append(executor.submit(fetch_data_for_store, store_domain))
    for store_domain in store_domains:
        futures.append(executor.submit(fetch_customer_journey_for_store, store_domain))
    for future in concurrent.futures.as_completed(futures):
        future.result()

for store_domain in store_domains:
    store_name = store_domain.split(".myshopify")[0]
    file_name = f"{store_name}_CustomerJourney_store.json"
    with open(file_name, 'r') as file:
        data = json.load(file)
    pd.set_option('display.max_colwidth', None)
    extracted_data = [json.loads(item['Data']) for item in data]
    flattened_data = list(chain.from_iterable(extracted_data))
    df = pd.DataFrame(flattened_data)
    df['timestamp'] = pd.to_datetime(df['timestamp'].str.replace('Z', '', regex=False), utc=True)
    df = df[~((df['ip'].isnull()) & (df['customerId'].isnull()))]
    ip_to_customer = df[pd.notnull(df['customerId'])].set_index('ip')['customerId'].to_dict()
    # Create the 'product_id' column
    df['product_id'] = df.apply(
        lambda row: row['label'] if row['action'] in ['Product'] else
        row['label'].split(':')[0] if row['action'] in ['Cart Add', 'Cart Remove', 'Cart Update'] and ':' in row[
            'label'] else None,
        axis=1
    )
    # Create the 'quantity' column
    df['quantity'] = df.apply(
        lambda row: row['value'] if row['action'] in ['Cart Add'] else
        row['value'].split(': ')[-1] if row['action'] == 'Cart Update' and ':' in row['value'] else None,
        axis=1
    )
    df['collection_name'] = df.apply(
        lambda row: row['label'].split('?')[0] if row['action'] in ['Collection'] else None,
        axis=1
    )
    # Create the 'search_term' column
    df['search_term'] = df.apply(
        lambda row: row['label'] if row['action'] in ['Search'] else None,
        axis=1
    )
    # Create the 'time_on_page' column
    df['time_on_page'] = df.apply(
        lambda row: row['value'] if row['action'] in ['Product', 'Collection', 'Cart', 'Home', 'Search', 'Blog', 'Page',
                                                      'Other'] else None,
        axis=1
    )
    # Create the 'product_name' column
    df['Product_Name'] = df.apply(
        lambda row: row['url'].split('/products/')[-1].split('?')[0].replace("-", " ") if '/products/' in row[
            'url'] else
        row['label'].split(': ')[1] if row['action'] in ['Cart Add', 'Cart Remove', 'Cart Update'] and ':' in row[
            'label'] else None,
        axis=1
    )
    # Filter actions based on desired values
    df = df[df['action'].isin(
        ['Product', 'Cart Add', 'Cart Update', 'Cart Remove', 'Cart', 'Collection', 'Search', 'Home'])]
    # Drop unnecessary columns
    df = df.drop(columns=['label', 'value', 'url', 'customerId'])
    # Remove duplicate rows
    df.drop_duplicates(inplace=True)
    # Reset index for the DataFrame
    df.reset_index(drop=True, inplace=True)
    # Rename columns to the desired names
    df.rename(columns={
        'action': 'Event',
        'ip': 'Customer_IP',
        'timestamp': 'Event_Time',
        'product_id': 'Product_ID',
        'quantity': 'Quantity',
        'collection_name': 'Collection_Name',
        'search_term': 'Search_Term',
        'time_on_page': 'Time_On_Page',
        'Product_Name': 'Product_Name'
    }, inplace=True)
    # ---------------------- STEP 8: Assign Sessions ----------------------
    df = df.sort_values(by=['Customer_IP', 'Event_Time'])
    df['session'] = 1


    def assign_sessions(group):
        session_id = 1
        for i in range(1, len(group)):
            if (group['Event_Time'].iloc[i] - group['Event_Time'].iloc[i - 1]).total_seconds() > 3600:
                session_id += 1
            group.loc[group.index[i], 'session'] = session_id
        return group


    df = df.groupby('Customer_IP').apply(assign_sessions)
    # ---------------------- STEP 9: Save Final Data ----------------------
    output_file = f"D:\\Vinita\\Project\\Final_streamlit_Project\\data\\{store_name}_CJ.csv"
    df.to_csv(output_file, index=False)
    print(f"Processed data saved to {output_file}")

    class CustomerDataFetcher:
        def __init__(self, json_file):
            self.json_file = json_file

        def get_customers(self):
            with open(self.json_file, 'r') as file:
                json_data = json.load(file)
                print(json_data)

            data = json_data
            customers = []
            for customer_json in data:
                customer_id = customer_json.get('id')
                created_at = customer_json.get('created_at')
                updated_at = customer_json.get('updated_at')
                orders_count = int(customer_json.get('orders_count', 0))  # Convert to int
                total_spent = float(customer_json.get('total_spent', 0.0))
                last_order_id = customer_json.get('last_order_id', None)  # Default to None if not present
                customers_first_name = customer_json.get('first_name') or ''
                customer_last_name = customer_json.get('last_name') or ''
                customer_full_name = customers_first_name + ' ' + customer_last_name
                default_address = customer_json.get('default_address', {})
                if default_address:  # Check if default_address is not None or empty
                    customer_default_address_province = default_address.get("province")
                    customer_default_address_country = default_address.get("country")
                else:
                    customer_default_address_province = None
                    customer_default_address_country = None

                customers.append({
                    "Customer_ID": customer_id,
                    "Customer_Created_At": created_at,
                    "Customer_Updated_At": updated_at,
                    "Customer_Orders_Count": orders_count,
                    "Customer_Total_Spent": total_spent,
                    "Customer_Last_Order_ID": last_order_id,
                    "Customer_Province": customer_default_address_province,
                    "Customer_Country": customer_default_address_country,
                    "Customer_Name": customer_full_name.strip()
                })
            return pd.DataFrame(customers)


    class OrderDataFetcher:
        def __init__(self, json_file):
            self.json_file = json_file

        def get_orders(self):
            with open(self.json_file, 'r') as file:
                json_data = json.load(file)
            data = json_data
            orders = []
            # Extract and process order data
            for order_json in data:
                order_id = order_json.get('id')
                order_created_at = order_json.get('created_at')
                order_updated_at = order_json.get('updated_at')
                order_cancelled_at = order_json.get('cancelled_at')
                order_cancel_reason = order_json.get('cancel_reason')
                current_total_discounts = order_json.get('total_discount')
                total_price = order_json.get('total_price')
                currency = order_json.get('currency')
                refer_site = order_json.get('referring_site')
                source = order_json.get('source_name')

                customer = order_json.get('customer',
                                          {})  # This will default to an empty dictionary if 'customer' is None
                if customer:  # Check if customer is not None or an empty dictionary
                    customer_id = customer.get('id')
                    customer_name = customer.get('displayName')
                    customer_email = customer.get('email')
                else:
                    customer_id = None
                    customer_name = None
                    customer_email = None

                refund_amount = sum(
                    float(refund.get('order_adjustments', [{}])[0].get('amount', 0))
                    for refund in order_json.get('refunds', [])
                    if refund.get('order_adjustments')
                )

                for line_item in order_json.get('lineItems', []):
                    product_id = line_item.get('product_id')
                    quantity = line_item.get('quantity')
                    price = line_item.get('price')
                    productwise_discount = line_item.get('total_discount')
                    product_variant_id = line_item.get('variant_id')
                    product_name = line_item.get('title')

                    orders.append({
                        "Order_ID": order_id,
                        "Customer_ID": customer_id,
                        "Order_Created_At": order_created_at,
                        "Order_Updated_At": order_updated_at,
                        "Order_Cancelled_At": order_cancelled_at,
                        "Order_Cancel_Reason": order_cancel_reason,
                        "Product_ID": product_id,
                        "Product_Variant_Id": product_variant_id,
                        "Product_Quantity": quantity,
                        "Order_Total_Price": total_price,
                        "Currency": currency,
                        "Product_Price": price,
                        "Order_Total_Discount": current_total_discounts,
                        "Product_Discount": productwise_discount,
                        "Order_Refund_Amount": refund_amount,
                        "Order_Referring_Site": refer_site,
                        "Order_Source_Name": source,
                        "Product_Name": product_name,
                        "Customer_Name": customer_name,
                        "Customer_Email": customer_email,
                    })
            return pd.DataFrame(orders)


    class ProductDataFetcher:
        def __init__(self, json_file):
            self.json_file = json_file

        def get_products(self):
            with open(self.json_file, 'r') as file:
                json_data = json.load(file)
            data = json_data
            products = []
            for product_json in data:

                product_id = product_json.get("id")
                title = product_json.get("title")
                created_at = product_json.get("created_at")
                product_type = product_json.get("product_type", "Unknown")
                product_published_at = product_json.get("published_at")
                body_html = product_json.get("body_html")

                # Process options
                for option in product_json.get("options", []):
                    option_names = option.get("name")
                    option_value = option.get("values", [None])[0]

                # Process images
                for image in product_json.get("images", []):
                    image_ids = image.get("id")
                    image_sources = image.get("src")

                for variant in product_json.get("variants", []):
                    variant_id = variant.get("id")
                    price = float(variant.get("price", 0))
                    inventory_quantity = int(variant.get("inventory_quantity", 0))
                    variant_created_at = variant.get("created_at")
                    variant_title = variant.get("title")
                    # Storing product information in the list
                    products.append({
                        "Product_ID": product_id,
                        "Product_Title": title,
                        "Product_Type": product_type,
                        "Product_Published_At": product_published_at,
                        "Product_Variant_Id": variant_id,
                        "Variant_Price": price,
                        "Variant_Inventory_Quantity": inventory_quantity,
                        "Variant_Created_At": variant_created_at,
                        "Product_Created_At": created_at,
                        "Body_Html": body_html,
                        "Variant_Title": variant_title,
                        "Option_Names": option_names,
                        "Option_Values": option_value,
                        "Image_Ids": image_ids,
                        "Image_Sources": image_sources
                    })
            return pd.DataFrame(products)

        def get_low_inventory_products(self):
            df = self.get_products()
            return df[df["Variant_Inventory_Quantity"] > 0].sort_values(by="Variant_Inventory_Quantity", ascending=True)


    ####################################################################################
    def fetch_store_data(store_name):
        customer_file = f"{store_name}_customers.json"
        order_file = f"{store_name}_orders.json"
        product_file = f"{store_name}_products.json"
        # Check if customer file exists before attempting to fetch data
        if os.path.exists(customer_file):
            customer_df = CustomerDataFetcher(customer_file).get_customers()
        else:
            print(f"{customer_file} not found. Skipping customer data fetch.")
            customer_df = None  # Set to None if file doesn't exist
        if os.path.exists(order_file):
            order_df = OrderDataFetcher(order_file).get_orders()
        else:
            print(f"{order_file} not found. Skipping order data fetch.")
            order_df = None  # Set to None if file doesn't exist
        if os.path.exists(product_file):
            product_df = ProductDataFetcher(product_file).get_products()
        else:
            print(f"{product_file} not found. Skipping product data fetch.")
            product_df = None  # Set to None if file doesn't exist
        if customer_df is not None:
            # Process customer_df (e.g., remove duplicates, save CSV)
            customer_df.to_csv(
                f"D:\\Vinita\\Project\\Final_streamlit_Project\\data\\{store_name}_Customers_Dataset.csv", index=False)
            print(f"Customer data has been saved to {store_name}_Customers_Dataset.csv")
        if order_df is not None:
            # Process order_df (e.g., remove duplicates, save CSV)
            order_df.to_csv(f"D:\\Vinita\\Project\\Final_streamlit_Project\\data\\{store_name}_Orders_Dataset.csv",
                            index=False)
            print(f"Order data has been saved to {store_name}_Orders_Dataset.csv")
        if product_df is not None:
            # Process product_df (e.g., remove duplicates, save CSV)
            product_df.to_csv(f"D:\\Vinita\\Project\\Final_streamlit_Project\\data\\{store_name}_Products_Dataset.csv",
                              index=False)
            print(f"Product data has been saved to {store_name}_Products_Dataset.csv")

        return customer_df, order_df, product_df


    # Call the function
    customers_df, orders_df, products_df = fetch_store_data(store_name)
    # Print the dataframe information
    if customers_df is not None:
        print("\nCustomers DataFrame:\n")
        print(customers_df.info())
    if orders_df is not None:
        print("\nOrders DataFrame:\n")
        print(orders_df.info())
    if products_df is not None:
        print("\nProducts DataFrame:\n")
        print(products_df.info())


    class AbandonedDataFetcher:
        def __init__(self, json_file):
            self.json_file = json_file

        def get_abandoned(self):
            with open(self.json_file, 'r') as file:
                json_data = json.load(file)
            data = json_data
            abandoned_details = []
            for item in data:
                # Extract general order details
                order_id = item.get('id')
                created_at = item.get('created_at')
                updated_at = item.get('updated_at')
                referring_site = item.get('referring_site')
                total_discounts = item.get('total_discounts')
                total_price = item.get('total_price')
                currency = item.get('currency')

                # Extract customer details
                customer = item.get('customer', {})
                customer_id = customer.get('id')

                # Loop through line items to create separate rows for each product
                for line_item in item.get('lineItems', []):  # Ensure correct field name
                    product_id = line_item.get('product_id')
                    quantity = line_item.get('quantity')
                    variant_price = line_item.get('variant_price')
                    variant_id = line_item.get('variant_id')

                    # Append details to the list
                    abandoned_details.append({
                        "Order_ID": order_id,
                        "Order_Created_At": created_at,
                        "Order_Updated_At": updated_at,
                        "Order_Referring_Site": referring_site,
                        "Order_Total_Discount": total_discounts,
                        "Order_Total_Price": total_price,
                        "Product_ID": product_id,
                        "Product_Quantity": quantity,
                        "Variant_ID": variant_id,
                        "Currency": currency,
                        "Product_Variant_Price": variant_price,
                        "Customer_ID": customer_id
                    })

            df_abandoned_details = pd.DataFrame(abandoned_details)
            return df_abandoned_details


    # #####################################################################################
    abandoned_file = f"{store_name}_abandoned_checkouts.json"
    if os.path.exists(abandoned_file):
        abandoned_details_fetcher = AbandonedDataFetcher(abandoned_file)
        df_abandoned_details = abandoned_details_fetcher.get_abandoned()
        print(df_abandoned_details.duplicated().sum())
        df_abandoned_details.drop_duplicates(inplace=True)
        print(df_abandoned_details.duplicated().sum())
        df_abandoned_details.to_csv(
            f"D:\\Vinita\\Project\\Final_streamlit_Project\\data\\{store_name}_AbandonedCheckouts.csv", index=False)
        print(f"Abandoned checkouts data has been saved to {store_name}_AbandonedCheckouts.csv")
        print("\nAbandoned Checkouts DataFrame:\n")
        print(df_abandoned_details.info())
    else:
        print(f"{abandoned_file} not found. Skipping abandoned checkouts data fetch.")

    # Todo- add Customer_name in Cusomer_Journey-------------------------------------------------
    cj_file = fr"D:\\Vinita\\Project\\Final_streamlit_Project\\data\\{store_name}_CJ.csv"
    customer_file = fr"D:\\Vinita\\Project\\Final_streamlit_Project\\data\\{store_name}_Customers_Dataset.csv"
    if os.path.exists(cj_file) and os.path.exists(customer_file):
        dyori_cj_df = pd.read_csv(cj_file)
        dyori_customer_df = pd.read_csv(customer_file)
        dyori_cj_df['Customer_IP'] = dyori_cj_df['Customer_IP'].astype(str).str.replace('.0', '', regex=False)
        dyori_customer_df['Customer_ID'] = dyori_customer_df['Customer_ID'].astype(str)
        merged_df = pd.merge(dyori_cj_df, dyori_customer_df[['Customer_ID', 'Customer_Name']], left_on='Customer_IP',
                             right_on='Customer_ID', how='left')
        merged_df.to_csv(cj_file, index=False)
        print("Customer names have been successfully added!")
    else:
        print(f"Files not found. Skipping the process for {store_name}.")

