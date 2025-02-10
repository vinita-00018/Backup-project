from PIL import Image
import streamlit as st
import pandas as pd
import altair as alt
from datetime import datetime
from wordcloud import WordCloud
import io
import os
import plotly.graph_objects as go

def add_tooltip_css():
    st.markdown(f"""
        <style>
        .tooltip {{
            position: relative;
            display: inline-block;
            cursor: pointer;
            color: #007BFF; /* Blue color for the info icon */
            font-weight: normal;
            margin-left: 3px; /* Add spacing between text and icon */
            font-size: 12px; /* Adjust the size of the icon */
        }}
        .tooltip .tooltiptext {{
            visibility: hidden;
            width: 250px; /* Adjust the width of the tooltip box */
            background-color: #333; /* Dark background for contrast */
            color: #fff; /* White text for readability */
            text-align: left; /* Align text to the left */
            border-radius: 6px;
            padding: 8px; /* Add padding for better readability */
            position: absolute;
            z-index: 1;
            top: 50%; /* Align vertically with the icon */
            left: 110%; /* Position it to the right of the icon */
            transform: translateY(-50%); /* Center vertically */
            opacity: 0; /* Hidden by default */
            transition: opacity 0.3s; /* Smooth transition */
        }}
        .tooltip:hover .tooltiptext {{
            visibility: visible;
            opacity: 1;
        }}
        </style>
    """, unsafe_allow_html=True)

def render_tooltip(info_text: str, icon: str = "ℹ️") -> str:
    return f"""
    <div class="tooltip">{icon}
      <span class="tooltiptext">{info_text}</span>
    </div>
    """

st.set_page_config(
    page_title="Dashboard",
    page_icon="🔧",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.markdown(
    """
    <style>
    .css-1d3912d {  /* This selector targets the Streamlit's dark theme toggle button */
        background-color: #000;
        color: white;
    }
    </style>
    """, unsafe_allow_html=True
)

def add_custom_css():
    st.markdown(
        """
        <style>
        .card {
            background-color: #454545;
            color: white;
            padding: 20px;
            border-radius: 10px;
            text-align: center;
            font-size: 20px;
            box-shadow: 2px 2px 10px rgba(0,0,0,0.5);
        }
        .card-container {
            position: absolute;
            top: 20px;  /* Adjust to place the card lower or higher */
            right: 20px;  /* Adjust to place the card further left or right */
            display: flex;
            gap: 10px;
            margin-bottom: 100px;  /* Increased space below the cards */
        }
        </style>
        """,
        unsafe_allow_html=True
    )

# def load_data(file_path):
#     df = pd.read_csv(file_path, parse_dates=False)
#     date_columns = ['Order_Created_At', 'Order_Updated_At', 'Event_Time', 'Customer_Created_At', 'Customer_Updated_At',
#                     'Order_Created_At', 'Order_Updated_At', 'Variant_Created_At', 'Product_Created_At']
#     for col in df.columns:
#         if col in date_columns and df[col].dtype == 'object':
#             try:
#                 df[col] = pd.to_datetime(df[col], errors='coerce', utc=True)
#             except Exception as e:
#                 st.error(f"Error parsing column '{col}': {e}")
#     return df

def load_data(file_path, encoding='utf-8', parse_dates=True):
    try:
        df = pd.read_csv(file_path, encoding=encoding, parse_dates=False)
    except UnicodeDecodeError:
        st.error(f"Error reading file {file_path} with encoding {encoding}. Trying alternative encoding...")
        return None

    if parse_dates:
        date_columns = ['Order_Created_At', 'Order_Updated_At', 'Event_Time', 'Customer_Created_At','Customer_Updated_At',
                        'Order_Created_At', 'Order_Updated_At', 'Variant_Created_At', 'Product_Created_At']
        for col in df.columns:
            if col in date_columns and df[col].dtype == 'object':
                try:
                    df[col] = pd.to_datetime(df[col], errors='coerce', utc=True)
                except Exception as e:
                    st.error(f"Error parsing column '{col}': {e}")
    return df

def get_store_names(data_dir):
    files = os.listdir(data_dir)
    store_names = set()  # Using set to avoid duplicates
    for file in files:
        store_name = file.split('_')[0]
        store_names.add(store_name)
    return list(store_names)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
data_dir = os.path.join(BASE_DIR, "data")
store_names = get_store_names(data_dir)
store_select = st.sidebar.selectbox('Select Store', store_names)

if store_select:
    data_files = {
        'AbandonedCheckouts': f"{store_select}_AbandonedCheckouts.csv",
        'CJ': f"{store_select}_CJ.csv",
        'Customers_Dataset': f"{store_select}_Customers_Dataset.csv",
        'Orders_Dataset': f"{store_select}_Orders_Dataset.csv",
        'Products_Dataset': f"{store_select}_Products_Dataset.csv"
    }
    try:
        df_abandoned_checkouts = load_data(os.path.join(data_dir, data_files['AbandonedCheckouts']))
        if df_abandoned_checkouts is not None and df_abandoned_checkouts.empty:
            df_abandoned_checkouts = None
    except:
        df_abandoned_checkouts = None

    try:
        df_cj = load_data(os.path.join(data_dir, data_files['CJ']))
        if df_cj is not None and df_cj.empty:
            df_cj = None
    except:
        df_cj = None

    try:
        df_customers = load_data(os.path.join(data_dir, data_files['Customers_Dataset']))
        if df_customers is not None and df_customers.empty:
            df_customers = None
    except:
        df_customers = None

    try:
        df_orders = load_data(os.path.join(data_dir, data_files['Orders_Dataset']))
        if df_orders is not None and df_orders.empty:
            df_orders = None
    except:
        df_orders = None

    try:
        df_products = load_data(os.path.join(data_dir, data_files['Products_Dataset']), encoding='latin1')
        if df_products is not None and df_products.empty:
            df_products = None
    except:
        df_products = None
# def filter_by_date(df, date_column, label_prefix=""):
#     min_date = df[date_column].min().date()
#     max_date = df[date_column].max().date()
#     start_date = st.sidebar.date_input(f'{label_prefix}Start Date', min_value=min_date, max_value=max_date,value=min_date)
#     end_date = st.sidebar.date_input(f'{label_prefix}End Date', min_value=min_date, max_value=max_date, value=max_date)
#     if start_date and end_date:
#         start_date = pd.to_datetime(start_date).tz_localize('UTC')
#         end_date = pd.to_datetime(end_date).tz_localize('UTC')
#         df[date_column] = pd.to_datetime(df[date_column])
#         filtered_data = df[(df[date_column] >= start_date) & (df[date_column] <= end_date)]
#         return filtered_data
#     return df

def filter_by_date(df, date_column, label_prefix=""):
    df[date_column] = pd.to_datetime(df[date_column], errors='coerce')
    min_date = df[date_column].min().date()
    max_date = df[date_column].max().date()
    start_date = st.sidebar.date_input(f'{label_prefix}Start Date', min_value=min_date, max_value=max_date,value=min_date)
    end_date = st.sidebar.date_input(f'{label_prefix}End Date', min_value=min_date, max_value=max_date, value=max_date)
    if start_date and end_date:
        start_date = pd.to_datetime(start_date).tz_localize('UTC')
        end_date = pd.to_datetime(end_date).tz_localize('UTC')
        filtered_data = df[(df[date_column] >= start_date) & (df[date_column] <= end_date)]
        return filtered_data
    return df

def show_customer_data_page():
    try:
        st.title('Customer Data')
        add_custom_css()
        # Todo- Card Creation for the above
        col1, col2, col3 = st.columns(3)
        if df_customers is not None and not df_customers.empty:
            with col1:
                if df_customers is None or df_customers.empty:
                    st.markdown("<h1 style='color: blue; text-align: center;'>Listed Customers Data Unavailable</h1>",unsafe_allow_html=True)
                    st.markdown("""
                        <div style="border: 2px solid black; padding: 20px; background-color: #454545; border-radius: 10px; text-align: center;">
                            <h3 style="font-size: 30px; color: white; font-weight: bold;">No data available for listed customers.</h3>
                        </div>
                    """, unsafe_allow_html=True)
                else:
                    total_listed_customers = df_customers['Customer_ID'].nunique()
                    add_tooltip_css()
                    tooltip_html = render_tooltip(f"Total number of listed customers: {total_listed_customers}")
                    st.markdown(
                        f"<h1 style='display: inline-block;'>Listed Customer{tooltip_html}</h1>", unsafe_allow_html=True
                    )
                    st.markdown(
                        f"""
                        <div class="card">
                            <p>Total Listed Customers</p>
                            <h1>{total_listed_customers}</h1>
                        </div>
                        """,
                        unsafe_allow_html=True
                    )
            # Column 2: Paying Customers
            with col2:
                # if df_orders.empty:
                if df_orders is None or df_orders.empty:
                    st.title("Paying Customers")
                    st.markdown("""
                        <div style="border: 2px solid black; padding: 20px; background-color: #454545; border-radius: 10px; text-align: center;">
                            <h3 style="font-size: 30px; color: white; font-weight: bold;">No data available for paying customers.</h3>
                        </div>
                    """, unsafe_allow_html=True)
                else:
                    total_paying_customers = df_orders['Customer_ID'].nunique()
                    add_tooltip_css()
                    tooltip_html = render_tooltip(
                        f"Total number of customers who made a payment: {total_paying_customers}")
                    st.markdown(
                        f"<h1 style='display: inline-block;'>Paying Customers{tooltip_html}</h1>",
                        unsafe_allow_html=True
                    )
                    st.markdown(
                        f"""
                        <div class="card">
                            <p>Total Paying Customers</p>
                            <h1>{total_paying_customers}</h1>
                        </div>
                        """,
                        unsafe_allow_html=True
                    )
            # Column 3: Repeat Customers (with data validation)
            with col3:
                if df_orders is None or df_orders.empty or 'Customer_Name' not in df_orders.columns or 'Order_Total_Price' not in df_orders.columns:
                    st.title("Repeat Customers")
                    st.markdown("""
                        <div style="border: 2px solid black; padding: 20px; background-color: #454545; border-radius: 10px; text-align: center;">
                            <h3 style="font-size: 30px; color: white; font-weight: bold;">Repeat Customers is Unavailable.</h3>
                        </div>
                    """, unsafe_allow_html=True)
                else:
                    customer_summary = df_orders.groupby("Order_ID").agg(
                        Total_Spending=("Order_Total_Price", 'first'),
                        Customer_Name=("Customer_Name", 'first')
                    ).reset_index()
                    customer_summary1 = customer_summary.groupby("Customer_Name").agg(
                        Orders_Placed=("Order_ID", "nunique"),
                        Total_Spending=("Total_Spending", 'sum'),
                    ).reset_index()
                    customer_summary1 = customer_summary1[customer_summary1['Orders_Placed'] >= 2]
                    repeat_customers = customer_summary1.shape[0]
                    add_tooltip_css()
                    tooltip_html = render_tooltip(
                        f"Number of repeat customers who placed two or more orders: {repeat_customers}")
                    st.markdown(f"<h1 style='display: inline-block;'>Repeat Customers{tooltip_html}</h1>",
                                unsafe_allow_html=True)
                    st.markdown(
                        f"""
                        <div class="card">
                            <p>Repeat Customers</p>
                            <h1>{repeat_customers}</h1>
                        </div>
                        """,
                        unsafe_allow_html=True
                    )
        else:
            with col1:
                st.title("Listed Customer")
                st.markdown("""
                    <div style="border: 2px solid black; padding: 20px; background-color: #454545; border-radius: 10px; text-align: center;">
                        <h3 style="font-size: 30px; color: white; font-weight: bold;">No Data Available for Listed Customer</h3>
                    </div>
                """, unsafe_allow_html=True)
            with col2:
                st.title("Paying Customers")
                st.markdown("""
                                    <div style="border: 2px solid black; padding: 20px; background-color: #454545; border-radius: 10px; text-align: center;">
                                        <h3 style="font-size: 30px; color: white; font-weight: bold;"> No Data Available for Paying Customers</h3>
                                    </div>
                                """, unsafe_allow_html=True)
            with col3:
                st.title("Repeat Customers")
                st.markdown("""
                                <div style="border: 2px solid black; padding: 20px; background-color: #454545; border-radius: 10px; text-align: center;">
                                <h3 style="font-size: 30px; color: white; font-weight: bold;">No Data Available for Repeat Customers</h3>
                                </div>
                                """, unsafe_allow_html=True)
        if df_customers is not None and not df_customers.empty:
            add_tooltip_css()
            tooltip_html = render_tooltip("Preview of customer data filtered by the selected date range.")
            st.markdown(f"<h1 style='display: inline-block;'>Preview Filtered Customer Data {tooltip_html}</h1>",unsafe_allow_html=True)
            st.subheader("Customer Data")
            filtered_customers = filter_by_date(df_customers, 'Customer_Created_At')
            st.dataframe(filtered_customers, use_container_width=True)
        else:
            st.title("Preview of customer data filtered by the selected date range.")
            st.markdown("""
                            <div style="border: 2px solid black; padding: 20px; background-color: #454545; border-radius: 10px; text-align: center;">
                            <h3 style="font-size: 30px; color: white; font-weight: bold;">No Data Available for Preview of customer data filtered by the selected date range.</h3>
                            </div>
                            
                            """, unsafe_allow_html=True)

        # Todo- Customer Name Top 5 and Least 5 with Price Spends----------------------------------------
        chart_col1, chart_col2 = st.columns(2)
        if  df_orders is not None and not df_orders.empty:
            order_df = df_orders.drop_duplicates("Order_ID")
            order_data = order_df.groupby('Customer_Name')['Order_Total_Price'].sum().reset_index()
            order_data = order_data.dropna(subset=['Customer_Name'])
            top_5_customers = order_data.nlargest(50, 'Order_Total_Price')
            least_5_customers = order_data.nsmallest(50, 'Order_Total_Price')
            with chart_col1:
                if df_orders is None or df_orders.empty:
                    st.title("Highest Valued Customers")
                    st.markdown("""
                        <div style="border: 2px solid black; padding: 20px; background-color: #454545; border-radius: 10px; text-align: center;">
                            <h3 style="font-size: 30px; color: white; font-weight: bold;">No data available for top customers.</h3>
                        </div>
                    """, unsafe_allow_html=True)
                else:
                    add_tooltip_css()
                    tooltip_html = render_tooltip(
                        "This chart shows the top N customers ranked by their total order price. "
                        "Hover over the bars to see detailed information, including the customer's name and their total spending in euros."
                    )
                    st.markdown(
                        f"<h1 style='display: inline-block;'>Highest Valued Customers {tooltip_html}</h1>",
                        unsafe_allow_html=True
                    )
                    top_n = st.slider("Select Top N Customers to Display", min_value=1, max_value=50, value=5)

                    top_5_customers_filtered = top_5_customers.nlargest(top_n, 'Order_Total_Price')
                    st.markdown("<h3 style='text-align: center;'>Top N Customers by Total Order Price</h3>",unsafe_allow_html=True)

                    chart = alt.Chart(top_5_customers_filtered).mark_bar().encode(
                        x=alt.X('Customer_Name:O', title='Customer Name',sort=top_5_customers_filtered['Order_Total_Price'].tolist()),
                        y=alt.Y('Order_Total_Price:Q', title='Total Order Price (€)'),
                        color=alt.Color('Order_Total_Price:Q', legend=None),
                        tooltip=['Customer_Name:N', 'Order_Total_Price:Q']
                    ).properties(
                        width=700,
                        height=400,
                        title="Top N Customers by Total Order Price"
                    )
                    text = chart.mark_text(
                        align='center',
                        baseline='middle',
                        dy=-10,  # Adjust the vertical position of the text
                        fontSize=12
                    ).encode(
                        text='Order_Total_Price:Q'
                    )
                    final_chart = chart + text
                    final_chart = final_chart.configure_axis(
                        labelAngle=0,  # Horizontal x-axis labels
                        labelFontSize=12,
                        titleFontSize=14
                    )
                    st.altair_chart(final_chart, use_container_width=True)
            # Column 2: Least Customers
            with chart_col2:
                if df_orders is None or df_orders.empty:
                    st.title("Least Valued Customers")
                    st.markdown("""
                        <div style="border: 2px solid black; padding: 20px; background-color: #454545; border-radius: 10px; text-align: center;">
                            <h3 style="font-size: 30px; color: white; font-weight: bold;">No data available for least customers.</h3>
                        </div>
                    """, unsafe_allow_html=True)
                else:
                    add_tooltip_css()
                    tooltip_html = render_tooltip(
                        "This chart shows the Least N customers ranked by their total order price. "
                        "Hover over the bars to see detailed information, including the customer's name and their total spending in euros."
                    )
                    st.markdown(
                        f"<h1 style='display: inline-block;'>Least Valued Customers {tooltip_html}</h1>",
                        unsafe_allow_html=True
                    )
                    top_n = st.slider("Select Least N Customers to Display", min_value=1, max_value=50, value=5)
                    least_5_customers_filtered = least_5_customers.nsmallest(top_n, 'Order_Total_Price')
                    st.markdown("<h3 style='text-align: center;'>Least N Customers by Total Order Price</h3>",
                                unsafe_allow_html=True)

                    chart = alt.Chart(least_5_customers_filtered).mark_bar().encode(
                        x=alt.X('Customer_Name:O', title='Customer Name',
                                sort=least_5_customers_filtered['Order_Total_Price'].tolist()),
                        y=alt.Y('Order_Total_Price:Q', title='Total Order Price (€)'),
                        color=alt.Color('Order_Total_Price:Q', legend=None),
                        tooltip=['Customer_Name:N', 'Order_Total_Price:Q']
                    ).properties(
                        width=700,
                        height=400,
                        title="Least N Customers by Total Order Price"
                    )
                    text = chart.mark_text(
                        align='center',
                        baseline='middle',
                        dy=-10,  # Adjust the vertical position of the text
                        fontSize=12
                    ).encode(
                        text='Order_Total_Price:Q'
                    )
                    final_chart = chart + text
                    final_chart = final_chart.configure_axis(
                        labelAngle=0,  # Horizontal x-axis labels
                        labelFontSize=12,
                        titleFontSize=14
                    )
                    st.altair_chart(final_chart, use_container_width=True)
        else:
            with chart_col1:
                st.title("Highest Valued Customers")
                st.markdown("""
                    <div style="border: 2px solid black; padding: 20px; background-color: #454545; border-radius: 10px; text-align: center;">
                        <h3 style="font-size: 30px; color: white; font-weight: bold;">No data available for top customers.</h3>
                    </div>
                """, unsafe_allow_html=True)
            with chart_col2:
                st.title("Least Valued Customers")
                st.markdown("""
                    <div style="border: 2px solid black; padding: 20px; background-color: #454545; border-radius: 10px; text-align: center;">
                        <h3 style="font-size: 30px; color: white; font-weight: bold;">No data available for least customers.</h3>
                    </div>
                """, unsafe_allow_html=True)

        # Todo- Customer Summary Table with Total Spend- Unique Customer Names----------------------------------------
        if df_orders is not None and not df_orders.empty:
            customer_summary = df_orders.groupby("Order_ID").agg(
                Total_Spending=("Order_Total_Price", 'first'),
                Customer_Name=("Customer_Name", "first")
            ).reset_index()
            # Aggregating Data by Customer
            customer_summary1 = customer_summary.groupby("Customer_Name").agg(
                Orders_Placed=("Order_ID", "nunique"),
                Total_Spending=("Total_Spending", 'sum'),
            ).reset_index()
            # Filter Customers with at least 2 orders
            customer_summary1 = customer_summary1[customer_summary1['Orders_Placed'] >= 2]
            customer_summary1 = customer_summary1.reset_index(drop=True)
            if not customer_summary1.empty:
                add_tooltip_css()
                tooltip_html = render_tooltip("This table summarizes customer data, including the total number of unique orders placed and the total spending for each customer. Only customers with at least two orders are included in this filtered view. Expand the section to preview the detailed data.")
                st.markdown(f"<h1 style='display: inline-block;'>Customer Order Summary{tooltip_html}</h1>",
                            unsafe_allow_html=True)
                st.subheader("Summary Table")

                # Display the Customer Summary Table
                st.dataframe(customer_summary1, use_container_width=True)
            else:
                st.title("Customer Order Summary")
                st.markdown("""
                    <div style="border: 2px solid black; padding: 20px; background-color: #454545; border-radius: 10px; text-align: center;">
                        <h3 style="font-size: 30px; color: white; font-weight: bold;">No data available for customers with two or more orders.</h3>
                    </div>
                """, unsafe_allow_html=True)
        else:
            st.title("Customer Order Summary")
            st.markdown("""
                <div style="border: 2px solid black; padding: 20px; background-color: #454545; border-radius: 10px; text-align: center;">
                    <h3 style="font-size: 30px; color: white; font-weight: bold;">No data available for customers with two or more orders.</h3>
                </div>
            """, unsafe_allow_html=True)

        # Todo Bar Graph for Customer Province Data and Country Data with Unique Count-----------------------------
        chart_col1, chart_col2 = st.columns(2)
        if df_customers is not None and not df_customers.empty:
            province_data = df_customers.groupby("Customer_Province")["Customer_ID"].nunique().reset_index()
            province_data = province_data.rename(columns={"Customer_ID": "Unique_Customers"})
            country_data = df_customers.groupby("Customer_Country")["Customer_ID"].nunique().reset_index()
            country_data = country_data.rename(columns={"Customer_ID": "Unique_Customers"})
            with chart_col1:
                add_tooltip_css()
                tooltip_html = render_tooltip("This chart visualizes the number of unique customers segmented by their provinces. The bars represent the total count of unique customers for each province, with exact numbers displayed above the bars. Hover over the bars to view the province name and corresponding unique customer count.")
                st.markdown(f"<h1 style='display: inline-block;'>Customer Province{tooltip_html}</h1>",unsafe_allow_html=True)
                st.markdown("<h3 style='text-align: center;'>Unique Customers by Province</h3>", unsafe_allow_html=True)
                # Check if province data is not empty
                if df_customers is not None and not df_customers.empty and not province_data.empty:
                    province_chart = alt.Chart(province_data).mark_bar().encode(
                        x=alt.X("Customer_Province:O", title="Customer Province", sort="-y"),
                        y=alt.Y("Unique_Customers:Q", title="Number of Unique Customers"),
                        color=alt.Color("Customer_Province:N", legend=None),
                        tooltip=["Customer_Province", "Unique_Customers"]
                    ).properties(width=350, height=300)
                    province_chart_text = province_chart.mark_text(
                        align='center',
                        baseline='middle',
                        dy=-10,
                        fontSize=12
                    ).encode(
                        text='Unique_Customers:Q'
                    )
                    province_chart = province_chart + province_chart_text
                    province_chart = province_chart.configure_axis(
                        labelAngle=90,
                        labelFontSize=14,
                        titleFontSize=16
                    )
                    st.altair_chart(province_chart, use_container_width=True)
                else:
                    # st.title("Unique Customers by Province")
                    st.markdown("""
                        <div style="border: 2px solid black; padding: 20px; background-color: #454545; border-radius: 10px; text-align: center;">
                            <h3 style="font-size: 30px; color: white; font-weight: bold;">No data available for unique customers by province.</h3>
                        </div>
                    """, unsafe_allow_html=True)
            # Country Chart
            with chart_col2:
                add_tooltip_css()
                tooltip_html = render_tooltip(
                    "This chart illustrates the number of unique customers grouped by their respective countries. Each bar represents the total count of unique customers for a specific country, with the exact values displayed above the bars. Hover over the bars to see detailed information, including the country name and the corresponding customer count."
                )
                st.markdown(f"<h1 style='display: inline-block;'>Customer Country{tooltip_html}</h1>",
                            unsafe_allow_html=True)
                st.markdown("<h3 style='text-align: center;'>Unique Customers by Country</h3>", unsafe_allow_html=True)

                if df_customers is not None and not df_customers.empty and not country_data.empty:
                    country_chart = alt.Chart(country_data).mark_bar().encode(
                        x=alt.X("Customer_Country:O", title="Customer Country", sort="-y"),
                        y=alt.Y("Unique_Customers:Q", title="Number of Unique Customers"),
                        color=alt.Color("Unique_Customers:Q", legend=None),
                        tooltip=["Customer_Country", "Unique_Customers"]
                    ).properties(width=350, height=300)

                    country_chart_text = country_chart.mark_text(
                        align='center',
                        baseline='middle',
                        dy=-10,
                        fontSize=12
                    ).encode(
                        text='Unique_Customers:Q'
                    )
                    country_chart = country_chart + country_chart_text
                    country_chart = country_chart.configure_axis(
                        labelAngle=0,
                        labelFontSize=14,
                        titleFontSize=16
                    )
                    st.altair_chart(country_chart, use_container_width=True)
                else:
                    # st.title("Unique Customers by Country")
                    st.markdown("""
                        <div style="border: 2px solid black; padding: 20px; background-color: #454545; border-radius: 10px; text-align: center;">
                            <h3 style="font-size: 30px; color: white; font-weight: bold;">No data available for unique customers by country.</h3>
                        </div>
                    """, unsafe_allow_html=True)
        else:
            with chart_col1:
                st.title("Unique Customers by Province")
                st.markdown("""
                    <div style="border: 2px solid black; padding: 20px; background-color: #454545; border-radius: 10px; text-align: center;">
                        <h3 style="font-size: 30px; color: white; font-weight: bold;">No data available for unique customers by province.</h3>
                    </div>
                """, unsafe_allow_html=True)
            with chart_col2:
                st.title("Unique Customers by Country")
                st.markdown("""
                    <div style="border: 2px solid black; padding: 20px; background-color: #454545; border-radius: 10px; text-align: center;">
                        <h3 style="font-size: 30px; color: white; font-weight: bold;">No data available for unique customers by country.</h3>
                    </div>
                """, unsafe_allow_html=True)
    except:
        st.markdown(
            "<h3 style='font-size: 30px; color: red; text-align: center;'><b>Dataset is currently unavailable.</b></h3>",
            unsafe_allow_html=True)

def show_cj_page():
    try:
        st.title('Customer Journey Data')
        add_custom_css()
        # Todo-Customer Journey Diagram:
        if df_cj is not None and not df_cj.empty:
            add_tooltip_css()
            tooltip_html = render_tooltip(f"Customer Journey from start to end on every page")
            st.markdown(f"<h1 style='display: inline-block;'>Customer Journey {tooltip_html}</h1>",unsafe_allow_html=True)
            df_filtered = df_cj[['Event', 'Customer_IP']]
            # Remove rows where Event is NaN
            df_filtered = df_filtered.dropna(subset=['Event'])
            # Remove duplicates to get unique customer events
            df_filtered = df_filtered.drop_duplicates(subset=['Customer_IP', 'Event'])
            # Create a list of events in order
            event_order = ['Home', 'Collection', 'Search', 'Product', 'Cart', 'Cart Add','Cart Update','Cart Remove']  # Adjust based on actual events
            # Sort the events by their order
            df_filtered['Event'] = pd.Categorical(df_filtered['Event'], categories=event_order, ordered=True)

            df_filtered = df_filtered.sort_values(by=['Customer_IP', 'Event'])
            # Create a list to track the transition of customers
            transitions = []
            # Track how many customers are at each event
            event_counts = df_filtered['Event'].value_counts().to_dict()
            for customer in df_filtered['Customer_IP'].unique():
                customer_events = df_filtered[df_filtered['Customer_IP'] == customer]['Event'].tolist()
                for i in range(1, len(customer_events)):
                    transitions.append((customer_events[i - 1], customer_events[i]))
            # Count the number of customers transitioning between events
            transition_counts = pd.Series(transitions).value_counts()
            # Prepare the nodes and links for the Sankey diagram
            unique_events = list(event_order)
            node_mapping = {event: index for index, event in enumerate(unique_events)}
            # Create nodes (Event Types)
            nodes = unique_events
            # Create links (Transitions)
            links = []
            drop_offs = {}  # Dictionary to store drop-off counts
            for (from_event, to_event), count in transition_counts.items():
                # Check if the events are valid before creating links
                if from_event in node_mapping and to_event in node_mapping:
                    links.append({
                        'source': node_mapping[from_event],
                        'target': node_mapping[to_event],
                        'value': count
                    })
                    total_at_event = event_counts.get(from_event, 0)
                    total_transitions = sum(link['value'] for link in links if nodes[link['source']] == from_event)
                    drop_off_count = total_at_event - total_transitions
                    drop_offs[(from_event, to_event)] = drop_off_count
            link_labels = []
            for (from_event, to_event), count in transition_counts.items():
                drop_off_count = drop_offs.get((from_event, to_event), 0)
                link_labels.append(f"Drop-off: {drop_off_count}")
            sankey_figure = go.Figure(go.Sankey(
                node=dict(
                    pad=20,
                    thickness=20,
                    line=dict(color="black", width=0.5),
                    label=nodes,
                    color=["#ff9259", "#66b3ff", "#99ff99", "#ffcc99", "#ff9789", "#66b2ff", "#99ff19", "#ffcc69"]
                ),
                link=dict(
                    source=[link['source'] for link in links],
                    target=[link['target'] for link in links],
                    value=[link['value'] for link in links],
                    # color=["rgba(255, 0, 0, 0.4)" for _ in links],  # Link color can be adjusted here
                    color=["rgba(30, 144, 255, 0.4)" for _ in links],  # Link color can be adjusted here
                    # color=["rgba(220, 20, 60, 0.4)" for _ in links],  # Link color can be adjusted here
                    label=link_labels  # Add the link labels with drop-off count
                )
            ))
            st.plotly_chart(sankey_figure, use_container_width=True)
        else:
            st.title("Customer Journey From start to end")
            st.markdown("""
                            <div style="border: 2px solid black; padding: 20px; background-color: #454545; border-radius: 10px; text-align: center;">
                                <h3 style="font-size: 30px; color: white; font-weight: bold;">No Customer Journey Data Available</h3>
                            </div>
                            """, unsafe_allow_html=True)


        # Todo- Card Creation for the above
        col1, col2, col3 = st.columns(3)
        if df_cj is not None and not df_cj.empty:
            with col1:
                total_listed_customers = df_cj['Customer_IP'].nunique()
                add_tooltip_css()
                tooltip_html = render_tooltip(f"Total unique customers: {total_listed_customers}")
                st.markdown(f"<h1 style='display: inline-block;'>Total Viewers {tooltip_html}</h1>",
                            unsafe_allow_html=True)
                st.markdown(
                    f"""
                        <div class="card">
                            <p>Total Viewers</p>
                            <h1>{total_listed_customers}</h1>
                        </div>
                    """,
                    unsafe_allow_html=True
                )

            with col2:
                max_session_df = df_cj.loc[df_cj.groupby('Customer_IP')['session'].idxmax()]
                customer_summary1 = max_session_df[max_session_df['session'] >= 2]
                repeat_customers = customer_summary1.shape[0]
                add_tooltip_css()
                tooltip_html = render_tooltip(
                    f"Total repeat customers (with 2 or more sessions): {repeat_customers}")
                st.markdown(f"<h1 style='display: inline-block;'>Repeat Customer {tooltip_html}</h1>",
                            unsafe_allow_html=True)
                st.markdown(
                    f"""
                        <div class="card">
                            <p>Repeat Viewers</p>
                            <h1>{repeat_customers}</h1>
                        </div>
                    """,
                    unsafe_allow_html=True
                )

            with col3:
                max_session_df = df_cj.loc[df_cj.groupby('Customer_IP')['session'].idxmax()]
                session_sum = max_session_df['session'].sum()
                add_tooltip_css()
                tooltip_html = render_tooltip(
                    f"Total sessions from customers with the highest session count: {session_sum}")
                st.markdown(f"<h1 style='display: inline-block;'>Total Sessions {tooltip_html}</h1>",
                            unsafe_allow_html=True)
                st.markdown(
                    f"""
                        <div class="card">
                            <p>Total Sessions</p>
                            <h1>{session_sum}</h1>
                        </div>
                    """,
                    unsafe_allow_html=True
                )
        else:
            with col1:
                st.title("Total Viewers")
                st.markdown("""
                    <div style="border: 2px solid black; padding: 20px; background-color: #454545; border-radius: 10px; text-align: center;">
                        <h3 style="font-size: 30px; color: white; font-weight: bold;">No Data Available for Total Viewers</h3>
                    </div>
                """, unsafe_allow_html=True)
            with col2:
                st.title("Repeat Viewers")
                st.markdown("""
                                    <div style="border: 2px solid black; padding: 20px; background-color: #454545; border-radius: 10px; text-align: center;">
                                        <h3 style="font-size: 30px; color: white; font-weight: bold;"> No Data Available Repeat Viewers</h3>
                                    </div>
                                """, unsafe_allow_html=True)
            with col3:
                st.title("Total Sessions")
                st.markdown("""
                                <div style="border: 2px solid black; padding: 20px; background-color: #454545; border-radius: 10px; text-align: center;">
                                    <h3 style="font-size: 30px; color: white; font-weight: bold;">No Data Available Total Sessions</h3>
                                </div>
                                """, unsafe_allow_html=True)
        if df_cj is not None and not df_cj.empty:
            add_tooltip_css()
            tooltip_html = render_tooltip("Preview of customer journey data filtered by the selected date range.")
            st.markdown(f"<h1 style='display: inline-block;'>Preview Filtered Customer Journey Data {tooltip_html}</h1>",
                        unsafe_allow_html=True)
            st.subheader("Customer Journey Data")
            filtered_cj = filter_by_date(df_cj, 'Event_Time')
            # with st.expander("Preview Filtered CJ Data"):
            st.dataframe(filtered_cj, use_container_width=True)
        else:
            st.title("Preview of customer journey data filtered by the selected date range")
            st.markdown("""
                            <div style="border: 2px solid black; padding: 20px; background-color: #454545; border-radius: 10px; text-align: center;">
                                <h3 style="font-size: 30px; color: white; font-weight: bold;">No Data Preview of customer journey data filtered by the selected date range</h3>
                            </div>
                            """, unsafe_allow_html=True)

        # #Todo- Session details on weekdays and Weekends
        col1, col2 = st.columns(2)
        if df_cj is not None and not df_cj.empty:
            try:
                df_temp = df_cj.copy()
                df_temp["Event_Time"] = pd.to_datetime(df_temp["Event_Time"], errors='coerce')
                df_temp = df_temp.dropna(subset=["Event_Time"])
                with col1:
                    add_tooltip_css()
                    tooltip_html = render_tooltip("This chart compares the total number of sessions on weekdays and weekends based on event timestamps. The data is grouped by unique customer sessions.")
                    st.markdown(f"<h1 style='display: inline-block;'>Session :Weekday,Weekend {tooltip_html}</h1>",unsafe_allow_html=True)
                    df_temp["Weekday_Weekend"] = df_temp["Event_Time"].dt.dayofweek.apply(lambda x: "Weekend" if x >= 5 else "Weekday")
                    grouped_filtered_df = df_temp.groupby(["Customer_IP", "session"]).first().reset_index()
                    weekday_count = grouped_filtered_df[grouped_filtered_df["Weekday_Weekend"] == "Weekday"].shape[0]
                    weekend_count = grouped_filtered_df[grouped_filtered_df["Weekday_Weekend"] == "Weekend"].shape[0]
                    counts = [weekday_count, weekend_count]
                    labels = ["Weekday", "Weekend"]
                    total_sessions = sum(counts)
                    pie_data = pd.DataFrame({
                        "Category": labels,
                        "Count": counts,
                        "Percentage": [(count / total_sessions) * 100 if total_sessions > 0 else 0 for count in counts]
                    })
                    pie_chart = alt.Chart(pie_data).mark_arc(size=200).encode(
                        theta=alt.Theta(field="Count", type="quantitative"),
                        color=alt.Color(field="Category", type="nominal"),
                        tooltip=["Category", "Count", "Percentage"]
                    ).properties(width=300, height=300)
                    st.altair_chart(pie_chart, use_container_width=True)
                with col2:
                    add_tooltip_css()
                    tooltip_html = render_tooltip(
                        "This chart displays the total number of sessions across different days of the week. Each segment represents the session count for a particular day.")
                    st.markdown(
                        f"<h1 style='display: inline-block;'>Sessions: Days of the Week {tooltip_html}</h1>",
                        unsafe_allow_html=True)
                    grouped_filtered_df["days_of_week"] = grouped_filtered_df["Event_Time"].dt.dayofweek.apply(
                        lambda x: ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"][x]
                    )
                    day_count = grouped_filtered_df["days_of_week"].value_counts()
                    day_count = day_count.reindex(
                        ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"], fill_value=0)
                    pie_data = pd.DataFrame({
                        "Day": day_count.index,
                        "Count": day_count.values
                    })
                    pie_chart = alt.Chart(pie_data).mark_arc(size=200).encode(
                        theta="Count:Q",
                        color=alt.Color("Day:N",sort=["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday","Sunday"]),
                        tooltip=["Day:N", "Count:Q"]
                    ).properties(width=300, height=300)

                    # Display Chart
                    st.altair_chart(pie_chart, use_container_width=True)

            except Exception as e:
                st.error(f"An error occurred: {e}")
        else:
            with col1:
                st.title("Total sessions:Weekday,Weekend")
                st.markdown("""
                    <div style="border: 2px solid black; padding: 20px; background-color: #454545; border-radius: 10px; text-align: center;">
                        <h3 style="font-size: 30px; color: white; font-weight: bold;">No Data Available for Total Sessions: Weekday vs Weekend</h3>
                    </div>
                """, unsafe_allow_html=True)

            with col2:
                st.title("Total Sessions by Day of the Week")
                st.markdown("""
                    <div style="border: 2px solid black; padding: 20px; background-color: #454545; border-radius: 10px; text-align: center;">
                        <h3 style="font-size: 30px; color: white; font-weight: bold;">No Data Available for Total Sessions by Day of the Week</h3>
                    </div>
                """, unsafe_allow_html=True)

        # Todo-Session per hours---------------------------------------------
        col1 = st.columns(1)[0]
        if df_cj is not None and not df_cj.empty:
            with col1:
                add_tooltip_css()
                tooltip_html = render_tooltip(
                    "This chart displays the total number of sessions by hour of the day. The line chart shows how sessions are distributed across different hours, with each point representing the number of sessions for a specific hour. Hover over the points or the line to see the number of sessions for each hour. The data is based on unique sessions for each customer IP, and the hours are shifted to a 1-24 range."
                )
                st.markdown(f"<h1 style='display: inline-block;'>Total sessions: hours of day {tooltip_html}</h1>",
                            unsafe_allow_html=True)

                # Convert to datetime
                df_cj['Event_Time'] = pd.to_datetime(df_cj['Event_Time'], errors='coerce', utc=True)
                dyori_cj_df = df_cj.dropna(subset=['Event_Time'])

                if not dyori_cj_df.empty:
                    # Process hour data
                    dyori_cj_df['hour_of_day'] = dyori_cj_df['Event_Time'].dt.hour + 1  # Shift hours to 1-24 range
                    grouped_filtered_df = dyori_cj_df.groupby(['Customer_IP', 'session']).first().reset_index()
                    hour_count = grouped_filtered_df['hour_of_day'].value_counts().sort_index()
                    hour_count = hour_count.reindex(range(1, 25), fill_value=0)

                    # Create DataFrame
                    hour_data = pd.DataFrame({
                        'Hour of Day': hour_count.index,
                        'Number of Sessions': hour_count.values
                    })

                    # Line Chart
                    line_chart = alt.Chart(hour_data).mark_line().encode(
                        x='Hour of Day:O',
                        y='Number of Sessions:Q',
                        tooltip=['Hour of Day:N', 'Number of Sessions:Q']
                    ).properties(title="Total Sessions by Hour of the Day")

                    # Points for Emphasis
                    points = alt.Chart(hour_data).mark_point(size=60, color='red').encode(
                        x='Hour of Day:O',
                        y='Number of Sessions:Q',
                        tooltip=['Hour of Day:N', 'Number of Sessions:Q']
                    )

                    # Combine and Display
                    st.altair_chart(line_chart + points, use_container_width=True)

                else:
                    st.title("Total Sessions by Hour of the Day")
                    st.markdown(""" 
                        <div style="border: 2px solid black; padding: 20px; background-color: #454545; border-radius: 10px; text-align: center;">
                            <h3 style="font-size: 30px; color: white; font-weight: bold;">Total Sessions by Hour of the Day No Data Available</h3>
                        </div>
                    """, unsafe_allow_html=True)
        else:
            with col1:
                st.title("Total sessions: hours of day")
                st.markdown("""
                    <div style="border: 2px solid black; padding: 20px; background-color: #454545; border-radius: 10px; text-align: center;">
                        <h3 style="font-size: 30px; color: white; font-weight: bold;">No Data Available for Total sessions: hours of day</h3>
                    </div>
                """, unsafe_allow_html=True)

        # Todo-Total sessions: day, month, quarter, year
        if df_cj is not None and not df_cj.empty:
            df_cj['day'] = df_cj['Event_Time'].dt.date
            df_cj['session'] = pd.to_numeric(df_cj['session'], errors='coerce')
            # Compute session counts
            session_count_per_customer = df_cj.groupby(['Customer_IP', 'day'])['session'].nunique().reset_index()
            session_count_per_day = session_count_per_customer.groupby('day')['session'].sum().reset_index()
            if not session_count_per_day.empty:
                # Create a complete date range
                start_date = session_count_per_day['day'].min()
                end_date = datetime.today().date()
                full_date_range = pd.date_range(start=start_date, end=end_date)

                # Reindex session_count_per_day to include all dates
                session_count_per_day.set_index('day', inplace=True)
                session_count_per_day = session_count_per_day.reindex(full_date_range, fill_value=0).reset_index()
                session_count_per_day.rename(columns={'index': 'day', 'session': 'session_count'}, inplace=True)

                # Monthly Data
                session_count_per_day['month'] = session_count_per_day['day'].dt.strftime('%Y-%m')
                session_count_per_month = session_count_per_day.groupby('month')['session_count'].sum().reset_index()

                # Ensure months are sorted chronologically
                session_count_per_month['month'] = pd.to_datetime(session_count_per_month['month'], format='%Y-%m')
                session_count_per_month = session_count_per_month.sort_values(by='month', ascending=True)

                # Quarterly Data
                session_count_per_day['quarter'] = session_count_per_day['day'].dt.to_period('Q').astype(str)
                session_count_per_quarter = session_count_per_day.groupby('quarter')[
                    'session_count'].sum().reset_index()

                # Yearly Data
                session_count_per_day['Year'] = session_count_per_day['day'].dt.year
                session_count_per_year = session_count_per_day.groupby('Year')['session_count'].sum().reset_index()
            else:
                session_count_per_month = None
                session_count_per_quarter = None
                session_count_per_year = None

            # Tooltip and Title
            add_tooltip_css()
            tooltip_html = render_tooltip(
                "This visualization displays session counts across different time periods (day, month, quarter, year). The x-axis represents the time period, and the y-axis shows the total session count. Use the radio buttons above to switch between views. Hover over the points for detailed session information."
            )
            st.markdown(f"<h1 style='display: inline-block;'>Session Count Visualizations {tooltip_html}</h1>",
                        unsafe_allow_html=True)

            # Radio selection
            view = st.radio("Select View",
                            ['Sessions per Day', 'Sessions per Month', 'Sessions per Quarter', 'Sessions per Year'])

            # Sessions per Day
            if view == 'Sessions per Day' and not session_count_per_day.empty:
                st.write("### Sessions per Day")
                chart_day = alt.Chart(session_count_per_day).mark_line().encode(
                    x=alt.X('day:T', title='Date', axis=alt.Axis(format="%b %d, %Y", labelAngle=90)),
                    y=alt.Y('session_count:Q', title='Number of Sessions')
                ).properties(title='Sessions per Day')

                points_day = alt.Chart(session_count_per_day).mark_point(size=60, color='red').encode(
                    x='day:T', y='session_count:Q', tooltip=['day:T', 'session_count:Q']
                )

                st.altair_chart(chart_day + points_day, use_container_width=True)

            elif view == 'Sessions per Day':
                st.title("Sessions per Day")
                st.markdown(""" 
                    <div style="border: 2px solid black; padding: 20px; background-color: #454545; border-radius: 10px; text-align: center;">
                        <h3 style="font-size: 30px; color: white; font-weight: bold;">Sessions per Day No Data Available</h3>
                    </div>
                """, unsafe_allow_html=True)

            # Sessions per Month
            if view == 'Sessions per Month' and session_count_per_month is not None:
                st.write("### Sessions per Month")
                chart_month = alt.Chart(session_count_per_month).mark_line().encode(
                    x=alt.X('month:T', title=None, axis=alt.Axis(tickCount=5, format='%b %Y', labelAngle=-90)),
                    y='session_count:Q'
                ).properties(title='Sessions per Month')

                points_month = alt.Chart(session_count_per_month).mark_point(size=60, color='red').encode(
                    x='month:T', y='session_count:Q', tooltip=['month:T', 'session_count:Q']
                )

                st.altair_chart(chart_month + points_month, use_container_width=True)

            elif view == 'Sessions per Month':
                st.title("Sessions per Month")
                st.markdown(""" 
                    <div style="border: 2px solid black; padding: 20px; background-color: #454545; border-radius: 10px; text-align: center;">
                        <h3 style="font-size: 30px; color: white; font-weight: bold;">Sessions per Month No Data Available</h3>
                    </div>
                """, unsafe_allow_html=True)
            # Sessions per Quarter
            if view == 'Sessions per Quarter' and session_count_per_quarter is not None:
                st.write("### Sessions per Quarter")
                chart_quarter = alt.Chart(session_count_per_quarter).mark_line().encode(
                    x='quarter:N', y='session_count:Q'
                ).properties(title='Sessions per Quarter')

                points_quarter = alt.Chart(session_count_per_quarter).mark_point(size=60, color='red').encode(
                    x='quarter:N', y='session_count:Q', tooltip=['quarter:N', 'session_count:Q']
                )
                st.altair_chart(chart_quarter + points_quarter, use_container_width=True)

            elif view == 'Sessions per Quarter':
                st.title("Sessions per Quarter")
                st.markdown(""" 
                    <div style="border: 2px solid black; padding: 20px; background-color: #454545; border-radius: 10px; text-align: center;">
                        <h3 style="font-size: 30px; color: white; font-weight: bold;">Sessions per Quarter No Data Available</h3>
                    </div>
                """, unsafe_allow_html=True)

            # Sessions per Year
            if view == 'Sessions per Year' and session_count_per_year is not None:
                st.write("### Sessions per Year")
                chart_year = alt.Chart(session_count_per_year).mark_line().encode(
                    x='Year:N', y='session_count:Q'
                ).properties(title='Sessions per Year')

                points_year = alt.Chart(session_count_per_year).mark_point(size=60, color='red').encode(
                    x='Year:N', y='session_count:Q', tooltip=['Year:N', 'session_count:Q']
                )
                st.altair_chart(chart_year + points_year, use_container_width=True)

            elif view == 'Sessions per Year':
                st.title("Sessions per Year")
                st.markdown(""" 
                    <div style="border: 2px solid black; padding: 20px; background-color: #454545; border-radius: 10px; text-align: center;">
                        <h3 style="font-size: 30px; color: white; font-weight: bold;">Sessions per Year No Data Available</h3>
                    </div>
                """, unsafe_allow_html=True)
        else:
            st.title("Total sessions: day, month, quarter, year")
            st.markdown(""" 
                <div style="border: 2px solid black; padding: 20px; background-color: #454545; border-radius: 10px; text-align: center;">
                    <h3 style="font-size: 30px; color: white; font-weight: bold;">No Data Available for Total sessions: day, month, quarter, year</h3>
                </div>
            """, unsafe_allow_html=True)

        # Todo-Total session duration-Average session duration-Least session duration-Highest session duration
        col1, col2, col3 = st.columns(3)
        if df_cj is not None and not df_cj.empty:
            def convert_seconds(seconds):
                hours = int(seconds // 3600)
                minutes = int((seconds % 3600) // 60)
                return f"{hours} hr {minutes} mini"
            df_cj['Event_Time'] = pd.to_datetime(df_cj['Event_Time'])
            groupby_session = df_cj.groupby(['session', 'Customer_IP']).agg(
                Time_On_Page=('Time_On_Page', 'sum')
            ).reset_index()

            # Calculate Overall Sum and Average
            if not groupby_session.empty and 'Time_On_Page' in groupby_session.columns:
                overall_sum = convert_seconds(groupby_session['Time_On_Page'].sum())
                overall_average = convert_seconds(groupby_session['Time_On_Page'].mean())
            else:
                overall_sum = None
                overall_average = None
            # Calculate Average Sessions Per Customer
            session_count_per_customer = df_cj.groupby('Customer_IP')['session'].max().reset_index()
            if not session_count_per_customer.empty and 'session' in session_count_per_customer.columns:
                average_sessions_per_customer = round(session_count_per_customer['session'].mean(), 2)
            else:
                average_sessions_per_customer = None
            # Column 1: Total Session Duration
            with col1:
                if overall_sum:
                    add_tooltip_css()
                    tooltip_html = render_tooltip(
                        f"The total time spent on the website across all sessions is {overall_sum}.")
                    st.markdown(f"<h1 style='display: inline-block;'>Session Duration {tooltip_html}</h1>",
                                unsafe_allow_html=True)
                    st.markdown(
                        f"""
                            <div class="card">
                                <p>Total session duration</p>
                                <h1>{overall_sum}</h1>
                            </div>
                        """,
                        unsafe_allow_html=True
                    )
                else:
                    st.title("Session Duration")
                    st.markdown(""" 
                        <div style="border: 2px solid black; padding: 20px; background-color: #454545; border-radius: 10px; text-align: center;">
                            <h3 style="font-size: 30px; color: white; font-weight: bold;">No Data Available</h3>
                        </div>
                    """, unsafe_allow_html=True)
            # Column 2: Average Session Duration
            with col2:
                if overall_average:
                    add_tooltip_css()
                    tooltip_html = render_tooltip(
                        f"The average time spent per session is {overall_average}.")
                    st.markdown(f"<h1 style='display: inline-block;'>Average Duration {tooltip_html}</h1>",
                                unsafe_allow_html=True)
                    st.markdown(
                        f"""
                            <div class="card">
                                <p>Average session duration</p>
                                <h1>{overall_average}</h1>
                            </div>
                        """,
                        unsafe_allow_html=True
                    )
                else:
                    st.title("Average Duration")
                    st.markdown(""" 
                        <div style="border: 2px solid black; padding: 20px; background-color: #454545; border-radius: 10px; text-align: center;">
                            <h3 style="font-size: 30px; color: white; font-weight: bold;">No Data Available</h3>
                        </div>
                    """, unsafe_allow_html=True)
            # Column 3: Average Number of Sessions Per Customer
            with col3:
                if average_sessions_per_customer:
                    add_tooltip_css()
                    tooltip_html = render_tooltip(
                        f"The average number of sessions per customer is {average_sessions_per_customer}.")
                    st.markdown(f"<h1 style='display: inline-block;'>Each Customer {tooltip_html}</h1>",
                                unsafe_allow_html=True)
                    st.markdown(
                        f"""
                            <div class="card">
                                <p>Average number of sessions per customer</p>
                                <h1>{average_sessions_per_customer}</h1>
                            </div>
                        """,
                        unsafe_allow_html=True
                    )
                else:
                    st.title("Each Customer")
                    st.markdown(""" 
                        <div style="border: 2px solid black; padding: 20px; background-color: #454545; border-radius: 10px; text-align: center;">
                            <h3 style="font-size: 30px; color: white; font-weight: bold;">No Data Available</h3>
                        </div>
                    """, unsafe_allow_html=True)
        else:
            with col1:
                st.title("Session Duration")
                st.markdown("""
                    <div style="border: 2px solid black; padding: 20px; background-color: #454545; border-radius: 10px; text-align: center;">
                        <h3 style="font-size: 30px; color: white; font-weight: bold;">No Data Available for Total session duration</h3>
                    </div>
                """, unsafe_allow_html=True)
            with col2:
                st.title("Average Duration")
                st.markdown("""
                                    <div style="border: 2px solid black; padding: 20px; background-color: #454545; border-radius: 10px; text-align: center;">
                                        <h3 style="font-size: 30px; color: white; font-weight: bold;"> No Data Available for Average Duration</h3>
                                    </div>
                                """, unsafe_allow_html=True)
            with col3:
                st.title("Each Customer")
                st.markdown("""
                                <div style="border: 2px solid black; padding: 20px; background-color: #454545; border-radius: 10px; text-align: center;">
                                    <h3 style="font-size: 30px; color: white; font-weight: bold;">No Data Available for  Each Customer</h3>
                                </div>
                                """, unsafe_allow_html=True)
        # Todo- List of TOP 10 customer on pages with time spent in each Events
        if df_cj is not None and not df_cj.empty:
            groupby_session = df_cj.groupby(['session', 'Customer_IP']).agg(
                Time_On_Page=('Time_On_Page', 'sum'),
                Event_time=('Event_Time', 'first')
            ).reset_index()
            # Check if there is data available
            if not groupby_session.empty and 'Customer_IP' in groupby_session.columns and 'Time_On_Page' in groupby_session.columns:
                groupby_session['Event_time'] = groupby_session['Event_time'].dt.date
                top_5_rows = groupby_session.nlargest(10, 'Time_On_Page')
                top_5_rows['Time_On_Page'] = top_5_rows['Time_On_Page'].apply(convert_seconds)
                top_5_rows = top_5_rows.drop(columns=['session'])
                add_tooltip_css()
                tooltip_html = render_tooltip(
                    "This table displays the top 10 customer sessions based on total time spent on the page. Hover over the rows to view detailed information about each session, including the customer IP, total time on page, and the first event time for each session.")
                st.markdown(f"<h1 style='display: inline-block;'>Top 10 Customer IP List Data {tooltip_html}</h1>",
                            unsafe_allow_html=True)
                st.subheader("Summary Table")
                st.dataframe(top_5_rows, use_container_width=True)

            else:
                # If no data is available, show title and message
                st.title("Top 10 Customer IP List Data")
                st.markdown(""" 
                    <div style="border: 2px solid black; padding: 20px; background-color: #454545; border-radius: 10px; text-align: center;">
                        <h3 style="font-size: 30px; color: white; font-weight: bold;">No Customer Data Available</h3>
                    </div>
                """, unsafe_allow_html=True)
        else:
            st.title("Top 10 Customer IP List Data")
            st.markdown(""" 
                <div style="border: 2px solid black; padding: 20px; background-color: #454545; border-radius: 10px; text-align: center;">
                    <h3 style="font-size: 30px; color: white; font-weight: bold;">No Customer Data Available</h3>
                </div>
            """, unsafe_allow_html=True)

        # Todo-Viewers with highest number of sessions
        if df_cj is not None and not df_cj.empty:
            max_session_per_ip = df_cj.groupby('Customer_IP')['session'].max().reset_index()
            if not max_session_per_ip.empty and 'Customer_IP' in max_session_per_ip.columns and 'session' in max_session_per_ip.columns:
                add_tooltip_css()
                tooltip_html = render_tooltip(
                    "This chart displays the maximum number of sessions per customer IP. The x-axis represents unique Customer IPs, and the y-axis shows the maximum session count for each IP. Hover over the bars to see the Customer IP and its corresponding session count. Use the slider to adjust the number of top IPs displayed.")
                st.markdown(
                    f"<h1 style='display: inline-block;'>Maximum Sessions per Customer IP {tooltip_html}</h1>",
                    unsafe_allow_html=True
                )
                top_n = st.slider("Select Top N IPs to Display", min_value=1, max_value=50, value=10)
                top_n_ip = max_session_per_ip.nlargest(top_n, 'session')
                st.markdown("<h3 style='text-align: center;'>Maximum Sessions per Customer IP</h3>",
                            unsafe_allow_html=True)

                # Create the bar chart
                chart = alt.Chart(top_n_ip).mark_bar().encode(
                    x=alt.X("Customer_IP:N", title="Customer IP", sort="-y"),
                    y=alt.Y("session:Q", title="Max Session"),
                    color=alt.Color("session:Q", legend=None),
                    tooltip=["Customer_IP", "session"]
                ).properties(
                    width=700,
                    height=400,
                    title="Maximum Sessions per Customer IP"
                )

                text = chart.mark_text(
                    align='center',
                    baseline='middle',
                    dy=-10,
                    fontSize=12
                ).encode(
                    text='session:Q'
                )
                final_chart = chart + text

                final_chart = final_chart.configure_axis(
                    labelAngle=0,  # Horizontal x-axis labels
                    labelFontSize=12,
                    titleFontSize=14
                )
                st.altair_chart(final_chart, use_container_width=True)

            else:
                st.title("Maximum Sessions per Customer IP")
                st.markdown(""" 
                    <div style="border: 2px solid black; padding: 20px; background-color: #454545; border-radius: 10px; text-align: center;">
                        <h3 style="font-size: 30px; color: white; font-weight: bold;">No Session Data Available</h3>
                    </div>
                """, unsafe_allow_html=True)
        else:
            st.title("Maximum Sessions per Customer IP")
            st.markdown(""" 
                <div style="border: 2px solid black; padding: 20px; background-color: #454545; border-radius: 10px; text-align: center;">
                    <h3 style="font-size: 30px; color: white; font-weight: bold;">No Session Data Available</h3>
                </div>
            """, unsafe_allow_html=True)
        # Todo-Most viewed product and collections logic (same as your current code)
        # Group the data by 'Product_Name' and 'Collection_Name'
        chart_col1, chart_col2 = st.columns(2)
        if df_cj is not None and not df_cj.empty:
            df_product_grouped = df_cj.groupby('Product_Name')['Customer_IP'].nunique().reset_index()
            df_product_grouped.rename(columns={'Customer_IP': 'Unique_Visitors'}, inplace=True)
            df_product_sorted = df_product_grouped.sort_values('Unique_Visitors', ascending=False)
            df_collection_grouped = df_cj.groupby('Collection_Name')['Customer_IP'].nunique().reset_index()
            df_collection_grouped.rename(columns={'Customer_IP': 'Unique_Visitors'}, inplace=True)
            df_collection_sorted = df_collection_grouped.sort_values('Unique_Visitors', ascending=False)
            # Create two columns for displaying charts

            # Check if 'Product_Name' column exists and has data
            if not df_product_grouped.empty and 'Product_Name' in df_product_grouped.columns:
                with chart_col1:
                    add_tooltip_css()
                    tooltip_html = render_tooltip(
                        "This chart displays the top N most popular products based on the number of unique visitors. The x-axis represents the product names, and the y-axis shows the number of unique visitors. Use the slider above to select how many top products to display. Hover over the bars for detailed information about each product's unique visitors count.")
                    st.markdown(
                        f"<h1 style='display: inline-block;'>Most Popular Products by Unique Visitors {tooltip_html}</h1>",
                        unsafe_allow_html=True)
                    top_n_products = st.slider("Select Top N Products to Display", min_value=1, max_value=50, value=10)
                    df_top_n_product = df_product_sorted.head(top_n_products)
                    st.markdown("<h3 style='text-align: center;'>Top N Most Popular Products</h3>",
                                unsafe_allow_html=True)

                    # Create the chart for products
                    product_chart = alt.Chart(df_top_n_product).mark_bar().encode(
                        x=alt.X('Product_Name:N', title='Product Name',
                                sort=df_top_n_product['Unique_Visitors'].tolist(),
                                axis=alt.Axis(labelAngle=90)),
                        y=alt.Y('Unique_Visitors:Q', title='Unique Visitors'),
                        color=alt.Color('Unique_Visitors:Q', legend=None),
                        tooltip=['Product_Name:N', 'Unique_Visitors:Q']
                    ).properties(width=600, height=300, title="Top N Most Popular Products by Unique Visitors")

                    # Adding text labels on the bars
                    product_text = product_chart.mark_text(
                        align='center',
                        baseline='middle',
                        dy=-10,
                        fontSize=12
                    ).encode(
                        text='Unique_Visitors:Q'
                    )

                    product_chart = product_chart + product_text
                    product_chart = product_chart.configure_axis(
                        labelAngle=90,
                        labelFontSize=12,
                        titleFontSize=16
                    )
                    st.altair_chart(product_chart, use_container_width=True)
            else:
                with chart_col1:
                    st.title("Most Popular Products by Unique Visitors")
                    st.markdown("""
                        <div style="border: 2px solid black; padding: 20px; background-color: #454545; border-radius: 10px; text-align: center;">
                            <h3 style="font-size: 30px; color: white; font-weight: bold;">No Product Data Available</h3>
                        </div>
                    """, unsafe_allow_html=True)

            if not df_collection_grouped.empty and 'Collection_Name' in df_collection_grouped.columns:
                with chart_col2:
                    add_tooltip_css()
                    tooltip_html = render_tooltip(
                        "This chart shows the top N most popular collections based on unique visitors. The x-axis represents the collection names, and the y-axis shows the number of unique visitors. Use the slider above to adjust the number of top collections displayed. Hover over the bars to see detailed information about each collection's unique visitors count.")
                    st.markdown(
                        f"<h1 style='display: inline-block;'>Most Popular Collections by Unique Visitors {tooltip_html}</h1>",
                        unsafe_allow_html=True)
                    top_n_collections = st.slider("Select Top N Collections to Display", min_value=1, max_value=50,
                                                  value=10)
                    df_top_n_collection = df_collection_sorted.head(top_n_collections)
                    st.markdown("<h3 style='text-align: center;'>Top N Most Popular Collections</h3>",
                                unsafe_allow_html=True)

                    # Create the chart for collections
                    collection_chart = alt.Chart(df_top_n_collection).mark_bar().encode(
                        x=alt.X('Collection_Name:N', title='Collection Name',
                                sort=df_top_n_collection['Unique_Visitors'].tolist(),
                                axis=alt.Axis(labelAngle=90)),
                        y=alt.Y('Unique_Visitors:Q', title='Unique Visitors'),
                        color=alt.Color('Unique_Visitors:Q', legend=None),
                        tooltip=['Collection_Name:N', 'Unique_Visitors:Q']
                    ).properties(width=600, height=300, title="Top N Most Popular Collections by Unique Visitors")

                    # Adding text labels on the bars
                    collection_text = collection_chart.mark_text(
                        align='center',
                        baseline='middle',
                        dy=-10,
                        fontSize=12
                    ).encode(
                        text='Unique_Visitors:Q'
                    )

                    collection_chart = collection_chart + collection_text
                    collection_chart = collection_chart.configure_axis(
                        labelAngle=0,
                        labelFontSize=14,
                        titleFontSize=16
                    )
                    st.altair_chart(collection_chart, use_container_width=True)
            else:
                with chart_col2:
                    st.title("Most Popular Collections by Unique Visitors")
                    st.markdown("""
                        <div style="border: 2px solid black; padding: 20px; background-color: #454545; border-radius: 10px; text-align: center;">
                            <h3 style="font-size: 30px; color: white; font-weight: bold;">No Collection Data Available</h3>
                        </div>
                    """, unsafe_allow_html=True)
        else:
            with chart_col1:
                st.title("Most Popular Products by Unique Visitors")
                st.markdown("""
                    <div style="border: 2px solid black; padding: 20px; background-color: #454545; border-radius: 10px; text-align: center;">
                        <h3 style="font-size: 30px; color: white; font-weight: bold;">No Product Data Available</h3>
                    </div>
                """, unsafe_allow_html=True)
            with chart_col2:
                st.title("Most Popular Collections by Unique Visitors")
                st.markdown("""
                    <div style="border: 2px solid black; padding: 20px; background-color: #454545; border-radius: 10px; text-align: center;">
                        <h3 style="font-size: 30px; color: white; font-weight: bold;">No Collection Data Available</h3>
                    </div>
                """, unsafe_allow_html=True)

        # Todo-Product Name Most add to card in chart
        chart_col1, chart_col2 = st.columns(2)
        if df_cj is not None and not df_cj.empty:
            df_cart_add = df_cj[df_cj['Event'] == 'Cart Add']
            df_grouped_cart_add = df_cart_add.groupby('Product_Name')['Customer_IP'].nunique().reset_index()
            df_grouped_cart_add.rename(columns={'Customer_IP': 'Unique_Visitors'}, inplace=True)
            with chart_col1:
                if not df_cj['Search_Term'].dropna().empty:  # Check if there are any search terms
                    st.markdown("<h3 style='text-align: center;'>Most Searched Terms</h3>", unsafe_allow_html=True)
                    search_terms = df_cj['Search_Term'].dropna().astype(str)  # Ensure no NaN values and all are strings
                    search_term_counts = search_terms.value_counts()
                    wordcloud = WordCloud(width=800, height=400, background_color='white').generate_from_frequencies(
                        search_term_counts)
                    image = wordcloud.to_image()
                    image_stream = io.BytesIO()
                    image.save(image_stream, format='PNG')
                    image_stream.seek(0)
                    st.image(image_stream, use_container_width=True)
                else:
                    st.title("Most Searched Terms")
                    st.markdown("""
                        <div style="border: 2px solid black; padding: 20px; background-color: #454545; border-radius: 10px; text-align: center;">
                            <h3 style="font-size: 30px; color: white; font-weight: bold;">No Search Term Data Available</h3>
                        </div>
                    """, unsafe_allow_html=True)
            with chart_col2:
                if not df_cart_add.empty and 'Product_Name' in df_cart_add.columns:
                    add_tooltip_css()
                    tooltip_html = render_tooltip(
                        "This chart displays the top N products that were most frequently added to the cart, based on the number of unique visitors. The x-axis represents the product names, and the y-axis shows the count of unique visitors who added those products to their cart. Use the slider above to adjust the number of top products displayed. Hover over the bars to see detailed information about the number of unique visitors for each product.")
                    st.markdown(f"<h1 style='display: inline-block;'>Most Added Products to Cart {tooltip_html}</h1>",
                                unsafe_allow_html=True)
                    top_n_products = st.slider("Select Top N Products to Display", min_value=10, max_value=50, value=10)
                    st.markdown(
                        f"<h3 style='text-align: center;'>Top {top_n_products} Most Added Products to Cart</h3>",
                        unsafe_allow_html=True)
                    # Adjust filtering logic to reflect slider value
                    df_top_n_cart_add = df_grouped_cart_add.sort_values('Unique_Visitors', ascending=False).head(
                        top_n_products)

                    # Modify the color encoding to use Unique_Visitors for coloring the bars
                    cart_add_chart = alt.Chart(df_top_n_cart_add).mark_bar().encode(
                        x=alt.X('Product_Name:N', title='Product Name',
                                sort=df_top_n_cart_add['Unique_Visitors'].tolist(),
                                axis=alt.Axis(labelAngle=90)),  # Rotate labels to 90 degrees for readability
                        y=alt.Y('Unique_Visitors:Q', title='Unique Visitors'),
                        color=alt.Color('Unique_Visitors:Q', legend=None),
                        # Color bars based on the Unique Visitors count
                        tooltip=['Product_Name:N', 'Unique_Visitors:Q']
                    ).properties(width=600, height=300, title="Top N Most Added Products to Cart by Unique Visitors")

                    # Add labels on top of the bars (number of unique visitors)
                    text = cart_add_chart.mark_text(
                        align='center',
                        baseline='middle',
                        dy=-10,  # Adjust label position
                        fontSize=12
                    ).encode(
                        text='Unique_Visitors:Q'
                    )
                    # Combine the bar chart and text labels
                    cart_add_chart = cart_add_chart + text

                    # Configure the axis for better readability
                    cart_add_chart = cart_add_chart.configure_axis(
                        labelAngle=0,  # Set the angle of the axis labels (x-axis) to 0 degrees
                        labelFontSize=14,  # Increase font size of labels
                        titleFontSize=16  # Increase font size of axis title
                    )

                    # Display the chart
                    st.altair_chart(cart_add_chart, use_container_width=True)
                else:
                    st.title("Most Added Products to Cart")
                    st.markdown("""
                        <div style="border: 2px solid black; padding: 20px; background-color: #454545; border-radius: 10px; text-align: center;">
                            <h3 style="font-size: 30px; color: white; font-weight: bold;">No Cart Add Data Available</h3>
                        </div>
                    """, unsafe_allow_html=True)
        else:
            with chart_col1:
                st.title("Most Searched Terms")
                st.markdown("""
                    <div style="border: 2px solid black; padding: 20px; background-color: #454545; border-radius: 10px; text-align: center;">
                        <h3 style="font-size: 30px; color: white; font-weight: bold;">No Search Term Data Available</h3>
                    </div>
                """, unsafe_allow_html=True)
            with chart_col2:
                st.title("Most Added Products to Cart")
                st.markdown("""
                    <div style="border: 2px solid black; padding: 20px; background-color: #454545; border-radius: 10px; text-align: center;">
                        <h3 style="font-size: 30px; color: white; font-weight: bold;">No Cart Add Data Available</h3>
                    </div>
                """, unsafe_allow_html=True)
        # Todo- Total add to cart product count--------------------------------------

        col1 = st.columns(1)[0]
        # Check if data for 'Cart Add' event and 'Customer_IP' column is available
        if df_cj is not None and not df_cj.empty:
            if not df_cj.empty and 'Event' in df_cj.columns and 'Customer_IP' in df_cj.columns:
                df_cart_add = df_cj[df_cj['Event'] == 'Cart Add']
                df_grouped_cart_add = (
                    df_cart_add.groupby('Product_Name')['Customer_IP']
                    .nunique()
                    .reset_index()
                    .rename(columns={'Customer_IP': 'Unique_Visitors'})
                )
                total_unique_visitors = df_grouped_cart_add['Unique_Visitors'].sum()

                with col1:
                    add_tooltip_css()
                    tooltip_html = render_tooltip(
                        f"Total number of unique visitors who added products to the cart: {total_unique_visitors}")
                    st.markdown(f"<h1 style='display: inline-block;'>Total Added to Cart Products {tooltip_html}</h1>",
                                unsafe_allow_html=True)
                    st.markdown(
                        f"""
                            <div class="card">
                                <p>Total added to cart products</p>
                                <h1>{total_unique_visitors}</h1>
                            </div>
                            """,
                        unsafe_allow_html=True
                    )
            else:
                st.title("Total Added to Cart Products")
                st.markdown(""" 
                    <div style="border: 2px solid black; padding: 20px; background-color: #454545; border-radius: 10px; text-align: center;">
                        <h3 style="font-size: 30px; color: white; font-weight: bold;">No Cart Add Data Available</h3>
                    </div>
                """, unsafe_allow_html=True)
        else:
            st.title("Total Added to Cart Products")
            st.markdown(""" 
                <div style="border: 2px solid black; padding: 20px; background-color: #454545; border-radius: 10px; text-align: center;">
                    <h3 style="font-size: 30px; color: white; font-weight: bold;">No Cart Add Data Available</h3>
                </div>
            """, unsafe_allow_html=True)

        # Todo- Avg time spent on each page and Total time spent on each page
        col1, col2 = st.columns(2)
        if df_cj is not None and not df_cj.empty:
            df_cj['Time_On_Page'] = pd.to_numeric(df_cj['Time_On_Page'], errors='coerce')
            events = ['Cart', 'Home', 'Product', 'Collection']
            filtered_df = df_cj[df_cj['Event'].isin(events)]
            # Check if there is data available for the events
            if not filtered_df.empty and 'Event' in filtered_df.columns and 'Time_On_Page' in filtered_df.columns:
                avg_time_per_event = filtered_df.groupby('Event')['Time_On_Page'].mean().reset_index()
                avg_time_per_event['Time_On_Page_Display'] = avg_time_per_event['Time_On_Page'].apply(convert_seconds)
                Total_time_spent = filtered_df.groupby('Event')['Time_On_Page'].sum().reset_index()
                Total_time_spent['Time_On_Page_Display'] = Total_time_spent['Time_On_Page'].apply(convert_seconds)
                with col1:
                    add_tooltip_css()
                    tooltip_html = render_tooltip(
                        "This pie chart displays the average time spent on each event (Cart, Home, Product, Collection). The size of each segment represents the average time spent on that event, and the color differentiates between event types. Hover over the segments to see the average time spent on each event, displayed in a human-readable format.")
                    st.markdown(f"<h1 style='display: inline-block;'>Avg Time Spent on Each Event {tooltip_html}</h1>",
                                unsafe_allow_html=True)
                    pie_chart_avg = alt.Chart(avg_time_per_event).mark_arc().encode(
                        theta='Time_On_Page:Q',
                        color='Event:N',
                        tooltip=['Event:N', 'Time_On_Page_Display:N']
                    ).properties(
                        title="Average Time Spent on Each Event",
                        width=350,
                        height=350
                    )
                    st.altair_chart(pie_chart_avg, use_container_width=True)
                # Column 2: Total Time Spent on Each Event
                with col2:
                    add_tooltip_css()
                    tooltip_html = render_tooltip(
                        "This pie chart shows the total time spent on each event (Cart, Home, Product, Collection). The size of each segment represents the total time spent on that event, and the color distinguishes between the different event types. Hover over the segments to view the total time spent on each event, displayed in a human-readable format.")
                    st.markdown(
                        f"<h1 style='display: inline-block;'>Total Time Spent on Each Event {tooltip_html}</h1>",
                        unsafe_allow_html=True)
                    pie_chart_total = alt.Chart(Total_time_spent).mark_arc().encode(
                        theta='Time_On_Page:Q',
                        color='Event:N',
                        tooltip=['Event:N', 'Time_On_Page_Display:N']
                    ).properties(
                        title="Total Time Spent on Each Event",
                        width=350,
                        height=350
                    )
                    st.altair_chart(pie_chart_total, use_container_width=True)
        else:
            with col1:
                st.title("Average Time Spent on Each Event")
                st.markdown(""" 
                    <div style="border: 2px solid black; padding: 20px; background-color: #454545; border-radius: 10px; text-align: center;">
                        <h3 style="font-size: 30px; color: white; font-weight: bold;">No Event Data Available</h3>
                    </div>
                """, unsafe_allow_html=True)

            with col2:
                st.title("Total Time Spent on Each Event")
                st.markdown(""" 
                    <div style="border: 2px solid black; padding: 20px; background-color: #454545; border-radius: 10px; text-align: center;">
                        <h3 style="font-size: 30px; color: white; font-weight: bold;">No Event Data Available</h3>
                    </div>
                """, unsafe_allow_html=True)

        # Todo-Time_Spend on Each Product ID with Product Name------------------------------------------

        chart_col1, chart_col2 = st.columns(2)
        if df_cj is not None and not df_cj.empty:
            with chart_col1:
                # df_cj['Product_ID'] = df_cj['Product_ID'].fillna('Unknown').astype(str).replace(".0", "", regex=True)
                # time_spent_per_product = df_cj.groupby(['Product_ID', 'Product_Name'])['Time_On_Page'].sum().reset_index()
                # if not df_cj.empty and 'Product_ID' in df_cj.columns and 'Time_On_Page' in df_cj.columns and not time_spent_per_product.empty:
                if not df_cj.empty and 'Product_ID' in df_cj.columns and 'Time_On_Page' in df_cj.columns:
                    df_cj['Product_ID'] = df_cj['Product_ID'].fillna('Unknown').astype(str).replace(".0", "", regex=True)
                    time_spent_per_product = df_cj.groupby(['Product_ID', 'Product_Name'])['Time_On_Page'].sum().reset_index()
                    time_spent_per_product_sorted = time_spent_per_product.sort_values(by='Time_On_Page',ascending=False)
                    time_spent_per_product_sorted['Time_On_Page'] = time_spent_per_product_sorted['Time_On_Page'].apply(convert_seconds)
                    add_tooltip_css()
                    tooltip_html = render_tooltip("This table displays the total time spent on each product. The data is grouped by Product ID and Name, and the total time spent is calculated for each product. The table is sorted in descending order, showing the products that have the highest total time spent on top. Hover over the rows to see the time spent on each product, displayed in a human-readable format.")
                    st.markdown(
                        f"<h1 style='display: inline-block;'>Summary of Total Time Spent Per Product {tooltip_html}</h1>",
                        unsafe_allow_html=True)
                    st.markdown("### Total Time Spent on Each Product")
                    st.dataframe(time_spent_per_product_sorted)
                else:
                    st.title("Summary of Total Time Spent Per Product")
                    st.markdown(""" 
                        <div style="border: 2px solid black; padding: 20px; background-color: #454545; border-radius: 10px; text-align: center;">
                        <h3 style="font-size: 30px; color: white; font-weight: bold;">No Data for Summary of Total Time Spent Per Product</h3>
                        </div>
                    """, unsafe_allow_html=True)

            # Column 2: Summary of Total Time Spent Per Collections
            with chart_col2:
                # time_spent_per_product = df_cj.groupby(['Collection_Name'])['Time_On_Page'].sum().reset_index()
                # time_spent_per_product_sorted = time_spent_per_product.sort_values(by='Time_On_Page', ascending=False)
                # if not df_cj.empty and 'Collection_Name' in df_cj.columns and 'Time_On_Page' in df_cj.columns and not time_spent_per_product_sorted.empty:
                if not df_cj.empty and 'Collection_Name' in df_cj.columns and 'Time_On_Page' in df_cj.columns:
                    time_spent_per_product = df_cj.groupby(['Collection_Name'])['Time_On_Page'].sum().reset_index()
                    time_spent_per_product_sorted = time_spent_per_product.sort_values(by='Time_On_Page',ascending=False)
                    time_spent_per_product_sorted['Time_On_Page'] = time_spent_per_product_sorted['Time_On_Page'].apply(convert_seconds)
                    add_tooltip_css()
                    tooltip_html = render_tooltip("This table displays the total time spent on each collection. The data is grouped by Collection Name, and the total time spent is calculated for each collection. The table is sorted in descending order, highlighting the collections with the most time spent. Hover over the rows to see the total time spent on each collection, displayed in a human-readable format.")
                    st.markdown(
                        f"<h1 style='display: inline-block;'>Summary of Total Time Spent Per Collections {tooltip_html}</h1>",
                        unsafe_allow_html=True)
                    st.markdown("### Total Time Spent on Each Collection")
                    st.dataframe(time_spent_per_product_sorted)
                else:
                    st.title("Summary of Total Time Spent Per Collections")
                    st.markdown(""" 
                        <div style="border: 2px solid black; padding: 20px; background-color: #454545; border-radius: 10px; text-align: center;">
                        <h3 style="font-size: 30px; color: white; font-weight: bold;">No Data for Summary of Total Time Spent Per Collections</h3>
                        </div>
                    """, unsafe_allow_html=True)
        else:
            with chart_col1:
                st.title("Summary of Total Time Spent Per Product")
                st.markdown(""" 
                    <div style="border: 2px solid black; padding: 20px; background-color: #454545; border-radius: 10px; text-align: center;">
                        <h3 style="font-size: 30px; color: white; font-weight: bold;">No Data for Summary of Total Time Spent Per Product</h3>
                    </div>
                """, unsafe_allow_html=True)
            with chart_col2:
                st.title("Summary of Total Time Spent Per Collections")
                st.markdown(""" 
                    <div style="border: 2px solid black; padding: 20px; background-color: #454545; border-radius: 10px; text-align: center;">
                        <h3 style="font-size: 30px; color: white; font-weight: bold;">No Data for Summary of Total Time Spent Per Collections</h3>
                    </div>
                """, unsafe_allow_html=True)

        # Todo- Viewers On Each Page
        if df_cj is not None and not df_cj.empty:
            events = ['Cart', 'Home', 'Product', 'Collection']
            filtered_df = df_cj[df_cj['Event'].isin(events)]

            if not filtered_df.empty:
                viewer_counts = filtered_df.groupby("Event")["Customer_IP"].nunique().reset_index()
                viewer_counts.columns = ["Event", "Total Viewers"]
                add_tooltip_css()
                tooltip_html = render_tooltip(
                    "This bar chart displays the total number of viewers for each page or event. The x-axis represents the different pages or events, and the y-axis shows the total viewers count. Hover over the bars to see the exact number of viewers for each page/event. The bars are color-coded based on the total viewers count to give a visual cue of viewer distribution."
                )
                st.markdown(f"<h1 style='display: inline-block;'>Total Viewers on Each Page {tooltip_html}</h1>",
                            unsafe_allow_html=True)
                st.markdown("<h3 style='text-align: center;'>Viewers by Page</h3>", unsafe_allow_html=True)

                page_chart = alt.Chart(viewer_counts).mark_bar().encode(
                    x=alt.X('Event:N', title='Page/Event', sort=viewer_counts['Event'].tolist()),
                    y=alt.Y('Total Viewers:Q', title='Total Viewers'),
                    color=alt.Color('Total Viewers:Q', legend=None),  # Color bars based on the 'Total Viewers' count
                    tooltip=['Event:N', 'Total Viewers:Q']
                ).properties(width=600, height=300, title="Viewers by Page")

                page_text = page_chart.mark_text(
                    align='center',
                    baseline='middle',
                    dy=-10,  # Adjust label position
                    fontSize=12
                ).encode(
                    text='Total Viewers:Q'
                )

                page_chart = page_chart + page_text
                page_chart = page_chart.configure_axis(
                    labelAngle=0,
                    labelFontSize=14,
                    titleFontSize=16
                )

                st.altair_chart(page_chart, use_container_width=True)

            else:
                st.title("Total Viewers on Each Page")
                st.markdown("""
                    <div style="border: 2px solid black; padding: 20px; background-color: #454545; border-radius: 10px; text-align: center;">
                        <h3 style="font-size: 30px; color: white; font-weight: bold;">Total Viewers on Each Page - No Data Available</h3>
                    </div>
                """, unsafe_allow_html=True)
        else:
            st.title("Total Viewers on Each Page")
            st.markdown("""
                <div style="border: 2px solid black; padding: 20px; background-color: #454545; border-radius: 10px; text-align: center;">
                    <h3 style="font-size: 30px; color: white; font-weight: bold;">Total Viewers on Each Page - No Data Available</h3>
                </div>
            """, unsafe_allow_html=True)

        # Todo -Bounce Rate of each Customer who spend time less then 30 second
        chart_col1, chart_col2 = st.columns(2)
        if df_cj is not None and not df_cj.empty:
            customer_time = df_cj.groupby('Customer_IP')['Time_On_Page'].sum().reset_index()
            if not customer_time.empty:
                filtered_customer_time = customer_time[customer_time['Time_On_Page'] < 30]
                total_customers = customer_time['Customer_IP'].nunique()
                customers_under_30_seconds = filtered_customer_time['Customer_IP'].nunique()

                if total_customers > 0:
                    percentage = (customers_under_30_seconds / total_customers) * 100
                else:
                    percentage = 0  # Avoid division by zero
                percentage = round(percentage, 2)
                # Streamlit layout for charts
                # Viewers by Event chart
                with chart_col1:
                    add_tooltip_css()
                    tooltip_html = render_tooltip(
                        "This section calculates the bounce rate, which is the percentage of customers who spend less than 30 seconds on the page. It compares the total number of unique customers to those with a time on page under 30 seconds, displaying the result as a percentage."
                    )
                    st.markdown(
                        f"<h1 style='display: inline-block;'>Customers Rate Under 30 Sec{tooltip_html}</h1>",
                        unsafe_allow_html=True)
                    st.markdown(
                        f"""
                            <div class="card">
                                <p>Bounce rate in percentage</p>
                                <h1>{percentage}%</h1>
                            </div>
                        """,
                        unsafe_allow_html=True
                    )
            else:
                with chart_col1:
                    st.title("Customers Rate Under 30 Seconds")
                    st.markdown("""
                        <div style="border: 2px solid black; padding: 20px; background-color: #454545; border-radius: 10px; text-align: center;">
                            <h3 style="font-size: 30px; color: white; font-weight: bold;">Customers Rate Under 30 Seconds - No Data Available</h3>
                        </div>
                    """, unsafe_allow_html=True)
        else:
            with chart_col1:
                st.title("Customers Rate Under 30 Seconds")
                st.markdown("""
                    <div style="border: 2px solid black; padding: 20px; background-color: #454545; border-radius: 10px; text-align: center;">
                        <h3 style="font-size: 30px; color: white; font-weight: bold;">Customers Rate Under 30 Seconds - No Data Available</h3>
                    </div>
                """, unsafe_allow_html=True)

        # Todo-Bounce Rate(%) by Event Type-------------------------------------------
        if df_cj is not None and not df_cj.empty:
            events = ['Cart', 'Home', 'Product', 'Collection']
            filtered_df = df_cj[df_cj['Event'].isin(events)]
            filtered_df = filtered_df.sort_values(by=['Customer_IP', 'session'], ascending=True)
            last_session_df = filtered_df.drop_duplicates(subset=['Customer_IP', 'session'], keep='last')
            bounce_df = last_session_df[last_session_df['Time_On_Page'] < 10]
            bounce_rate_by_event = {}
            # Calculate bounce rates
            for event in events:
                total_events_event_count = df_cj[df_cj['Event'] == event]['Customer_IP'].nunique()
                bounce_events_event_count = bounce_df[bounce_df['Event'] == event]['Customer_IP'].nunique()
                bounce_rate_percentage = (
                                                     bounce_events_event_count / total_events_event_count) * 100 if total_events_event_count > 0 else 0
                bounce_rate_by_event[event] = bounce_rate_percentage
            # Convert bounce rates dictionary to DataFrame
            bounce_rate_df = pd.DataFrame(list(bounce_rate_by_event.items()), columns=['Event', 'Bounce Rate'])
            bounce_rate_df['Bounce Rate'] = bounce_rate_df['Bounce Rate'].round(2)
            # Filter out events with a zero bounce rate (if you want)
            bounce_rate_df = bounce_rate_df[bounce_rate_df['Bounce Rate'] > 0]
            # If there is valid bounce rate data
            if not bounce_rate_df.empty:
                with chart_col2:
                    add_tooltip_css()
                    tooltip_html = render_tooltip(
                        "This bar chart shows the bounce rate percentage for each event type. The x-axis represents different event types, and the y-axis shows the bounce rate for each event, displayed as a percentage. Hover over the bars to view the exact bounce rate percentage for each event type. The bars are color-coded based on the bounce rate, providing a visual indication of the bounce rate distribution across event types.")
                    st.markdown(f"<h1 style='display: inline-block;'>Bounce Rate(%) by Event Type {tooltip_html}</h1>",
                                unsafe_allow_html=True)

                    # Create the Altair chart
                    bounce_chart = alt.Chart(bounce_rate_df).mark_bar().encode(
                        x=alt.X('Event:N', title='Event Type', sort=bounce_rate_df['Event'].tolist()),
                        y=alt.Y('Bounce Rate:Q', title='Bounce Rate (%)'),
                        color=alt.Color('Bounce Rate:Q', legend=None),
                        tooltip=['Event:N', 'Bounce Rate:Q']
                    ).properties(width=600, height=300, title="Bounce Rate by Event Type")

                    bounce_text = bounce_chart.mark_text(
                        align='center',
                        baseline='middle',
                        dy=-10,  # Adjust label position
                        fontSize=12
                    ).encode(
                        text='Bounce Rate:Q'
                    )

                    bounce_chart = bounce_chart + bounce_text
                    bounce_chart = bounce_chart.configure_axis(
                        labelAngle=0,
                        labelFontSize=14,
                        titleFontSize=16
                    )
                    st.altair_chart(bounce_chart)

            # Else: No valid bounce rate data
            else:
                with chart_col2:
                    st.title("Bounce Rate(%) by Event Type")
                    st.markdown("""
                        <div style="border: 2px solid black; padding: 20px; background-color: #454545; border-radius: 10px; text-align: center;">
                            <h3 style="font-size: 30px; color: white; font-weight: bold;">Bounce Rate(%) by Event Type - No Data Available</h3>
                        </div>
                    """, unsafe_allow_html=True)
        else:
            with chart_col2:
                st.title("Customers Rate Under 30 Seconds")
                st.markdown("""
                    <div style="border: 2px solid black; padding: 20px; background-color: #454545; border-radius: 10px; text-align: center;">
                        <h3 style="font-size: 30px; color: white; font-weight: bold;">Customers Rate Under 30 Seconds - No Data Available</h3>
                    </div>
                """, unsafe_allow_html=True)

    except:
        st.markdown(
            "<h3 style='font-size: 30px; color: red; text-align: center;'><b>Dataset is currently unavailable.</b></h3>",
            unsafe_allow_html=True)

def show_order_data_page():
    st.title('Order Data')
    add_custom_css()
    try:
        # Todo- Card Creation for the above -----------------------------------
        col1 = st.columns(1)[0]
        with col1:
            if df_orders is not None and not df_orders.empty:
                total_listed_customers = df_orders['Order_ID'].nunique()
                add_tooltip_css()
                tooltip_html = render_tooltip(f"Total number of unique orders placed: {total_listed_customers}")
                st.markdown(f"<h1 style='display: inline-block;'>Unique Orders {tooltip_html}</h1>",
                            unsafe_allow_html=True)
                st.markdown(f"""
                    <div class="card">
                        <p>Total orders placed</p>
                        <h1>{total_listed_customers}</h1>
                    </div>
                    """, unsafe_allow_html=True)
            else:
                st.title("Total orders placed")
                st.markdown("""
                    <div style="border: 2px solid black; padding: 20px; background-color: #454545; border-radius: 10px; text-align: center;">
                        <h3 style="font-size: 30px; color: white; font-weight: bold;">No data available for unique orders placed.</h3>
                    </div>
                """, unsafe_allow_html=True)
            # Customer Order Data Section
            if df_orders is not None and not df_orders.empty:
                add_tooltip_css()
                tooltip_html = render_tooltip("Preview of customer order data filtered by the selected date range.")
                st.markdown(f"<h1 style='display: inline-block;'>Preview Filtered Order Data {tooltip_html}</h1>",
                            unsafe_allow_html=True)
                filtered_orders = filter_by_date(df_orders, 'Order_Created_At')
                st.subheader("Customer Order Data")
                st.dataframe(filtered_orders)
            else:
                st.title("Preview of customer order data filtered by the selected date range")
                st.markdown("""
                                <div style="border: 2px solid black; padding: 20px; background-color: #454545; border-radius: 10px; text-align: center;">
                                    <h3 style="font-size: 30px; color: white; font-weight: bold;">Data Frame is empty</h3>
                                </div>
                            """, unsafe_allow_html=True)

        # Todo----Total orders placed: Weekday vs Weekend
        try:
            col1,col2=st.columns(2)
            if df_orders is not None and not df_orders.empty:

                df_orders_ = df_orders.groupby('Order_ID').agg({'Order_Created_At': 'first'})
                df_orders_['Order_Created_At'] = pd.to_datetime(df_orders_['Order_Created_At'], errors='coerce',utc=True)
                df_orders_['Weekday_Weekend'] = df_orders_['Order_Created_At'].dt.dayofweek.apply(lambda x: 'Weekend' if x >= 5 else 'Weekday')
                weekday_count = df_orders_[df_orders_['Weekday_Weekend'] == 'Weekday'].shape[0]
                weekend_count = df_orders_[df_orders_['Weekday_Weekend'] == 'Weekend'].shape[0]
                counts = [weekday_count, weekend_count]
                labels = ['Weekday', 'Weekend']
                pie_data = pd.DataFrame({
                    'Category': labels,
                    'Count': counts,
                    'Percentage': [(count / sum(counts)) * 100 for count in counts]  # Calculate percentage
                })
                pie_data['Label'] = pie_data['Percentage'].round(1).astype(str) + '%'
                # Plotting the Pie Chart
                pie_chart = alt.Chart(pie_data).mark_arc().encode(
                    theta=alt.Theta(field="Count", type="quantitative"),
                    color=alt.Color(field="Category", type="nominal"),
                    tooltip=["Category", "Count", "Percentage"],  # Show both count and percentage in tooltip
                )
                with col1:
                    # st.write(f"Weekday Count: {weekday_count} ({(weekday_count / sum(counts)) * 100:.2f}%)")
                    # st.write(f"Weekend Count: {weekend_count} ({(weekend_count / sum(counts)) * 100:.2f}%)")
                    add_tooltip_css()
                    tooltip_html = render_tooltip(
                        "This pie chart illustrates the distribution of orders placed on weekdays versus weekends. Hover over each segment to view detailed information, including the category (Weekday or Weekend), the total order count, and its percentage share of the overall orders.")
                    st.markdown(
                        f"<h1 style='display: inline-block;'>Total orders placed: Weekday vs Weekend {tooltip_html}</h1>",
                        unsafe_allow_html=True)
                    st.altair_chart(pie_chart, use_container_width=True)


                df_orders_ = df_orders.groupby('Order_ID').agg({'Order_Created_At': 'first'}).reset_index()
                df_orders_['Order_Created_At'] = pd.to_datetime(df_orders_['Order_Created_At'], errors='coerce',utc=True)
                # col1 = st.columns(1)[0]
                with col2:

                    df_orders_['days_of_week'] = df_orders_['Order_Created_At'].dt.dayofweek.apply(
                        lambda x: ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'][x]
                    )
                    day_count = df_orders_['days_of_week'].value_counts()
                    day_count = day_count.reindex(
                        ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'], fill_value=0)
                    pie_data = pd.DataFrame({
                        'Day': day_count.index,
                        'Count': day_count.values
                    })
                    pie_chart = alt.Chart(pie_data).mark_arc().encode(
                        theta=alt.Theta(field="Count", type="quantitative"),
                        color=alt.Color(field="Day", type="nominal",
                                        sort=['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday',
                                              'Sunday']),
                        tooltip=["Day:N", "Count:Q"]
                    )
                    add_tooltip_css()
                    tooltip_html = render_tooltip(
                        "This chart displays the total number of orders placed on each day of the week. Hover over the sections of the pie chart to view detailed information, including the specific day and the corresponding order count.")
                    st.markdown(
                        f"<h1 style='display: inline-block;'>Total Orders Placed: Days of the Week {tooltip_html}</h1>",
                        unsafe_allow_html=True)
                    st.altair_chart(pie_chart, use_container_width=True)
            else:
                with col1:
                    st.title("Orders by Weekday/Weekend")
                    st.markdown("""
                        <div style="border: 2px solid black; padding: 20px; background-color: #454545; border-radius: 10px; text-align: center;">
                            <h3 style="font-size: 30px; color: white; font-weight: bold;">No data available for the Weekday vs Weekend analysis.</h3>
                        </div>
                    """, unsafe_allow_html=True)
                with col2:
                    st.title("Orders by Day of the Week")
                    st.markdown("""
                                   <div style="border: 2px solid black; padding: 20px; background-color: #454545; border-radius: 10px; text-align: center;">
                                       <h3 style="font-size: 30px; color: white; font-weight: bold;">No data available for the Days of the Week analysis.</h3>
                                   </div>
                               """, unsafe_allow_html=True)
        except:
            col1, col2 = st.columns(2)
            with col1:
                st.title("Orders by Weekday/Weekend")
                st.markdown("""
                    <div style="border: 2px solid black; padding: 20px; background-color: #454545; border-radius: 10px; text-align: center;">
                        <h3 style="font-size: 30px; color: white; font-weight: bold;">No data available for the Weekday vs Weekend analysis.</h3>
                    </div>
                """, unsafe_allow_html=True)
            with col2:
                st.title("Orders by Day of the Week")
                st.markdown("""
                               <div style="border: 2px solid black; padding: 20px; background-color: #454545; border-radius: 10px; text-align: center;">
                                   <h3 style="font-size: 30px; color: white; font-weight: bold;">No data available for the Days of the Week analysis.</h3>
                               </div>
                           """, unsafe_allow_html=True)


        # Todo-Total Orders Placed: Hours of the Day-----------------------
        if df_orders is not None and not df_orders.empty:
            df_orders_ = df_orders.groupby('Order_ID').agg({'Order_Created_At': 'first'}).reset_index()
            df_orders_['Order_Created_At'] = pd.to_datetime(df_orders_['Order_Created_At'], errors='coerce', utc=True)
            col1 = st.columns(1)[0]

            with col1:
                df_orders_['hour_of_day'] = df_orders_['Order_Created_At'].dt.hour + 1  # Shift hours to 1-24 range
                hour_count = df_orders_['hour_of_day'].value_counts().sort_index()
                hour_count = hour_count.reindex(range(1, 25), fill_value=0)

                if hour_count.sum() > 0:  # Check if there's data available
                    hour_data = pd.DataFrame({
                        'Hour of Day': hour_count.index,
                        'Number of Orders': hour_count.values
                    })

                    add_tooltip_css()
                    tooltip_html = render_tooltip(
                        "This line chart represents the total number of orders placed during each hour of the day. Hover over the points or the line to see the specific hour and the corresponding order count.")
                    st.markdown(
                        f"<h1 style='display: inline-block;'>Total Orders Placed: Hours of the Day {tooltip_html}</h1>",
                        unsafe_allow_html=True
                    )

                    st.markdown("<h3 style='text-align: center;'>Orders by Hour of the Day</h3>",
                                unsafe_allow_html=True)
                    line_chart = alt.Chart(hour_data).mark_line().encode(
                        x=alt.X('Hour of Day:O', title='Hour of Day', scale=alt.Scale(domain=list(range(1, 25)))),
                        y=alt.Y('Number of Orders:Q', title='Number of Orders'),
                        tooltip=['Hour of Day', 'Number of Orders']
                    ).properties(
                        width=700,
                        height=400
                    )
                    # Add points to the line chart
                    points = alt.Chart(hour_data).mark_point(size=100, color='red').encode(
                        x=alt.X('Hour of Day:O', title='Hour of Day'),
                        y=alt.Y('Number of Orders:Q', title='Number of Orders'),
                        tooltip=['Hour of Day', 'Number of Orders']
                    )
                    # Combine the line chart and points
                    combined_chart = line_chart + points
                    st.altair_chart(combined_chart, use_container_width=True)
                else:
                    st.title("Total Orders Placed: Hours of the Day")
                    st.markdown("""
                        <div style="border: 2px solid black; padding: 20px; background-color: #454545; border-radius: 10px; text-align: center;">
                            <h3 style="font-size: 30px; color: white; font-weight: bold;">No data available for the Hours of the Day analysis.</h3>
                        </div>
                    """, unsafe_allow_html=True)
        else:
            st.title("Total Orders Placed: Hours of the Day")
            st.markdown("""
                <div style="border: 2px solid black; padding: 20px; background-color: #454545; border-radius: 10px; text-align: center;">
                    <h3 style="font-size: 30px; color: white; font-weight: bold;">No data available for the Hours of the Day analysis.</h3>
                </div>
            """, unsafe_allow_html=True)

        # Todo-Total orders placed: day, month, quarter, year
        if df_orders is not None and not df_orders.empty:
            df_orders_ = df_orders.groupby('Order_ID').agg({'Order_Created_At': 'first'}).reset_index()
            df_orders_['Order_Created_At'] = pd.to_datetime(df_orders_['Order_Created_At'], errors='coerce', utc=True)
            col1 = st.columns(1)[0]
            with col1:
                # Extract the day, month, quarter, and year from 'Order_Created_At'
                df_orders_['day'] = df_orders_['Order_Created_At'].dt.date
                # Calculate total orders placed per day
                orders_per_day = df_orders_.groupby('day').size().reset_index(name='order_count')
                # Create a complete date range
                start_date = orders_per_day['day'].min()
                end_date = datetime.today().date()
                full_date_range = pd.date_range(start=start_date, end=end_date)
                # Reindex `orders_per_day` to ensure all dates are included
                orders_per_day.set_index('day', inplace=True)
                orders_per_day = orders_per_day.reindex(full_date_range, fill_value=0).reset_index()
                orders_per_day.rename(columns={'index': 'day'}, inplace=True)
                # Add 'month', 'quarter', and 'year' columns
                orders_per_day['month'] = orders_per_day['day'].dt.to_period('M').astype(str)  # Year-Month format
                orders_per_day['quarter'] = orders_per_day['day'].dt.to_period('Q').astype(str)  # Year-Quarter format
                orders_per_day['year'] = orders_per_day['day'].dt.year  # Year format
                # Group data-----
                orders_per_month = orders_per_day.groupby('month')['order_count'].sum().reset_index()
                orders_per_month['month'] = pd.to_datetime(orders_per_month['month'], format='%Y-%m')
                orders_per_month = orders_per_month.sort_values(by='month')
                orders_per_quarter = orders_per_day.groupby('quarter')['order_count'].sum().reset_index()
                orders_per_year = orders_per_day.groupby('year')['order_count'].sum().reset_index()

                if orders_per_day.empty or orders_per_month.empty or orders_per_quarter.empty or orders_per_year.empty:
                    st.markdown("""
                        <div style="border: 2px solid black; padding: 20px; background-color: #454545; border-radius: 10px; text-align: center;">
                            <h3 style="font-size: 30px; color: white; font-weight: bold;">No data available for Order Count Visualizations.</h3>
                        </div>
                    """, unsafe_allow_html=True)
                else:
                    add_tooltip_css()
                    tooltip_html = render_tooltip(
                        "This chart displays the total number of orders placed. Select a view to see orders by Day, Month, Quarter, or Year. "
                        "Hover over the points or line to view the specific time period (Day, Month, Quarter, or Year) and the corresponding order count.")
                    st.markdown(
                        f"<h1 style='display: inline-block;'>Order Count Visualizations {tooltip_html}</h1>",
                        unsafe_allow_html=True
                    )
                    view = st.radio("Select View",
                                    ['Orders per Day', 'Orders per Month', 'Orders per Quarter', 'Orders per Year'])
                    if view == 'Orders per Day':
                        st.title('Orders Placed: Days')
                        st.markdown("<h3 style='text-align: center;'>Orders by Day</h3>", unsafe_allow_html=True)
                        line_chart = alt.Chart(orders_per_day).mark_line().encode(
                            x=alt.X('day:T', title='Date',
                                    axis=alt.Axis(format="%b %d, %Y", labelAngle=-90, tickMinStep=1)),
                            y=alt.Y('order_count:Q', title='Number of Orders'),
                            tooltip=['day:T', 'order_count:Q']
                        )
                        points = alt.Chart(orders_per_day).mark_point(size=60, color='red').encode(
                            x='day:T',
                            y='order_count:Q',
                            tooltip=['day:T', 'order_count:Q']
                        )
                        combined_chart = line_chart + points
                        st.altair_chart(combined_chart, use_container_width=True)
                    elif view == 'Orders per Month':
                        st.title('Orders Placed: Months')
                        st.markdown("<h3 style='text-align: center;'>Orders by Month</h3>", unsafe_allow_html=True)
                        # Ensure that the month column is formatted as 'Month Year'
                        orders_per_month['month'] = orders_per_month['month'].dt.strftime(
                            '%b %Y')  # Convert to Month Year format
                        # Create the chart with the properly formatted month labels
                        line_chart = alt.Chart(orders_per_month).mark_line().encode(
                            x=alt.X('month:O', title='Month', axis=alt.Axis(labelAngle=-45)),
                            # Ordinal scale for months
                            y=alt.Y('order_count:Q', title='Number of Orders'),
                            tooltip=[alt.Tooltip('month:N', title='Month'), 'order_count:Q']
                        )
                        points = alt.Chart(orders_per_month).mark_point(size=60, color='red').encode(
                            x=alt.X('month:O', title='Month'),
                            y=alt.Y('order_count:Q', title='Number of Orders'),
                            tooltip=[alt.Tooltip('month:N', title='Month'), 'order_count:Q']
                        )
                        combined_chart = line_chart + points
                        st.altair_chart(combined_chart, use_container_width=True)

                    elif view == 'Orders per Quarter':
                        st.title('Orders Placed: Quarters')
                        st.markdown("<h3 style='text-align: center;'>Orders by Quarter</h3>", unsafe_allow_html=True)
                        line_chart = alt.Chart(orders_per_quarter).mark_line().encode(
                            x=alt.X('quarter:N', title='Quarter'),
                            y=alt.Y('order_count:Q', title='Number of Orders'),
                            tooltip=['quarter:N', 'order_count:Q']
                        )
                        points = alt.Chart(orders_per_quarter).mark_point(size=60, color='red').encode(
                            x='quarter:N',
                            y='order_count:Q',
                            tooltip=['quarter:N', 'order_count:Q']
                        )
                        combined_chart = line_chart + points
                        st.altair_chart(combined_chart, use_container_width=True)

                    elif view == 'Orders per Year':
                        st.title('Orders Placed: Years')
                        st.markdown("<h3 style='text-align: center;'>Orders by Year</h3>", unsafe_allow_html=True)
                        line_chart = alt.Chart(orders_per_year).mark_line().encode(
                            x=alt.X('year:O', title='Year'),
                            y=alt.Y('order_count:Q', title='Number of Orders'),
                            tooltip=['year:O', 'order_count:Q']
                        )
                        points = alt.Chart(orders_per_year).mark_point(size=60, color='red').encode(
                            x='year:O',
                            y='order_count:Q',
                            tooltip=['year:O', 'order_count:Q']
                        )
                        combined_chart = line_chart + points
                        st.altair_chart(combined_chart, use_container_width=True)
        else:
            st.title("Total orders placed: day, month, quarter, year")
            st.markdown("""
                <div style="border: 2px solid black; padding: 20px; background-color: #454545; border-radius: 10px; text-align: center;">
                    <h3 style="font-size: 30px; color: white; font-weight: bold;">No data available for the Total orders placed: day, month, quarter, year</h3>
                </div>
            """, unsafe_allow_html=True)

        # Todo--Average orders per customer
        col1, col2, col3, col4 = st.columns(4)
        if df_orders is not None and not df_orders.empty:
            orders_per_customer = df_orders.groupby('Customer_ID')['Order_ID'].nunique().reset_index()
            average_orders_per_customer = orders_per_customer['Order_ID'].mean()
            average_orders_per_customer = round(average_orders_per_customer, 2)
            # Total canceled orders
            total_canceled_orders = df_orders[df_orders['Order_Cancelled_At'].notna()].shape[0]
            # Most orders placed by a customer
            customer_order_counts = df_orders.groupby('Customer_ID')['Order_ID'].nunique()
            max_orders = customer_order_counts.max()
            # Average order value
            order_data = df_orders.groupby('Order_ID').agg({'Order_Total_Price': 'first'}).reset_index()
            average_order_value = order_data['Order_Total_Price'].mean()
            average_order_value = round(average_order_value, 2)
            # For average orders per customer
            with col1:
                if df_orders is not None and not df_orders.empty:
                    st.title("Avg orders")
                    st.markdown("""
                        <div style="border: 2px solid black; padding: 20px; background-color: #454545; border-radius: 10px; text-align: center;">
                            <h3 style="font-size: 30px; color: white; font-weight: bold;">No Data Available for Average Orders per Customer.</h3>
                        </div>
                    """, unsafe_allow_html=True)
                else:
                    add_tooltip_css()
                    tooltip_html = render_tooltip(
                        f"The average number of orders placed per customer is {average_orders_per_customer}")
                    st.markdown(
                        f"<h1 style='display: inline-block;'>Avg orders {tooltip_html}</h1>", unsafe_allow_html=True
                    )
                    st.markdown(
                        f"""
                            <div class="card">
                                <p>Average orders per customer</p>
                                <h1>{average_orders_per_customer}</h1>
                            </div>
                            """,
                        unsafe_allow_html=True
                    )

            # For total canceled orders
            with col2:
                if df_orders is not None and not df_orders.empty:
                    st.title("Order Cancel")
                    st.markdown("""
                        <div style="border: 2px solid black; padding: 20px; background-color: #454545; border-radius: 10px; text-align: center;">
                            <h3 style="font-size: 30px; color: white; font-weight: bold;">No Data Available for Total Canceled Orders.</h3>
                        </div>
                    """, unsafe_allow_html=True)
                else:
                    add_tooltip_css()
                    tooltip_html = render_tooltip(f"The total number of canceled orders is {total_canceled_orders}")
                    st.markdown(
                        f"<h1 style='display: inline-block;'>Order Cancel{tooltip_html}</h1>", unsafe_allow_html=True
                    )
                    st.markdown(
                        f"""
                            <div class="card">
                                <p>Total orders cancelled</p>
                                <h1>{total_canceled_orders}</h1>
                            </div>
                            """,
                        unsafe_allow_html=True
                    )

            # For most orders placed by a customer
            with col3:
                if df_orders is not None and not df_orders.empty:
                    st.title("Max orders")
                    st.markdown("""
                        <div style="border: 2px solid black; padding: 20px; background-color: #454545; border-radius: 10px; text-align: center;">
                            <h3 style="font-size: 30px; color: white; font-weight: bold;">No Data Available for Most Orders by Customer.</h3>
                        </div>
                    """, unsafe_allow_html=True)
                else:
                    add_tooltip_css()
                    tooltip_html = render_tooltip(
                        f"The maximum number of orders placed by a single customer is {max_orders}")
                    st.markdown(
                        f"<h1 style='display: inline-block;'>Max orders {tooltip_html}</h1>", unsafe_allow_html=True
                    )
                    st.markdown(
                        f"""
                            <div class="card">
                                <p>Most orders placed by a customer</p>
                                <h1>{max_orders}</h1>
                            </div>
                            """,
                        unsafe_allow_html=True
                    )
            # For average order value
            with col4:
                if df_orders is not None and not df_orders.empty:
                    st.title("Order Value")
                    st.markdown("""
                        <div style="border: 2px solid black; padding: 20px; background-color: #454545; border-radius: 10px; text-align: center;">
                            <h3 style="font-size: 30px; color: white; font-weight: bold;">No Data Available for Average Order Value.</h3>
                        </div>
                    """, unsafe_allow_html=True)
                else:
                    add_tooltip_css()
                    tooltip_html = render_tooltip(f"The average order value is €{average_order_value}")
                    st.markdown(
                        f"<h1 style='display: inline-block;'>Order Value {tooltip_html}</h1>", unsafe_allow_html=True
                    )
                    st.markdown(
                        f"""
                            <div class="card">
                                <p>Average order value</p>
                                <h1>{average_order_value}</h1>
                            </div>
                            """,
                        unsafe_allow_html=True
                    )
        else:
            with col1:
                st.title("Avg orders")
                st.markdown("""
                    <div style="border: 2px solid black; padding: 20px; background-color: #454545; border-radius: 10px; text-align: center;">
                        <h3 style="font-size: 30px; color: white; font-weight: bold;">No Data Available for Average Orders per Customer.</h3>
                    </div>
                """, unsafe_allow_html=True)
            with col2:
                st.title("Order Cancel")
                st.markdown("""
                    <div style="border: 2px solid black; padding: 20px; background-color: #454545; border-radius: 10px; text-align: center;">
                        <h3 style="font-size: 30px; color: white; font-weight: bold;">No Data Available for Total Canceled Orders.</h3>
                    </div>
                """, unsafe_allow_html=True)
            with col3:
                st.title("Max orders")
                st.markdown("""
                    <div style="border: 2px solid black; padding: 20px; background-color: #454545; border-radius: 10px; text-align: center;">
                        <h3 style="font-size: 30px; color: white; font-weight: bold;">No Data Available for Most Orders by Customer.</h3>
                    </div>
                """, unsafe_allow_html=True)
            with col4:
                st.title("Order Value")
                st.markdown("""
                    <div style="border: 2px solid black; padding: 20px; background-color: #454545; border-radius: 10px; text-align: center;">
                        <h3 style="font-size: 30px; color: white; font-weight: bold;">No Data Available for Order Value by Customer.</h3>
                    </div>
                """, unsafe_allow_html=True)

        # Todo-Highest valued orders and Least valued orders-------------------------------------
        chart_col1, chart_col2 = st.columns(2)
        if df_orders is not None and not df_orders.empty:
            order_data = df_orders.groupby('Customer_Name').agg(
                {'Order_ID': 'first', 'Order_Total_Price': 'first'}).reset_index()
            order_data = order_data.dropna(subset=['Order_ID'])
            top_customers = order_data.nlargest(50, 'Order_Total_Price')
            least_customers = order_data.nsmallest(50, 'Order_Total_Price')

            # For Highest Valued Orders
            with chart_col1:
                if df_orders is not None and not df_orders.empty:
                    st.title("Highest valued orders")
                    st.markdown("""
                        <div style="border: 2px solid black; padding: 20px; background-color: #454545; border-radius: 10px; text-align: center;">
                            <h3 style="font-size: 30px; color: white; font-weight: bold;">No Data Available for Highest Valued Orders.</h3>
                        </div>
                    """, unsafe_allow_html=True)
                else:
                    add_tooltip_css()
                    tooltip_html = render_tooltip(
                        "This bar chart displays the top N customers by their total order price. Hover over each bar to view the customer name and the corresponding total order price in euros.")
                    st.markdown(
                        f"<h1 style='display: inline-block;'>Highest valued orders {tooltip_html}</h1>",
                        unsafe_allow_html=True
                    )
                    top_n = st.slider("Select Top N Customers to Display", min_value=1, max_value=50, value=5,
                                      key="top_n_largest")
                    top_customers_filtered = top_customers.nlargest(top_n, 'Order_Total_Price')
                    st.markdown("<h3 style='text-align: center;'>Top N Customers by Total Order Price</h3>",
                                unsafe_allow_html=True)
                    # Create bar chart for Top N Customers
                    top_chart = alt.Chart(top_customers_filtered).mark_bar().encode(
                        x=alt.X('Customer_Name:O', title='Customer Name',
                                sort=top_customers_filtered['Order_Total_Price'].tolist()),
                        # Customer_Name on X-axis
                        y=alt.Y('Order_Total_Price:Q', title='Total Order Price (€)'),  # Order_Total_Price on Y-axis
                        color=alt.Color('Order_Total_Price:Q', legend=None),  # Color bars by Order_Total_Price
                        tooltip=['Customer_Name:N',
                                 alt.Tooltip('Order_Total_Price:Q', title='Total Order Price (€)', format=".2f")]
                    ).properties(width=350, height=300)
                    # Add text on bars
                    top_chart_text = top_chart.mark_text(
                        align='center',
                        baseline='middle',
                        dy=-10,  # Adjust text position
                        fontSize=12
                    ).encode(
                        text=alt.Text('Order_Total_Price:Q', format=".2f")
                        # Add € symbol and format to 2 decimal places
                    )
                    top_chart = top_chart + top_chart_text
                    top_chart = top_chart.configure_axis(
                        labelAngle=0,
                        labelFontSize=14,
                        titleFontSize=16
                    )
                    st.altair_chart(top_chart, use_container_width=True)

            # For Least Valued Orders
            with chart_col2:
                if df_orders is not None and not df_orders.empty:
                    st.title("Least valued orders")
                    st.markdown("""
                        <div style="border: 2px solid black; padding: 20px; background-color: #454545; border-radius: 10px; text-align: center;">
                            <h3 style="font-size: 30px; color: white; font-weight: bold;">No Data Available for Least Valued Orders.</h3>
                        </div>
                    """, unsafe_allow_html=True)
                else:
                    add_tooltip_css()
                    tooltip_html = render_tooltip(
                        "This bar chart displays the least N customers by their total order price. Hover over each bar to view the customer name and the corresponding total order price in euros.")
                    st.markdown(
                        f"<h1 style='display: inline-block;'>Least valued orders {tooltip_html}</h1>",
                        unsafe_allow_html=True
                    )
                    least_n = st.slider("Select Least N Customers to Display", min_value=1, max_value=50, value=5,
                                        key="top_n_smallest")
                    least_customers_filtered = least_customers.nsmallest(least_n, 'Order_Total_Price')
                    st.markdown("<h3 style='text-align: center;'>Least N Customers by Total Order Price</h3>",
                                unsafe_allow_html=True)
                    least_chart = alt.Chart(least_customers_filtered).mark_bar().encode(
                        x=alt.X('Customer_Name:O', title='Customer Name',
                                sort=least_customers_filtered['Order_Total_Price'].tolist()),
                        # Customer_Name on X-axis
                        y=alt.Y('Order_Total_Price:Q', title='Total Order Price (€)'),  # Order_Total_Price on Y-axis
                        color=alt.Color('Order_Total_Price:Q', legend=None),  # Color bars by Order_Total_Price
                        tooltip=['Customer_Name:N',
                                 alt.Tooltip('Order_Total_Price:Q', title='Total Order Price (€)', format=".2f")]
                    ).properties(width=350, height=300)
                    # Add text on bars
                    least_chart_text = least_chart.mark_text(
                        align='center',
                        baseline='middle',
                        dy=-10,  # Adjust text position
                        fontSize=12
                    ).encode(
                        text=alt.Text('Order_Total_Price:Q', format=".2f")
                        # Add € symbol and format to 2 decimal places
                    )
                    least_chart = least_chart + least_chart_text
                    least_chart = least_chart.configure_axis(
                        labelAngle=0,
                        labelFontSize=14,
                        titleFontSize=16
                    )
                    st.altair_chart(least_chart, use_container_width=True)
        else:
            with chart_col1:
                st.title("Highest valued orders")
                st.markdown("""
                    <div style="border: 2px solid black; padding: 20px; background-color: #454545; border-radius: 10px; text-align: center;">
                        <h3 style="font-size: 30px; color: white; font-weight: bold;">No Data Available for Highest Valued Orders.</h3>
                    </div>
                """, unsafe_allow_html=True)
            with chart_col2:
                st.title("Least valued orders")
                st.markdown("""
                    <div style="border: 2px solid black; padding: 20px; background-color: #454545; border-radius: 10px; text-align: center;">
                        <h3 style="font-size: 30px; color: white; font-weight: bold;">No Data Available for Least Valued Orders.</h3>
                    </div>
                """, unsafe_allow_html=True)

        # Todo-Total Order by Referring Site
        try:
            if df_orders is not None and not df_orders.empty:
                df_unique_orders = df_orders.drop_duplicates(subset="Order_ID", keep="first")
                total_orders_by_site = df_unique_orders.groupby("Order_Referring_Site")["Order_ID"].count().reset_index()
                total_orders_by_site.columns = ["Referring Site", "Total Orders"]
                if not total_orders_by_site.empty:
                    add_tooltip_css()
                    tooltip_html = render_tooltip("This bar chart displays the top N referring sites by total number of orders. Hover over each bar to see the referring site and the corresponding number of orders.")
                    st.markdown(f"<h1 style='display: inline-block;'>Total Orders by Referring Sites {tooltip_html}</h1>",
                                unsafe_allow_html=True)
                    st.markdown("### Visualizing the count of total orders grouped by referring sites")
                    top_n = st.slider("Select Top N Referring Sites to Display", min_value=1,
                                      max_value=len(total_orders_by_site), value=5)
                    top_order_sites = total_orders_by_site.nlargest(top_n, "Total Orders")
                    # Step 4: Create Altair Chart
                    chart = alt.Chart(top_order_sites).mark_bar().encode(
                        x=alt.X("Referring Site:O", title="Referring Site", sort="-y"),
                        y=alt.Y("Total Orders:Q", title="Number of Orders"),
                        color=alt.Color("Total Orders:Q", legend=None),
                        tooltip=["Referring Site:N", "Total Orders:Q"]
                    ).properties(
                        width=700,
                        height=400,
                        title="Top N Referring Sites by Total Orders"
                    )
                    text = chart.mark_text(
                        align="center",
                        baseline="middle",
                        dy=-10  # Adjust text position
                    ).encode(
                        text="Total Orders:Q"
                    )
                    final_chart = chart + text
                    final_chart = final_chart.configure_axis(
                        labelAngle=0,
                        labelFontSize=12,
                        titleFontSize=14
                    )
                    st.altair_chart(final_chart, use_container_width=True)
                else:
                    st.title("Total Orders by Referring Sites")
                    st.markdown("""
                        <div style="border: 2px solid black; padding: 20px; background-color: #454545; border-radius: 10px; text-align: center;">
                            <h3 style="font-size: 30px; color: white; font-weight: bold;">No Data Available for Total Orders by Referring Sites.</h3>
                        </div>
                    """, unsafe_allow_html=True)
            else:
                st.title("Total Orders by Referring Sites")
                st.markdown("""
                    <div style="border: 2px solid black; padding: 20px; background-color: #454545; border-radius: 10px; text-align: center;">
                        <h3 style="font-size: 30px; color: white; font-weight: bold;">No Data Available for Total Orders by Referring Sites.</h3>
                    </div>
                """, unsafe_allow_html=True)

        except:
            st.markdown(
                f"<h3 style='font-size: 30px; color: red; text-align: center;'><b>Datasets Currently are unavailable</b></h3>",
                unsafe_allow_html=True
            )
    except:
        st.markdown(
            f"<h3 style='font-size: 30px; color: red; text-align: center;'><b>Dataset Currently unavaialbe</b></h3>",
            unsafe_allow_html=True)

def show_abandoned_checkouts_page():
    st.title('Abandoned Checkouts')
    add_custom_css()
    try:
        # Todo- Card Creation for the above
        try:
            col1 = st.columns(1)[0]
            if df_abandoned_checkouts is not None and not df_abandoned_checkouts.empty:
                with col1:
                    abandoned_orders = df_abandoned_checkouts['Order_ID'].nunique()
                    add_tooltip_css()
                    tooltip_html = render_tooltip(f"Total number of abandoned orders: {abandoned_orders}")
                    st.markdown(
                        f"<h1 style='display: inline-block;'>Abandoned Orders {tooltip_html}</h1>",
                        unsafe_allow_html=True
                    )
                    st.markdown(
                        f"""
                            <div class="card">
                                <p>Total abandoned orders</p>
                                <h1>{abandoned_orders}</h1>
                            </div>
                            """,
                        unsafe_allow_html=True
                    )
            else:
                with col1:
                    st.title("Abandoned Orders")
                    st.markdown("""
                        <div style="border: 2px solid black; padding: 20px; background-color: #454545; border-radius: 10px; text-align: center;">
                            <h3 style="font-size: 30px; color: white; font-weight: bold;">No Data Available for Total number of abandoned orders</h3>
                        </div>
                    """, unsafe_allow_html=True)

        except:
            st.markdown(
                f"<h3 style='font-size: 30px; color: red; text-align: center;'><b>Datasets Currently is unavaialbe</b></h3>",
                unsafe_allow_html=True)
        if df_abandoned_checkouts is not None and not df_abandoned_checkouts.empty:
            add_tooltip_css()
            tooltip_html = render_tooltip("Preview of abandoned checkouts data filtered by the selected date range.")
            st.markdown(
                f"<h1 style='display: inline-block;'>Preview Filtered Abandoned Checkouts Data {tooltip_html}</h1>",
                unsafe_allow_html=True
            )
            st.subheader("Abandoned Checkouts Data")
            filtered_abandoned_checkouts = filter_by_date(df_abandoned_checkouts, 'Order_Created_At')
            st.dataframe(filtered_abandoned_checkouts)
        else:
            st.title("Preview of Abandoned Checkouts data filtered by the selected date range")
            st.markdown("""
                            <div style="border: 2px solid black; padding: 20px; background-color: #454545; border-radius: 10px; text-align: center;">
                                <h3 style="font-size: 30px; color: white; font-weight: bold;">Data Frame is empty</h3>
                            </div>
                        """, unsafe_allow_html=True)
        # Todo-Total Order placed on weekdays and weekend

        col1, col2 = st.columns(2)
        if df_abandoned_checkouts is not None and not df_abandoned_checkouts.empty:
            add_tooltip_css()
            df_abandoned_checkouts_ = df_abandoned_checkouts.groupby('Order_ID').agg({'Order_Created_At': 'first'})
            df_abandoned_checkouts_['Order_Created_At'] = pd.to_datetime(df_abandoned_checkouts_['Order_Created_At'],
                                                                         errors='coerce', utc=True)
            df_abandoned_checkouts_['Weekday_Weekend'] = df_abandoned_checkouts_['Order_Created_At'].dt.dayofweek.apply(
                lambda x: 'Weekend' if x >= 5 else 'Weekday'
            )
            weekday_count = df_abandoned_checkouts_[df_abandoned_checkouts_['Weekday_Weekend'] == 'Weekday'].shape[0]
            weekend_count = df_abandoned_checkouts_[df_abandoned_checkouts_['Weekday_Weekend'] == 'Weekend'].shape[0]
            counts = [weekday_count, weekend_count]
            labels = ['Weekday', 'Weekend']
            pie_data = pd.DataFrame({
                'Category': labels,
                'Count': counts,
                'Percentage': [(count / sum(counts)) * 100 for count in counts]  # Calculate percentage
            })
            pie_data['Label'] = pie_data['Percentage'].round(1).astype(str) + '%'
            pie_chart = alt.Chart(pie_data).mark_arc().encode(
                theta=alt.Theta(field="Count", type="quantitative"),
                color=alt.Color(field="Category", type="nominal"),
                tooltip=["Category", "Count", "Percentage"],  # Show both count and percentage in tooltip
            )
            with col1:
                # st.write(f"Weekday Count: {weekday_count} ({(weekday_count / sum(counts)) * 100:.2f}%)")
                # st.write(f"Weekend Count: {weekend_count} ({(weekend_count / sum(counts)) * 100:.2f}%)")
                tooltip_html = render_tooltip(
                    "This pie chart shows the distribution of abandoned orders between weekdays and weekends. Hover over each segment to see the category (Weekday or Weekend), the total count of orders, and their percentage representation.")
                st.markdown(
                    f"<h1 style='display: inline-block;'>Total Orders Abandoned: Weekday vs Weekend {tooltip_html}</h1>",
                    unsafe_allow_html=True
                )
                st.altair_chart(pie_chart, use_container_width=True)
        else:
            with col1:
                st.title("Orders abandoned by Weekday/Weekend")
                st.markdown("""
                    <div style="border: 2px solid black; padding: 20px; background-color: #454545; border-radius: 10px; text-align: center;">
                        <h3 style="font-size: 30px; color: white; font-weight: bold;">No Data Available for Orders abandoned by Weekday/Weekend</h3>
                    </div>
                """, unsafe_allow_html=True)
        # Todo----------Total orders abandoned: days of week-------------------
        if df_abandoned_checkouts is not None and not df_abandoned_checkouts.empty:
            df_abandoned_checkouts_ = df_abandoned_checkouts.groupby('Order_ID').agg(
                {'Order_Created_At': 'first'}).reset_index()
            df_abandoned_checkouts_['Order_Created_At'] = pd.to_datetime(df_abandoned_checkouts_['Order_Created_At'],
                                                                         errors='coerce', utc=True)
            with col2:
                df_abandoned_checkouts_['days_of_week'] = df_abandoned_checkouts_[
                    'Order_Created_At'].dt.dayofweek.apply(
                    lambda x: ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'][x]
                )
                day_count = df_abandoned_checkouts_['days_of_week'].value_counts()
                day_count = day_count.reindex(
                    ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'],
                    fill_value=0)
                pie_data = pd.DataFrame({
                    'Day': day_count.index,
                    'Count': day_count.values
                })
                pie_chart = alt.Chart(pie_data).mark_arc().encode(
                    theta=alt.Theta(field="Count", type="quantitative"),
                    color=alt.Color(field="Day", type="nominal",
                                    sort=['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday',
                                          'Sunday']),
                    tooltip=["Day:N", "Count:Q"]
                )
                add_tooltip_css()
                tooltip_html = render_tooltip(
                    "Hover over the chart segments to view the number of abandoned orders for each day of the week. The chart shows how orders are distributed across different weekdays, with each segment representing a specific day.")
                st.markdown(
                    f"<h1 style='display: inline-block;'>Total Orders Abandoned: Days of Week {tooltip_html}</h1>",
                    unsafe_allow_html=True
                )
                st.altair_chart(pie_chart, use_container_width=True)
        else:
            with col2:
                st.title("Orders abandoned by Day of the Week")
                st.markdown("""
                    <div style="border: 2px solid black; padding: 20px; background-color: #454545; border-radius: 10px; text-align: center;">
                        <h3 style="font-size: 30px; color: white; font-weight: bold;">No Data Available for Orders abandoned by Day of the Week</h3>
                    </div>
                """, unsafe_allow_html=True)
        # Todo- Total orders abandoned: hours of day
        try:
            col1 = st.columns(1)[0]
            if df_abandoned_checkouts is not None and not df_abandoned_checkouts.empty:
                df_abandoned_checkouts_ = df_abandoned_checkouts.groupby('Order_ID').agg(
                    {'Order_Created_At': 'first'}).reset_index()
                df_abandoned_checkouts_['Order_Created_At'] = pd.to_datetime(
                    df_abandoned_checkouts_['Order_Created_At'],
                    errors='coerce', utc=True)
                with col1:
                    # Add 'hour_of_day' column for the df_abandoned_checkouts_ DataFrame
                    df_abandoned_checkouts_['hour_of_day'] = df_abandoned_checkouts_[
                                                                 'Order_Created_At'].dt.hour + 1  # Shift hours to 1-24 range
                    # Count orders per hour
                    hour_count = df_abandoned_checkouts_['hour_of_day'].value_counts().sort_index()
                    # Reindex to ensure every hour from 1 to 24 is included
                    hour_count = hour_count.reindex(range(1, 25), fill_value=0)
                    # Prepare data for the chart
                    hour_data = pd.DataFrame({
                        'Hour of Day': hour_count.index,
                        'Number of Orders': hour_count.values
                    })
                    add_tooltip_css()
                    tooltip_html = render_tooltip(
                        "Hover over the chart to see the number of abandoned orders for each hour of the day. The chart visualizes the distribution of abandoned orders across different hours, providing insights into peak abandonment times.")
                    st.markdown(
                        f"<h1 style='display: inline-block;'>Total Orders Abandoned: Hours of Day {tooltip_html}</h1>",
                        unsafe_allow_html=True
                    )

                    st.markdown("<h3 style='text-align: center;'>Orders abandoned by Hour of the Day</h3>",
                                unsafe_allow_html=True)
                    line_chart = alt.Chart(hour_data).mark_line().encode(
                        x=alt.X('Hour of Day:O', title='Hour of Day', scale=alt.Scale(domain=list(range(1, 25)))),
                        y=alt.Y('Number of Orders:Q', title='Number of Orders'),
                        tooltip=['Hour of Day', 'Number of Orders']
                    ).properties(
                        width=700,
                        height=400
                    )
                    # Add points to the line chart
                    points = alt.Chart(hour_data).mark_point(size=100, color='red').encode(
                        x=alt.X('Hour of Day:O', title='Hour of Day'),
                        y=alt.Y('Number of Orders:Q', title='Number of Orders'),
                        tooltip=['Hour of Day', 'Number of Orders']
                    )
                    # Combine the line chart and points
                    combined_chart = line_chart + points
                    st.altair_chart(combined_chart, use_container_width=True)
            else:
                with col1:
                    st.title("Orders abandoned by Hour of the Day")
                    st.markdown("""
                        <div style="border: 2px solid black; padding: 20px; background-color: #454545; border-radius: 10px; text-align: center;">
                            <h3 style="font-size: 30px; color: white; font-weight: bold;">No Data Available for Orders abandoned by Hour of the Day</h3>
                        </div>
                    """, unsafe_allow_html=True)
        except:
            st.markdown(
                f"<h3 style='font-size: 30px; color: red; text-align: center;'><b>Datasets Currently is unavaialbe</b></h3>",
                unsafe_allow_html=True)

        # Todo-Total orders abandoned: day, month, quarter, year
        try:
            col1 = st.columns(1)[0]
            if df_abandoned_checkouts is not None and not df_abandoned_checkouts.empty:
                df_abandoned_checkouts_ = df_abandoned_checkouts.groupby('Order_ID').agg(
                    {'Order_Created_At': 'first'}).reset_index()
                # Convert 'Order_Created_At' to datetime if not already done for abandoned checkouts
                df_abandoned_checkouts_['Order_Created_At'] = pd.to_datetime(
                    df_abandoned_checkouts_['Order_Created_At'],
                    errors='coerce', utc=True)
                with col1:
                    # Extract the day, month, quarter, and year from 'Order_Created_At'
                    df_abandoned_checkouts_['day'] = df_abandoned_checkouts_['Order_Created_At'].dt.date
                    # Calculate total abandoned orders placed per day
                    abandoned_orders_per_day = df_abandoned_checkouts_.groupby('day').size().reset_index(
                        name='order_count')
                    # Create a complete date range for abandoned orders
                    start_date = abandoned_orders_per_day['day'].min()
                    end_date = datetime.today().date()
                    full_date_range = pd.date_range(start=start_date, end=end_date)
                    # Reindex `abandoned_orders_per_day` to ensure all dates are included
                    abandoned_orders_per_day.set_index('day', inplace=True)
                    abandoned_orders_per_day = abandoned_orders_per_day.reindex(full_date_range,
                                                                                fill_value=0).reset_index()
                    abandoned_orders_per_day.rename(columns={'index': 'day'}, inplace=True)
                    # Add 'month', 'quarter', and 'year' columns
                    abandoned_orders_per_day['month'] = abandoned_orders_per_day['day'].dt.to_period('M').astype(
                        str)  # Year-Month format
                    abandoned_orders_per_day['quarter'] = abandoned_orders_per_day['day'].dt.to_period('Q').astype(
                        str)  # Year-Quarter format
                    abandoned_orders_per_day['year'] = abandoned_orders_per_day['day'].dt.year  # Year format
                    # Group data for abandoned orders
                    abandoned_orders_per_month = abandoned_orders_per_day.groupby('month')[
                        'order_count'].sum().reset_index()
                    abandoned_orders_per_month['month'] = pd.to_datetime(abandoned_orders_per_month['month'],
                                                                         format='%Y-%m')
                    abandoned_orders_per_month = abandoned_orders_per_month.sort_values(by='month')
                    # Group data for Quarter data set
                    abandoned_orders_per_quarter = abandoned_orders_per_day.groupby('quarter')[
                        'order_count'].sum().reset_index()
                    # group data for year dataset
                    abandoned_orders_per_year = abandoned_orders_per_day.groupby('year')[
                        'order_count'].sum().reset_index()
                    add_tooltip_css()
                    tooltip_html = render_tooltip(
                        "Hover over the chart to view detailed data for each time period.See the total number of abandoned orders for each day, month, quarter, or year.Check the specific order count for each time interval.The tooltip shows both the time period and the corresponding abandoned order count.")
                    st.markdown(
                        f"<h1 style='display: inline-block;'>Abandoned Order Count Visualizations {tooltip_html}</h1>",
                        unsafe_allow_html=True
                    )
                    view = st.radio("Select View", ['Abandoned Orders per Day', 'Abandoned Orders per Month',
                                                    'Abandoned Orders per Quarter', 'Abandoned Orders per Year'])
                    if view == 'Abandoned Orders per Day':
                        st.title('Abandoned Orders: Days')
                        st.markdown("<h3 style='text-align: center;'>Abandoned Orders by Day</h3>",
                                    unsafe_allow_html=True)
                        line_chart = alt.Chart(abandoned_orders_per_day).mark_line().encode(
                            x=alt.X('day:T', title='Date',
                                    axis=alt.Axis(format="%b %d, %Y", labelAngle=-90, tickMinStep=1)),
                            y=alt.Y('order_count:Q', title='Number of Abandoned Orders'),
                            tooltip=['day:T', 'order_count:Q']
                        )
                        points = alt.Chart(abandoned_orders_per_day).mark_point(size=60, color='red').encode(
                            x='day:T',
                            y='order_count:Q',
                            tooltip=['day:T', 'order_count:Q']
                        )
                        combined_chart = line_chart + points
                        st.altair_chart(combined_chart, use_container_width=True)

                    elif view == 'Abandoned Orders per Month':
                        st.title('Abandoned Orders: Months')
                        st.markdown("<h3 style='text-align: center;'>Abandoned Orders by Month</h3>",
                                    unsafe_allow_html=True)
                        # Ensure 'month' is a datetime column
                        abandoned_orders_per_month['month'] = pd.to_datetime(abandoned_orders_per_month['month'],
                                                                             format='%Y-%m-%d')
                        # Create the chart with formatted month labels
                        line_chart = alt.Chart(abandoned_orders_per_month).mark_line().encode(
                            x=alt.X('month:T', title='Month', axis=alt.Axis(format='%b %Y', labelAngle=-45)),
                            # Date type with custom formatting
                            y=alt.Y('order_count:Q', title='Number of Abandoned Orders'),
                            tooltip=[alt.Tooltip('month:T', title='Month'), 'order_count:Q']
                        )
                        points = alt.Chart(abandoned_orders_per_month).mark_point(size=60, color='red').encode(
                            x=alt.X('month:T', title='Month'),
                            y=alt.Y('order_count:Q', title='Number of Abandoned Orders'),
                            tooltip=[alt.Tooltip('month:T', title='Month'), 'order_count:Q']
                        )
                        combined_chart = line_chart + points
                        st.altair_chart(combined_chart, use_container_width=True)

                    elif view == 'Abandoned Orders per Quarter':
                        st.title('Abandoned Orders: Quarters')
                        st.markdown("<h3 style='text-align: center;'>Abandoned Orders by Quarter</h3>",
                                    unsafe_allow_html=True)
                        line_chart = alt.Chart(abandoned_orders_per_quarter).mark_line().encode(
                            x=alt.X('quarter:N', title='Quarter'),
                            y=alt.Y('order_count:Q', title='Number of Abandoned Orders'),
                            tooltip=['quarter:N', 'order_count:Q']
                        )
                        points = alt.Chart(abandoned_orders_per_quarter).mark_point(size=60, color='red').encode(
                            x='quarter:N',
                            y='order_count:Q',
                            tooltip=['quarter:N', 'order_count:Q']
                        )
                        combined_chart = line_chart + points
                        st.altair_chart(combined_chart, use_container_width=True)
                    elif view == 'Abandoned Orders per Year':
                        st.title('Abandoned Orders: Years')
                        st.markdown("<h3 style='text-align: center;'>Abandoned Orders by Year</h3>",
                                    unsafe_allow_html=True)
                        line_chart = alt.Chart(abandoned_orders_per_year).mark_line().encode(
                            x=alt.X('year:O', title='Year'),
                            y=alt.Y('order_count:Q', title='Number of Abandoned Orders'),
                            tooltip=['year:O', 'order_count:Q']
                        )
                        points = alt.Chart(abandoned_orders_per_year).mark_point(size=60, color='red').encode(
                            x='year:O',
                            y='order_count:Q',
                            tooltip=['year:O', 'order_count:Q']
                        )
                        combined_chart = line_chart + points
                        st.altair_chart(combined_chart, use_container_width=True)

                # Todo-Average abandoned orders per customer------------------------------------------------------------
                col1, col2 = st.columns(2)
                with col1:
                    abandoned_orders_per_customer = df_abandoned_checkouts.groupby('Customer_ID')['Order_ID'].nunique()
                    average_abandoned_orders = abandoned_orders_per_customer.mean()
                    add_tooltip_css()
                    tooltip_html = render_tooltip(f"Total number of abandoned orders: {abandoned_orders}")
                    st.markdown(
                        f"<h1 style='display: inline-block;'>Average Abandoned Orders {tooltip_html}</h1>",
                        unsafe_allow_html=True
                    )
                    st.markdown(
                        f"""
                            <div class="card">
                                <p>Average abandoned orders per customer</p>
                                <h1>{int(average_abandoned_orders)}</h1>
                            </div>
                            """,
                        unsafe_allow_html=True
                    )
                with col2:
                    most_abandoned_orders_per_customer = df_abandoned_checkouts.groupby('Customer_ID')[
                        'Order_ID'].nunique()
                    most_abandoned_orders_per_customer = most_abandoned_orders_per_customer.max()
                    add_tooltip_css()
                    tooltip_html = render_tooltip(
                        f"Customer with the most abandoned orders: {most_abandoned_orders_per_customer}")
                    st.markdown(
                        f"<h1 style='display: inline-block;'>Most orders Abandoned {tooltip_html}</h1>",
                        unsafe_allow_html=True
                    )
                    st.markdown(
                        f"""
                            <div class="card">
                                <p>Most orders abandoned by a customer</p>
                                <h1>{int(most_abandoned_orders_per_customer)}</h1>
                            </div>
                            """,
                        unsafe_allow_html=True
                    )
            else:
                with col1:
                    st.title("Total orders abandoned: day, month, quarter, year")
                    st.markdown("""
                        <div style="border: 2px solid black; padding: 20px; background-color: #454545; border-radius: 10px; text-align: center;">
                            <h3 style="font-size: 30px; color: white; font-weight: bold;">No Data Available for Total orders abandoned day, month, quarter, year</h3>
                        </div>
                    """, unsafe_allow_html=True)

            # Todo- Referring Sites by Abandoned Orders Top N
            if df_abandoned_checkouts is not None and not df_abandoned_checkouts.empty:
                df_abandoned_checkouts['Order_Referring_Site'] = df_abandoned_checkouts['Order_Referring_Site'].fillna(
                    'Unknown')  # Handle missing values
                df_abandoned_checkouts['Order_ID'] = df_abandoned_checkouts['Order_ID'].astype(
                    str)  # Ensure Order_ID is treated as a string
                referring_sites = df_abandoned_checkouts.groupby('Order_Referring_Site')[
                    'Order_ID'].nunique().reset_index()
                referring_sites = referring_sites.rename(columns={'Order_ID': 'Total_Abandoned_Orders'})
                referring_sites = referring_sites.sort_values('Total_Abandoned_Orders', ascending=False)
                add_tooltip_css()
                tooltip_html = render_tooltip("""Hover over the chart to view detailed data for each referring site.
                    Check the total number of abandoned orders associated with each referring site.
                    The tooltip shows the referring site name and the corresponding abandoned order count.""")
                st.markdown(
                    f"<h1 style='display: inline-block;'>Total Abandoned Orders by Referring Sites {tooltip_html}</h1>",
                    unsafe_allow_html=True
                )
                top_n = st.slider("Select Top N Referring Sites to Display", min_value=1, max_value=50, value=10,
                                  key="top_n_sites")
                top_referring_sites = referring_sites.head(top_n)
                st.markdown("<h3 style='text-align: center;'>Top N Referring Sites by Abandoned Orders</h3>",
                            unsafe_allow_html=True)
                chart = alt.Chart(top_referring_sites).mark_bar().encode(
                    x=alt.X('Order_Referring_Site:O', title='Referring Site', sort='-y'),  # X-axis for sites
                    y=alt.Y('Total_Abandoned_Orders:Q', title='Total Abandoned Orders'),
                    # Y-axis for total abandoned orders
                    color=alt.Color('Total_Abandoned_Orders:Q', legend=None),  # Color bars by count
                    tooltip=['Order_Referring_Site:N', 'Total_Abandoned_Orders:Q']  # Add tooltips
                ).properties(width=700, height=400)
                chart_text = chart.mark_text(
                    align='center',
                    baseline='bottom',
                    dy=-10,
                    fontSize=12
                ).encode(
                    text='Total_Abandoned_Orders:Q'
                )
                final_chart = chart + chart_text
                st.altair_chart(final_chart, use_container_width=True)
            else:
                st.title("Total Abandoned Orders by Referring Sites")
                st.markdown("""
                    <div style="border: 2px solid black; padding: 20px; background-color: #454545; border-radius: 10px; text-align: center;">
                        <h3 style="font-size: 30px; color: white; font-weight: bold;">No Data Available for Total Abandoned Orders by Referring Sites</h3>
                    </div>
                """, unsafe_allow_html=True)
        except:
            st.markdown(
                f"<h3 style='font-size: 30px; color: red; text-align: center;'><b>Datasets Currently is unavaialbe</b></h3>",
                unsafe_allow_html=True)
    except:
        st.markdown(
            f"<h3 style='font-size: 30px; color: red; text-align: center;'><b>Dataset Currently unavaialbe</b></h3>",
            unsafe_allow_html=True)

def show_products_page():
    st.title('Products Data')
    add_custom_css()
    try:
        # Todo-Average number of products ordered by a customer
        try:
            col1, col2 = st.columns(2)
            if df_orders is not None and not df_orders.empty:
                customer_product_counts = df_orders.groupby('Customer_ID')['Product_ID'].nunique().reset_index()
                average_products_per_customer = customer_product_counts['Product_ID'].mean()
                average_products_per_customer = round(average_products_per_customer, 2)
                # Total Product counts---------------------------------------------
                df_products_cleaned = df_products.dropna(subset=['Product_Published_At'])
                total_product_count = df_products_cleaned['Product_ID'].nunique()
                with col1:
                    add_tooltip_css()
                    tooltip_html = render_tooltip(
                        f"The average number of products per customer is {average_products_per_customer}")
                    st.markdown(
                        f"<h1 style='display: inline-block;'>Average order {tooltip_html}</h1>", unsafe_allow_html=True
                    )
                    st.markdown(
                        f"""
                               <div class="card">
                                   <p>Average number of products ordered by a customer</p>
                                   <h1>{average_products_per_customer}</h1>
                               </div>
                               """,
                        unsafe_allow_html=True
                    )
                with col2:
                    add_tooltip_css()
                    tooltip_html = render_tooltip(f"The total number of products listed is {total_product_count}")
                    st.markdown(
                        f"<h1 style='display: inline-block;'>Total products {tooltip_html}</h1>", unsafe_allow_html=True
                    )
                    st.markdown(
                        f"""
                               <div class="card">
                                   <p>Total products</p>
                                   <h1>{total_product_count}</h1>
                               </div>
                               """,
                        unsafe_allow_html=True
                    )
            else:
                with col1:
                    st.title("Average Order")
                    st.markdown("""
                        <div style="border: 2px solid black; padding: 20px; background-color: #454545; border-radius: 10px; text-align: center;">
                            <h3 style="font-size: 30px; color: white; font-weight: bold;">No Data Available for average products</h3>
                        </div>
                    """, unsafe_allow_html=True)
                with col2:
                    st.title("Total products")
                    st.markdown("""
                        <div style="border: 2px solid black; padding: 20px; background-color: #454545; border-radius: 10px; text-align: center;">
                            <h3 style="font-size: 30px; color: white; font-weight: bold;">No Data Available for Total products</h3>
                        </div>
                    """, unsafe_allow_html=True)
        except:
            st.markdown(
                f"<h3 style='font-size: 30px; color: red; text-align: center;'><b>Datasets Currently is unavaialbe</b></h3>",
                unsafe_allow_html=True)
        # print(df_products)
        if df_products is not None and not df_products.empty:
            add_tooltip_css()
            tooltip_html = render_tooltip("Preview of product data filtered by the selected date range.")
            st.markdown(f"<h1 style='display: inline-block;'>Preview Filtered Product Data {tooltip_html}</h1>",
                        unsafe_allow_html=True)
            filtered_products = filter_by_date(df_products, 'Product_Created_At')
            st.dataframe(filtered_products, use_container_width=True)
        else:
            st.title("Preview of product data filtered by the selected date range")
            st.markdown("""
                <div style="border: 2px solid black; padding: 20px; background-color: #454545; border-radius: 10px; text-align: center;">
                    <h3 style="font-size: 30px; color: white; font-weight: bold;">No Data Available for Products</h3>
                </div>
            """, unsafe_allow_html=True)

        # Todo-Count of products in each type----------------------------------------
        try:
            col1, col2 = st.columns(2)
            if df_products is not None and not df_products.empty:
                with col1:
                    df_products_ = df_products.dropna(subset=['Product_Published_At'])
                    # df_products_['Product_Type'] = df_products_['Product_Type'].replace("", "No Type")
                    df_products_['Product_Type'] = df_products_['Product_Type'].astype(str).replace(
                        {"nan": "No Type", "": "No Type"})
                    product_counts = df_products_.groupby('Product_Type')['Product_ID'].nunique().reset_index()
                    product_counts.columns = ['Product_Type', 'Count']
                    add_tooltip_css()
                    tooltip_html = render_tooltip("Hover over the bars to see the product type and the corresponding count of unique products in that type.")
                    st.markdown(f"<h1 style='display: inline-block;'>Product Count by Type {tooltip_html}</h1>",
                                unsafe_allow_html=True)
                    st.markdown("### Visualizing the count of unique products in each type")
                    top_n = st.slider("Select Top N Product Types to Display", min_value=1,
                                      max_value=len(product_counts),
                                      value=5)
                    top_product_counts = product_counts.nlargest(top_n, 'Count')
                    chart = alt.Chart(top_product_counts).mark_bar().encode(
                        x=alt.X('Product_Type:O', title='Product Type', sort='-y'),
                        y=alt.Y('Count:Q', title='Number of Products'),
                        color=alt.Color('Count:Q', legend=None),
                        tooltip=['Product_Type:N', 'Count:Q']
                    ).properties(
                        width=700,
                        height=400,
                        title="Top N Product Types by Count"
                    )
                    text = chart.mark_text(
                        align='center',
                        baseline='middle',
                        dy=-10  # Adjust text position
                    ).encode(
                        text='Count:Q'
                    )
                    final_chart = chart + text
                    final_chart = final_chart.configure_axis(
                        labelAngle=0,
                        labelFontSize=12,
                        titleFontSize=14
                    )
                    st.altair_chart(final_chart, use_container_width=True)
                with col2:
                    # Todo- Most Sold Product----------------------------------------------
                    if df_orders is not None and not df_orders.empty:
                        product_sales = df_orders.groupby('Product_Name')['Product_Quantity'].sum().reset_index()
                        product_sales = product_sales.sort_values(by='Product_Quantity', ascending=False)
                        add_tooltip_css()
                        tooltip_html = render_tooltip(
                            "Hover over the bars to see the product name and the total quantity sold for each product.")
                        st.markdown(
                            f"<h1 style='display: inline-block;'>Most Sold Products {tooltip_html}</h1>",
                            unsafe_allow_html=True
                        )
                        st.markdown("### Displaying the most sold products by quantity")
                        top_n = st.slider("Select Top N Most Sold Products to Display", min_value=1,
                                          max_value=len(product_sales),
                                          value=5)
                        top_sold_products = product_sales.head(top_n)
                        chart = alt.Chart(top_sold_products).mark_bar().encode(
                            x=alt.X('Product_Name:O', title='Product Name', sort='-y'),
                            y=alt.Y('Product_Quantity:Q', title='Quantity Sold'),
                            color=alt.Color('Product_Quantity:Q', legend=None),
                            tooltip=['Product_Name:N', 'Product_Quantity:Q']
                        ).properties(
                            width=700,
                            height=400,
                            title="Top N Most Sold Products"
                        )
                        text = chart.mark_text(
                            align='center',
                            baseline='middle',
                            dy=-10  # Adjust text position
                        ).encode(
                            text='Product_Quantity:Q'
                        )
                        final_chart = chart + text
                        final_chart = final_chart.configure_axis(
                            labelAngle=0,
                            labelFontSize=12,
                            titleFontSize=14
                        )
                        st.altair_chart(final_chart, use_container_width=True)
                    else:
                        st.title("Most Sold Products")
                        st.markdown("""
                            <div style="border: 2px solid black; padding: 20px; background-color: #454545; border-radius: 10px; text-align: center;">
                                <h3 style="font-size: 30px; color: white; font-weight: bold;">No Data Available for Most Sold Products</h3>
                            </div>
                        """, unsafe_allow_html=True)
            else:
                with col1:
                    st.title("Product Count by Type")
                    st.markdown("""
                        <div style="border: 2px solid black; padding: 20px; background-color: #454545; border-radius: 10px; text-align: center;">
                            <h3 style="font-size: 30px; color: white; font-weight: bold;">No Data Available for Product Count by Type</h3>
                        </div>
                    """, unsafe_allow_html=True)
                with col2:
                    st.title("Most Sold Products")
                    st.markdown("""
                        <div style="border: 2px solid black; padding: 20px; background-color: #454545; border-radius: 10px; text-align: center;">
                            <h3 style="font-size: 30px; color: white; font-weight: bold;">No Data Available for Most Sold Products</h3>
                        </div>
                    """, unsafe_allow_html=True)
        except:
            st.markdown(
                f"<h3 style='font-size: 30px; color: red; text-align: center;'><b>Datasets Currently is unavaialbe</b></h3>",
                unsafe_allow_html=True)

        # Todo-Most Price Product
        try:
            col1, col2 = st.columns(2)
            if df_products is not None and not df_products.empty:
                with col1:
                    df_products_ = df_products.dropna(subset=['Product_Published_At'])
                    most_priced = (
                        df_products_.groupby(["Product_ID", "Product_Title"])
                        .agg({"Variant_Price": "max"})
                        .reset_index()
                        .sort_values(by="Variant_Price", ascending=False)
                    )
                    add_tooltip_css()
                    tooltip_html = render_tooltip(
                        "Hover over the bars to view detailed information about the product title and its highest variant price, showcasing the most expensive products.")
                    st.markdown(
                        f"<h1 style='display: inline-block;'>Most Priced Products {tooltip_html}</h1>",
                        unsafe_allow_html=True
                    )
                    st.markdown("### Displaying the most expensive products by price")
                    top_n = st.slider("Select Top N Most Priced Products to Display", min_value=1,
                                      max_value=len(most_priced), value=5)
                    top_priced_products = most_priced.head(top_n)
                    chart = alt.Chart(top_priced_products).mark_bar().encode(
                        x=alt.X('Product_Title:O', title='Product Title', sort='-y'),
                        y=alt.Y('Variant_Price:Q', title='Price ($)'),
                        color=alt.Color('Variant_Price:Q', legend=None),
                        tooltip=['Product_Title:N', 'Variant_Price:Q']
                    ).properties(
                        width=700,
                        height=400,
                        title="Top N Most Priced Products"
                    )
                    text = chart.mark_text(
                        align='center',
                        baseline='middle',
                        dy=-10  # Adjust text position
                    ).encode(
                        text='Variant_Price:Q'
                    )
                    final_chart = chart + text
                    final_chart = final_chart.configure_axis(
                        labelAngle=90,
                        labelFontSize=12,
                        titleFontSize=14
                    )
                    st.altair_chart(final_chart, use_container_width=True)
                with col2:
                    df_products_ = df_products.dropna(subset=['Product_Published_At'])
                    Least_priced = (
                        df_products_.groupby(["Product_ID", "Product_Title"])
                        .agg({"Variant_Price": "min"})
                        .reset_index()
                    )
                    price_order = Least_priced.sort_values(by="Variant_Price", ascending=True)["Product_Title"].tolist()
                    add_tooltip_css()
                    tooltip_html = render_tooltip(
                        "Hover over the bars to view the detailed information about the product title and its lowest variant price, showcasing the least expensive products.")
                    st.markdown(
                        f"<h1 style='display: inline-block;'>Least Priced Products {tooltip_html}</h1>",
                        unsafe_allow_html=True
                    )
                    st.markdown("### Displaying the Least expensive products by price")
                    top_n = st.slider("Select Top N Products to Display", min_value=1, max_value=len(Least_priced),
                                      value=5)
                    top_priced_products = Least_priced.head(top_n)
                    chart = alt.Chart(top_priced_products).mark_bar().encode(
                        x=alt.X('Product_Title:O', title='Product Title', sort=price_order),
                        # Custom sort order for X axis
                        y=alt.Y('Variant_Price:Q', title='Price ($)'),
                        color=alt.Color('Variant_Price:Q', legend=None),
                        tooltip=['Product_Title:N', 'Variant_Price:Q']
                    ).properties(
                        width=700,
                        height=400,
                        title="Top N Least Priced Products"
                    )
                    text = chart.mark_text(
                        align='center',
                        baseline='middle',
                        dy=-10  # Adjust text position
                    ).encode(
                        text='Variant_Price:Q'
                    )
                    # Combine chart and text
                    final_chart = chart + text
                    final_chart = final_chart.configure_axis(
                        labelAngle=90,
                        labelFontSize=12,
                        titleFontSize=14
                    )
                    # Display chart
                    st.altair_chart(final_chart, use_container_width=True)
            else:
                with col1:
                    st.title("Most Priced Products ")
                    st.markdown("""
                        <div style="border: 2px solid black; padding: 20px; background-color: #454545; border-radius: 10px; text-align: center;">
                            <h3 style="font-size: 30px; color: white; font-weight: bold;">No Data Available for Most Priced Products </h3>
                        </div>
                    """, unsafe_allow_html=True)
                with col2:
                    st.title("Least Priced Products")
                    st.markdown("""
                        <div style="border: 2px solid black; padding: 20px; background-color: #454545; border-radius: 10px; text-align: center;">
                            <h3 style="font-size: 30px; color: white; font-weight: bold;">No Data Available for Least Priced Products</h3>
                        </div>
                    """, unsafe_allow_html=True)
        except:
            st.markdown(
                f"<h3 style='font-size: 30px; color: red; text-align: center;'><b>Datasets Currently is unavaialbe</b></h3>",
                unsafe_allow_html=True)

        # Todo- List for unsold product----------------------------------
        try:
            if (df_products is not None and not df_products.empty) and (df_orders is not None and not df_orders.empty):
                df_products_cleaned = df_products.dropna(subset=['Product_Published_At'])
                sold_product_ids = df_orders['Product_ID'].unique()
                all_product_ids = df_products_cleaned['Product_ID']
                unsold_product_ids = all_product_ids[~all_product_ids.isin(sold_product_ids)]
                unsold_products = df_products_cleaned[df_products_cleaned['Product_ID'].isin(unsold_product_ids)]
                unsold_products_grouped = unsold_products.groupby(
                    ['Product_ID', 'Product_Title', 'Product_Published_At'], as_index=False).first()
                unsold_products_grouped['Product_ID'] = unsold_products_grouped['Product_ID'].astype(str).replace(",",
                                                                                                                  "",
                                                                                                                  regex=True)
                unsold_products_grouped['Product_Published_At'] = \
                unsold_products_grouped['Product_Published_At'].str.split("T").str[0]
                unsold_products_grouped['Product_Published_At'] = pd.to_datetime(
                    unsold_products_grouped['Product_Published_At'])
                unsold_products_grouped_sorted = unsold_products_grouped.sort_values(by='Product_Published_At',
                                                                                     ascending=True)
                unsold_products_grouped_sorted['Product_Published_At'] = unsold_products_grouped_sorted[
                    'Product_Published_At'].dt.date
                Data_display = unsold_products_grouped_sorted[['Product_ID', 'Product_Title', 'Product_Published_At']]
                add_tooltip_css()
                tooltip_html = render_tooltip(
                    "Displaying a summary of products that have not been sold, including their product IDs, titles, and the date they were published.")
                st.markdown(
                    f"<h1 style='display: inline-block;'>Unsold Products Summary {tooltip_html}</h1>",
                    unsafe_allow_html=True
                )
                st.markdown("### List of Products That Have Not Been Sold")
                st.dataframe(Data_display, use_container_width=True)
            else:
                st.title("Unsold Products Summary")
                st.markdown("""
                    <div style="border: 2px solid black; padding: 20px; background-color: #454545; border-radius: 10px; text-align: center;">
                        <h3 style="font-size: 30px; color: white; font-weight: bold;">No Data Available for Unsold Products Summary</h3>
                    </div>
                """, unsafe_allow_html=True)

            # Todo-Count of products in each price range
            if df_products is not None and not df_products.empty:
                df_products_cleaned = df_products.dropna(subset=['Product_Published_At'])
                min_price = df_products_cleaned['Variant_Price'].min()
                max_price = df_products_cleaned['Variant_Price'].max()
                price_range = max_price - min_price
                q1 = df_products_cleaned['Variant_Price'].quantile(0.25)
                q3 = df_products_cleaned['Variant_Price'].quantile(0.75)
                iqr = q3 - q1
                n = len(df_products_cleaned)
                # Freedman-Diaconis Rule to calculate bin width
                if iqr > 0 and n > 1:
                    bin_width = 2 * iqr / (n ** (1 / 3))  # Dynamic bin width
                else:
                    bin_width = price_range / 5  # Fallback: divide into 5 bins if data is uniform
                # Calculate the number of bins dynamically
                num_bins = max(1, int(price_range / bin_width))  # Ensure at least 1 bin
                # Create dynamic price bins based on the min and max price
                price_bins = pd.cut(df_products_cleaned['Variant_Price'], bins=num_bins)
                # Adjust bin edges if any lower bounds are negative
                bin_edges = price_bins.cat.categories
                if bin_edges[0].left < 0:
                    bin_edges = pd.IntervalIndex(
                        [pd.Interval(max(0, interval.left), interval.right) for interval in bin_edges])
                    price_bins = pd.cut(df_products_cleaned['Variant_Price'], bins=bin_edges)
                # Create dynamic labels for the price bins
                price_labels = [f"{max(0, round(bin.left, 2))} - {round(bin.right, 2)}" for bin in
                                price_bins.cat.categories]
                # Create a new column 'Price_Range' to categorize products into dynamic price ranges
                df_products_cleaned['Price_Range'] = pd.cut(df_products_cleaned['Variant_Price'], bins=num_bins,
                                                            labels=price_labels, include_lowest=True)
                price_range_counts = df_products_cleaned['Price_Range'].value_counts().reset_index()
                price_range_counts.columns = ['Price_Range', 'Count']
                add_tooltip_css()
                tooltip_html = render_tooltip(
                    "This chart displays the count of products in different price ranges. Hover over each bar to view the specific price range and the corresponding number of products.")
                st.markdown(
                    f"<h1 style='display: inline-block;'>Count of Products in Each Price Range {tooltip_html}</h1>",
                    unsafe_allow_html=True
                )
                st.markdown("### Displaying the count of products in different price ranges")
                chart = alt.Chart(price_range_counts).mark_bar().encode(
                    x=alt.X('Price_Range:N', title='Price Range', sort=price_labels),
                    y=alt.Y('Count:Q', title='Number of Products'),
                    color=alt.Color('Count:Q', legend=None),
                    tooltip=['Price_Range:N', 'Count:Q']
                ).properties(
                    width=700,
                    height=400,
                    title="Count of Products in Each Price Range"
                )
                # Adding text labels on the bars
                text = chart.mark_text(
                    align='center',
                    baseline='middle',
                    dy=-10  # Adjust text position
                ).encode(
                    text='Count:Q'
                )
                final_chart = chart + text
                final_chart = final_chart.configure_axis(
                    labelAngle=90,
                    labelFontSize=10,
                    titleFontSize=14
                )
                st.altair_chart(final_chart, use_container_width=True)
            else:
                st.title("Count of Products in Each Price Range")
                st.markdown("""
                    <div style="border: 2px solid black; padding: 20px; background-color: #454545; border-radius: 10px; text-align: center;">
                        <h3 style="font-size: 30px; color: white; font-weight: bold;">No Data Available for Count of Products in Each Price Range</h3>
                    </div>
                """, unsafe_allow_html=True)

        except:
            st.markdown(
                f"<h3 style='font-size: 30px; color: red; text-align: center;'><b>Datasets Currently is unavaialbe</b></h3>",
                unsafe_allow_html=True)
    except:
        st.markdown(
            f"<h3 style='font-size: 30px; color: red; text-align: center;'><b>Dataset Currently is unavaialbe</b></h3>",
            unsafe_allow_html=True)

def show_revenue_page():
    st.title('Revenue Data')
    add_custom_css()
    try:
        try:
            col1, col2, col3 = st.columns(3)
            if df_orders is not None and not df_orders.empty:
                df_unique_orders = df_orders.drop_duplicates(subset='Order_ID', keep='first')
                Total_price = df_unique_orders['Order_Total_Price'].sum()
                Total_price = round(Total_price, 2)
                Average_Revenue_ = df_unique_orders['Order_Total_Price'].mean()
                Average_Revenue = round(Average_Revenue_, 2)
                Total_amaount_refund = df_unique_orders['Order_Refund_Amount'].sum()
                with col1:
                    add_tooltip_css()
                    tooltip_html = render_tooltip(
                        f"This section calculates Total revenue from unique orders: €{Total_price}")
                    st.markdown(
                        f"<h1 style='display: inline-block;'>Total revenue {tooltip_html}</h1>", unsafe_allow_html=True
                    )
                    st.markdown(
                        f"""
                                   <div class="card">
                                       <p>Total revenue</p>
                                       <h1>€{Total_price}</h1>

                                   </div>
                                   """,
                        unsafe_allow_html=True
                    )
                with col2:
                    add_tooltip_css()
                    tooltip_html = render_tooltip(
                        f"This section calculates Average revenue per unique order: €{Average_Revenue}")
                    st.markdown(
                        f"<h1 style='display: inline-block;'>Average Revenue {tooltip_html}</h1>",
                        unsafe_allow_html=True
                    )
                    st.markdown(
                        f"""
                                   <div class="card">
                                       <p>Average revenue by customer</p>
                                       <h1>€{Average_Revenue}</h1>

                                   </div>
                                   """,
                        unsafe_allow_html=True
                    )
                with col3:
                    add_tooltip_css()
                    tooltip_html = render_tooltip(
                        f"This section calculates Total Order Refund Amount: €{Total_amaount_refund}")
                    st.markdown(
                        f"<h1 style='display: inline-block;'>Amount Refund {tooltip_html}</h1>", unsafe_allow_html=True
                    )
                    st.markdown(
                        f"""
                                   <div class="card">
                                       <p>Total amount refund</p>
                                       <h1>€{Total_amaount_refund}</h1>

                                   </div>
                                   """,
                        unsafe_allow_html=True
                    )
            else:
                with col1:
                    st.title("Total revenue")
                    st.markdown("""
                        <div style="border: 2px solid black; padding: 20px; background-color: #454545; border-radius: 10px; text-align: center;">
                            <h3 style="font-size: 30px; color: white; font-weight: bold;">No Data Available for Total revenue</h3>
                        </div>
                    """, unsafe_allow_html=True)
                with col2:
                    st.title("Average Revenue")
                    st.markdown("""
                        <div style="border: 2px solid black; padding: 20px; background-color: #454545; border-radius: 10px; text-align: center;">
                            <h3 style="font-size: 30px; color: white; font-weight: bold;">No Data Available for Average Revenue</h3>
                        </div>
                    """, unsafe_allow_html=True)
                with col3:
                    st.title("Amount Refund")
                    st.markdown("""
                        <div style="border: 2px solid black; padding: 20px; background-color: #454545; border-radius: 10px; text-align: center;">
                            <h3 style="font-size: 30px; color: white; font-weight: bold;">No Data Available for Amount Refund</h3>
                        </div>
                    """, unsafe_allow_html=True)
        except:
            st.markdown(
                f"<h3 style='font-size: 30px; color: red; text-align: center;'><b>Datasets Currently is unavaialbe</b></h3>",
                unsafe_allow_html=True)
        if df_orders is not None and not df_orders.empty:
            add_tooltip_css()
            tooltip_html = render_tooltip("Preview of revenue data filtered by the selected date range.")
            st.markdown(f"<h1 style='display: inline-block;'>Preview Filtered Revenue Data {tooltip_html}</h1>",
                        unsafe_allow_html=True
                        )
            filtered_products = filter_by_date(df_orders, 'Order_Created_At')
            st.dataframe(filtered_products)
        else:
            st.title("Preview of revenue data filtered by the selected date range.")
            st.markdown("""
                <div style="border: 2px solid black; padding: 20px; background-color: #454545; border-radius: 10px; text-align: center;">
                    <h3 style="font-size: 30px; color: white; font-weight: bold;">No Data Available for Preview Filtered Revenue Data</h3>
                </div>
            """, unsafe_allow_html=True)

        # Todo-Total revenue placed: weekday vs weekend-----------------------
        col1, col2 = st.columns(2)
        try:
            if df_orders is not None and not df_orders.empty:
                df_unique_orders = df_orders.drop_duplicates(subset='Order_ID', keep='first')
                # Ensure the 'Order_Created_At' column is in datetime format
                df_unique_orders['Order_Created_At'] = pd.to_datetime(df_unique_orders['Order_Created_At'],
                                                                      errors='coerce',
                                                                      utc=True)
                # Add a new column for Weekday/Weekend
                df_unique_orders['Weekday_Weekend'] = df_unique_orders['Order_Created_At'].dt.dayofweek.apply(
                    lambda x: 'Weekend' if x >= 5 else 'Weekday'
                )
                weekday_revenue = df_unique_orders[df_unique_orders['Weekday_Weekend'] == 'Weekday'][
                    'Order_Total_Price'].sum()
                weekend_revenue = df_unique_orders[df_unique_orders['Weekday_Weekend'] == 'Weekend'][
                    'Order_Total_Price'].sum()
                revenues = [weekday_revenue, weekend_revenue]
                labels = ['Weekday', 'Weekend']
                pie_data_revenue = pd.DataFrame({
                    'Category': labels,
                    'Revenue': revenues,
                    'Percentage': [(rev / sum(revenues)) * 100 for rev in revenues]
                })
                pie_data_revenue['Label'] = pie_data_revenue['Percentage'].round(1).astype(str) + '%'
                # Add formatted revenue with € symbol for tooltips
                pie_data_revenue['Total_Revenue'] = '€' + pie_data_revenue['Revenue'].round(2).astype(str)
                # Create the pie chart using Altair
                pie_chart_revenue = alt.Chart(pie_data_revenue).mark_arc().encode(
                    theta=alt.Theta(field="Revenue", type="quantitative"),
                    color=alt.Color(field="Category", type="nominal"),
                    tooltip=["Category", "Total_Revenue:N", "Percentage"]
                )
                # Display the results in Streamlit
                # st.write(f"Weekday Revenue: €{weekday_revenue:.2f} ({(weekday_revenue / sum(revenues)) * 100:.2f}%)")
                # st.write(f"Weekend Revenue: €{weekend_revenue:.2f} ({(weekend_revenue / sum(revenues)) * 100:.2f}%)")
                # st.markdown("<h3 style='text-align: center;'>Revenue by Weekday/Weekend</h3>", unsafe_allow_html=True)
                with col1:
                    add_tooltip_css()
                    tooltip_html = render_tooltip("This pie chart compares the total revenue generated on weekdays and weekends. Hover over the chart to see the category (Weekday or Weekend), the total revenue in euros, and the percentage contribution to the overall revenue.")
                    st.markdown(f"<h1 style='display: inline-block;'>Total Revenue Placed: Weekday vs Weekend {tooltip_html}</h1>",unsafe_allow_html=True
                    )
                    st.altair_chart(pie_chart_revenue, use_container_width=True)
            else:
                st.title("Total Revenue Placed: Weekday vs Weekend")
                st.markdown("""
                       <div style="border: 2px solid black; padding: 20px; background-color: #454545; border-radius: 10px; text-align: center;">
                           <h3 style="font-size: 30px; color: white; font-weight: bold;">No Data Available for Total Revenue Placed: Weekday vs Weekend</h3>
                       </div>
                   """, unsafe_allow_html=True)

            # Todo-Total revenue placed: days of week--------------------------
            if df_orders is not None and not df_orders.empty:
                df_unique_orders = df_orders.drop_duplicates(subset='Order_ID', keep='first')
                df_unique_orders['Order_Created_At'] = pd.to_datetime(df_unique_orders['Order_Created_At'],errors='coerce',utc=True)
                df_unique_orders['days_of_week'] = df_unique_orders['Order_Created_At'].dt.dayofweek.apply(
                    lambda x: ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'][x]
                )
                revenue_per_day = df_unique_orders.groupby('days_of_week')['Order_Total_Price'].sum()
                revenue_per_day = revenue_per_day.reindex(
                    ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'], fill_value=0
                )
                pie_data_revenue = pd.DataFrame({
                    'Day': revenue_per_day.index,
                    'Revenue': revenue_per_day.values
                })
                pie_data_revenue['Total_Revenue'] = '€' + pie_data_revenue['Revenue'].round(2).astype(str)
                pie_chart_revenue = alt.Chart(pie_data_revenue).mark_arc().encode(
                    theta=alt.Theta(field="Revenue", type="quantitative"),
                    color=alt.Color(field="Day", type="nominal",
                                    sort=['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday',
                                          'Sunday']),
                    tooltip=["Day:N", "Total_Revenue:N"]
                )
                with col2:
                    add_tooltip_css()
                    tooltip_html = render_tooltip(
                        "This pie chart visualizes the total revenue generated for each day of the week. Hover over the chart to view the specific day, total revenue in euros, and its contribution to the overall revenue.")
                    st.markdown(
                        f"<h1 style='display: inline-block;'>Total Revenue Placed: Days of the Week {tooltip_html}</h1>",
                        unsafe_allow_html=True)
                    st.altair_chart(pie_chart_revenue, use_container_width=True)
            else:
                st.title("Total Revenue Placed: Days of the Week")
                st.markdown("""
                       <div style="border: 2px solid black; padding: 20px; background-color: #454545; border-radius: 10px; text-align: center;">
                           <h3 style="font-size: 30px; color: white; font-weight: bold;">No Data Available for Total Revenue Placed: Days of the Week</h3>
                       </div>
                   """, unsafe_allow_html=True)
        except:
            with col1:
                st.title("Total Revenue Placed: Weekday vs Weekend")
                st.markdown("""
                       <div style="border: 2px solid black; padding: 20px; background-color: #454545; border-radius: 10px; text-align: center;">
                           <h3 style="font-size: 30px; color: white; font-weight: bold;">No Data Available for Total Revenue Placed: Weekday vs Weekend</h3>
                       </div>
                   """, unsafe_allow_html=True)
            with col2:
                st.title("Total Revenue Placed: Days of the Week")
                st.markdown("""
                       <div style="border: 2px solid black; padding: 20px; background-color: #454545; border-radius: 10px; text-align: center;">
                           <h3 style="font-size: 30px; color: white; font-weight: bold;">No Data Available for Total Revenue Placed: Days of the Week</h3>
                       </div>
                   """, unsafe_allow_html=True)


        # Todo---Total revenue placed: hours of day-------------------------------
        try:
            if df_orders is not None and not df_orders.empty:
                df_unique_orders = df_orders.drop_duplicates(subset='Order_ID', keep='first')
                df_unique_orders['Order_Created_At'] = pd.to_datetime(df_unique_orders['Order_Created_At'],
                                                                      errors='coerce',
                                                                      utc=True)
                df_unique_orders['hour_of_day'] = df_unique_orders[
                                                      'Order_Created_At'].dt.hour + 1  # Shift hours to 1-24 range
                revenue_per_hour = df_unique_orders.groupby('hour_of_day')['Order_Total_Price'].sum()
                revenue_per_hour = revenue_per_hour.reindex(range(1, 25), fill_value=0)
                hour_revenue_data = pd.DataFrame({
                    'Hour of Day': revenue_per_hour.index,
                    'Total Revenue': revenue_per_hour.values
                })
                hour_revenue_data['Overall Revenue'] = '€' + hour_revenue_data['Total Revenue'].round(2).astype(str)
                add_tooltip_css()
                tooltip_html = render_tooltip(
                    "This line chart displays the total revenue placed for each hour of the day. Hover over the chart to see the specific hour and the corresponding revenue in euros. The blue points highlight the total revenue at each hour.")
                st.markdown(
                    f"<h1 style='display: inline-block;'>Total Revenue Placed: Hours of Day {tooltip_html}</h1>",
                    unsafe_allow_html=True)
                st.markdown("<h3 style='text-align: center;'>Revenue by Hour of the Day</h3>", unsafe_allow_html=True)
                line_chart = alt.Chart(hour_revenue_data).mark_line().encode(
                    x=alt.X('Hour of Day:O', title='Hour of Day', scale=alt.Scale(domain=list(range(1, 25)))),
                    y=alt.Y('Total Revenue:Q', title='Total Revenue (€)'),
                    tooltip=['Hour of Day', 'Overall Revenue']
                ).properties(
                    width=700,
                    height=400
                )
                points = alt.Chart(hour_revenue_data).mark_point(size=100, color='blue').encode(
                    x=alt.X('Hour of Day:O', title='Hour of Day'),
                    y=alt.Y('Total Revenue:Q', title='Total Revenue (€)'),
                    tooltip=['Hour of Day', 'Overall Revenue']
                )
                combined_chart = line_chart + points
                st.altair_chart(combined_chart, use_container_width=True)
            else:
                st.title("Total Revenue Placed: Hours of Day")
                st.markdown("""
                       <div style="border: 2px solid black; padding: 20px; background-color: #454545; border-radius: 10px; text-align: center;">
                           <h3 style="font-size: 30px; color: white; font-weight: bold;">No Data Available for Total Revenue Placed: Hours of Day</h3>
                       </div>
                   """, unsafe_allow_html=True)

        except:
            st.markdown(
                f"<h3 style='font-size: 30px; color: red; text-align: center;'><b>Datasets Currently is unavaialbe</b></h3>",
                unsafe_allow_html=True)

        # Todo-Total revenue placed: day, month, quarter, year----------------------------
        try:
            if df_orders is not None and not df_orders.empty:
                df_unique_orders = df_orders.drop_duplicates(subset='Order_ID', keep='first')
                # Ensure the 'Order_Created_At' column is in datetime format
                df_unique_orders['Order_Created_At'] = pd.to_datetime(df_unique_orders['Order_Created_At'],
                                                                      errors='coerce',
                                                                      utc=True)
                # Extract the day, month, quarter, and year
                df_unique_orders['day'] = df_unique_orders['Order_Created_At'].dt.date
                df_unique_orders['month'] = df_unique_orders['Order_Created_At'].dt.to_period('M').astype(
                    str)  # Year-Month format
                df_unique_orders['quarter'] = df_unique_orders['Order_Created_At'].dt.to_period('Q').astype(
                    str)  # Year-Quarter format
                df_unique_orders['year'] = df_unique_orders['Order_Created_At'].dt.year
                # Group by day, month, quarter, and year to calculate total revenue
                revenue_per_day = df_unique_orders.groupby('day')['Order_Total_Price'].sum().reset_index()
                revenue_per_month = df_unique_orders.groupby('month')['Order_Total_Price'].sum().reset_index()
                revenue_per_quarter = df_unique_orders.groupby('quarter')['Order_Total_Price'].sum().reset_index()
                revenue_per_year = df_unique_orders.groupby('year')['Order_Total_Price'].sum().reset_index()
                add_tooltip_css()
                tooltip_html = render_tooltip(
                    "This chart displays the total revenue for different time periods. Select between daily, monthly, quarterly, or yearly views. Hover over the chart to see the specific time period (day, month, quarter, or year) and the corresponding total revenue in euros.")
                st.markdown(
                    f"<h1 style='display: inline-block;'>Revenue Visualizations (Day,Month,Quarter,Year) {tooltip_html}</h1>",
                    unsafe_allow_html=True
                )
                view = st.radio("Select View",
                                ['Revenue per Day', 'Revenue per Month', 'Revenue per Quarter', 'Revenue per Year'])
                if view == 'Revenue per Day':
                    st.title('Revenue Placed: Days')
                    st.markdown("<h3 style='text-align: center;'>Revenue by Day</h3>", unsafe_allow_html=True)
                    # Define the line chart-------------
                    line_chart = alt.Chart(revenue_per_day).mark_line().encode(
                        x=alt.X(
                            'day:T',
                            title='Date',
                            axis=alt.Axis(format="%b %d, %Y", labelAngle=-90, tickMinStep=1)  # Adjust axis labels
                        ),
                        y=alt.Y('Order_Total_Price:Q', title='Total Revenue (€)'),
                        tooltip=['day:T', alt.Tooltip('Order_Total_Price:Q', format=",.2f", title="Total Revenue (€)")]
                    )
                    # Define the points for emphasis--------
                    points = alt.Chart(revenue_per_day).mark_point(size=60, color='blue').encode(
                        x='day:T',
                        y='Order_Total_Price:Q',
                        tooltip=['day:T', alt.Tooltip('Order_Total_Price:Q', format=",.2f", title="Total Revenue (€)")]
                    )
                    # Combine the line chart and points
                    combined_chart = line_chart + points
                    # Set chart properties
                    combined_chart = combined_chart.properties(
                        width=700,  # Adjust width for better readability
                        height=400  # Adjust height
                    )
                    # Display the chart in Streamlit
                    st.altair_chart(combined_chart, use_container_width=True)
                elif view == 'Revenue per Month':
                    st.title('Revenue Placed: Months')
                    st.markdown("<h3 style='text-align: center;'>Revenue by Month</h3>", unsafe_allow_html=True)
                    revenue_per_month['month'] = pd.to_datetime(revenue_per_month['month'], format='%Y-%m')
                    revenue_per_month['month'] = revenue_per_month['month'].dt.strftime(
                        '%b %Y')  # Convert to Month Year format
                    line_chart = alt.Chart(revenue_per_month).mark_line().encode(
                        x=alt.X('month:O', title='Month', axis=alt.Axis(labelAngle=-45)),
                        y=alt.Y('Order_Total_Price:Q', title='Total Revenue (€)'),
                        tooltip=['month:N',
                                 alt.Tooltip('Order_Total_Price:Q', format=",.2f", title="Total Revenue (€)")]
                    )
                    points = alt.Chart(revenue_per_month).mark_point(size=60, color='blue').encode(
                        x='month:O',
                        y='Order_Total_Price:Q',
                        tooltip=['month:N',
                                 alt.Tooltip('Order_Total_Price:Q', format=",.2f", title="Total Revenue (€)")]
                    )
                    combined_chart = line_chart + points
                    st.altair_chart(combined_chart, use_container_width=True)

                elif view == 'Revenue per Quarter':
                    st.title('Revenue Placed: Quarters')
                    st.markdown("<h3 style='text-align: center;'>Revenue by Quarter</h3>", unsafe_allow_html=True)
                    line_chart = alt.Chart(revenue_per_quarter).mark_line().encode(
                        x=alt.X('quarter:N', title='Quarter'),
                        y=alt.Y('Order_Total_Price:Q', title='Total Revenue (€)'),
                        tooltip=['quarter:N',
                                 alt.Tooltip('Order_Total_Price:Q', format=",.2f", title="Total Revenue (€)")]
                    )
                    points = alt.Chart(revenue_per_quarter).mark_point(size=60, color='blue').encode(
                        x='quarter:N',
                        y='Order_Total_Price:Q',
                        tooltip=['quarter:N',
                                 alt.Tooltip('Order_Total_Price:Q', format=",.2f", title="Total Revenue (€)")]
                    )
                    combined_chart = line_chart + points
                    st.altair_chart(combined_chart, use_container_width=True)

                elif view == 'Revenue per Year':
                    st.title('Revenue Placed: Years')
                    st.markdown("<h3 style='text-align: center;'>Revenue by Year</h3>", unsafe_allow_html=True)
                    line_chart = alt.Chart(revenue_per_year).mark_line().encode(
                        x=alt.X('year:O', title='Year'),
                        y=alt.Y('Order_Total_Price:Q', title='Total Revenue (€)'),
                        tooltip=['year:O', alt.Tooltip('Order_Total_Price:Q', format=",.2f", title="Total Revenue (€)")]
                    )
                    points = alt.Chart(revenue_per_year).mark_point(size=60, color='blue').encode(
                        x='year:O',
                        y='Order_Total_Price:Q',
                        tooltip=['year:O', alt.Tooltip('Order_Total_Price:Q', format=",.2f", title="Total Revenue (€)")]
                    )
                    combined_chart = line_chart + points
                    st.altair_chart(combined_chart, use_container_width=True)
            else:
                st.title("Revenue Visualizations (Day,Month,Quarter,Year)")
                st.markdown("""
                       <div style="border: 2px solid black; padding: 20px; background-color: #454545; border-radius: 10px; text-align: center;">
                           <h3 style="font-size: 30px; color: white; font-weight: bold;">No Data Available for Revenue Visualizations (Day,Month,Quarter,Year)</h3>
                       </div>
                   """, unsafe_allow_html=True)
        except:
            st.markdown(
                f"<h3 style='font-size: 30px; color: red; text-align: center;'><b>Datasets Currently is unavaialbe</b></h3>",
                unsafe_allow_html=True)

        # Todo-Order Refering site chart
        try:
            if df_orders is not None and not df_orders.empty:
                df_unique_orders = df_orders.drop_duplicates(subset="Order_ID", keep="first")
                total_revenue_by_site = df_unique_orders.groupby("Order_Referring_Site")["Order_Total_Price"].sum().reset_index()
                total_revenue_by_site.columns = ["Referring Site", "Total Revenue"]
                if not total_revenue_by_site.empty:
                    add_tooltip_css()
                    tooltip_html = render_tooltip(
                        "This chart displays the total revenue generated by different referring sites. You can use the slider to select the top N referring sites to visualize. Hover over each bar to see the referring site and its corresponding total revenue in euros.")
                    st.markdown(
                        f"<h1 style='display: inline-block;'>Total Revenue by Referring Sites {tooltip_html}</h1>",
                        unsafe_allow_html=True
                    )
                    st.markdown("### Visualizing the total revenue generated by different referring sites")
                    top_n = st.slider("Select Top N Referring Sites to Display", min_value=1,
                                      max_value=len(total_revenue_by_site), value=5)
                    top_revenue_sites = total_revenue_by_site.nlargest(top_n, "Total Revenue")
                    # Step 4: Create Altair Chart
                    chart = alt.Chart(top_revenue_sites).mark_bar().encode(
                        x=alt.X("Referring Site:O", title="Referring Site", sort="-y"),
                        y=alt.Y("Total Revenue:Q", title="Total Revenue (€)"),
                        color=alt.Color("Total Revenue:Q", legend=None),
                        tooltip=["Referring Site:N",
                                 alt.Tooltip("Total Revenue:Q", format=",.2f", title="Total Revenue (€)")],
                    ).properties(
                        width=700,
                        height=400,
                        title="Top N Referring Sites by Total Revenue"
                    )
                    # Adding text labels on the bars
                    text = chart.mark_text(
                        align="center",
                        baseline="middle",
                        dy=-10  # Adjust text position
                    ).encode(
                        text=alt.Text("Total Revenue:Q", format=",.2f")
                    )
                    # Combine chart and text
                    final_chart = chart + text
                    final_chart = final_chart.configure_axis(
                        labelAngle=0,
                        labelFontSize=12,
                        titleFontSize=14
                    )
                    st.altair_chart(final_chart, use_container_width=True)
                else:
                    st.title("Order Refering site chart")
                    st.markdown("""
                           <div style="border: 2px solid black; padding: 20px; background-color: #454545; border-radius: 10px; text-align: center;">
                               <h3 style="font-size: 30px; color: white; font-weight: bold;">No Data Available for Total Revenue by Referring Sites</h3>
                           </div>
                       """, unsafe_allow_html=True)

            else:
                st.title("Order Refering site chart")
                st.markdown("""
                       <div style="border: 2px solid black; padding: 20px; background-color: #454545; border-radius: 10px; text-align: center;">
                           <h3 style="font-size: 30px; color: white; font-weight: bold;">No Data Available for Total Revenue by Referring Sites</h3>
                       </div>
                   """, unsafe_allow_html=True)

        except:
            st.markdown(
                f"<h3 style='font-size: 30px; color: red; text-align: center;'><b>Datasets Currently is unavaialbe</b></h3>",
                unsafe_allow_html=True)
    except:
        st.markdown(
            f"<h3 style='font-size: 30px; color: red; text-align: center;'><b>Datasets Currently is unavaialbe</b></h3>",
            unsafe_allow_html=True)

page = st.sidebar.selectbox("Select a Page",['Customer Journey', 'Customer Data', 'Order Data', 'Abandoned Checkouts', 'Products','Revenue'])

if page == 'Customer Journey':
    show_cj_page()

elif page == 'Customer Data':
    show_customer_data_page()

elif page == 'Order Data':
    show_order_data_page()

elif page == 'Abandoned Checkouts':
    show_abandoned_checkouts_page()

elif page == 'Products':
    show_products_page()

elif page == 'Revenue':
    show_revenue_page()
