import streamlit as st
import pandas as pd
from datetime import timedelta, time
import pymysql

def get_db_connection():
        return pymysql.connect(
            host="localhost",
            user="root",
            password="root",
            database="SalesInsightsDB"
        )


intro = st.sidebar.radio('Main Menu',["Home page","Check In for Retail Order's Business Analysis"])
if  'Home page'==intro:
    # Project Introduction
    st.title("Retail Order Data Analysis")
    st.subheader("A Deep Dive into Sales Data Using Kaggle API, SQL, and Streamlit")

    st.write("""
        In this project, we aim to analyze and optimize sales performance by identifying key trends, top-performing products, 
        and growth opportunities using a dataset of sales transactions.
        Our goal is to gain insights that will guide decision-making and drive business growth.
    """)

    st.write("""
        **Tools & Technologies Used:**
        - **Kaggle API**: For fetching retail order data directly from Kaggle.
        - **Python**: Utilized for data extraction, cleaning, and analysis.
        - **SQL**: Used for storing and querying the cleaned dataset.
        - **Streamlit**: For building a real-time, interactive dashboard to display insights.
    """)

    # Project Objectives
    st.subheader("Project Objectives")
    st.write("""
        1. **Identify Top-Performing Products**: Analyze the products and categories that contribute the most to revenue and profit.
        2. **Sales Trend Analysis**: Perform Year-over-Year (YoY) and Month-over-Month (MoM) comparisons to identify sales trends.
        3. **Profit Margin Analysis**: Highlight the subcategories with the highest profit margins to guide future sales strategies.
        4. **Regional Analysis**: Explore sales data by region to identify high-performing areas.
        5. **Discount Impact**: Analyze the effects of discounts on product sales, especially those with more than 20 percentage off.
    """)

    st.write("""
        This project integrates multiple components:
        - **Data Extraction**: Using Kaggle API to fetch raw sales data.
        - **Data Cleaning**: Applying transformations to standardize and prepare the data for analysis.
        - **SQL Database Integration**: Moving the cleaned data into SQL for efficient querying and analysis.
        - **Business Insights**: Extracting valuable insights using SQL queries to answer key business questions.
        - **Interactive Dashboard**: Building a real-time dashboard in Streamlit to visualize the findings.
    """)

    # Tools Overview
    st.write("""
        **Streamlit Features for This Project:**
        
        1. **Real-Time Data Queries**: Query your SQL database and display live results directly in the Streamlit interface.
        2. **Dynamic Data Filtering**: Allow users to filter and analyze the dataset based on various criteria such as product category, region, and sales trends.
        3. **Visualization**: Use charts and tables to present insights visually for better interpretation and decision-making.
    """)

    # Brief Explanation of the Flow
    st.subheader("Project Flow")
    st.write("""
        1. **Data Extraction**: Fetch raw data from Kaggle API.
        2. **Data Cleaning**: Clean the dataset to handle missing values, standardize column names, and derive new columns (e.g., discount, sale price).
        3. **SQL Integration**: Load cleaned data into a SQL database for efficient querying.
        4. **SQL Queries**: Run advanced queries to generate business insights and trends.
        5. **Streamlit Dashboard**: Build an interactive dashboard to display the results dynamically.
    """)

    # Conclusion
    st.write("""
        This project aims to provide meaningful insights into the retail order data, helping businesses make informed decisions about 
        their products, sales strategies, and market opportunities. By combining data extraction, cleaning, SQL analysis, and Streamlit 
        visualization, we offer a comprehensive approach to analyzing retail sales performance.
    """)

    # Call to Action
    st.write("""
        🚀 **Ready to explore the data?** Use the interactive tools below to filter and analyze the data in real-time!
    """)

else:
    # Business Analysis Section
    st.title("Retail Order's Business Analysis")
    st.subheader("Dive deeper into the business analysis of retail orders.")

    def Top_10_Highest_Revenue_Products():
        myconnection = get_db_connection()
        mycursor=myconnection.cursor(pymysql.cursors.SSCursor)
        query="""SELECT products.product_id, SUM(products.sale_price * products.quantity) AS total_revenue
        FROM products GROUP BY products.product_id ORDER BY total_revenue DESC LIMIT 10"""
        mycursor.execute(query)
        results = mycursor.fetchall()
        mycursor.close()
        myconnection.close()
        df = pd.DataFrame(results, columns=["Product ID", "Total Revenue"])
        return df

    def Top_5_Cities_Highest_Profit():
        myconnection = get_db_connection()
        mycursor=myconnection.cursor(pymysql.cursors.SSCursor)
        query="""SELECT orders.city, SUM(products.profit) AS total_profit FROM orders
        JOIN products ON orders.order_id = products.order_id GROUP BY orders.city ORDER BY 
        total_profit DESC LIMIT 5"""
        mycursor.execute(query)
        results = mycursor.fetchall()
        mycursor.close()
        myconnection.close()
        df = pd.DataFrame(results, columns=["City", "Total Profit"])
        return df

    def Total_Discount_given_foreach_category():
        myconnection = get_db_connection()
        mycursor=myconnection.cursor(pymysql.cursors.SSCursor)
        query="""SELECT orders.category, SUM(products.discount) AS total_discount FROM 
        orders JOIN products ON orders.order_id = products.order_id GROUP BY orders.category"""
        mycursor.execute(query)
        results = mycursor.fetchall()
        mycursor.close()
        myconnection.close()
        df = pd.DataFrame(results, columns=["Category", "Total Discount"])
        return df

    def Average_sale_price_per_product():
        myconnection = get_db_connection()
        mycursor=myconnection.cursor(pymysql.cursors.SSCursor)
        query="""SELECT orders.category, AVG(products.sale_price) AS avg_sale_price FROM orders JOIN 
        products ON orders.order_id = products.order_id GROUP BY orders.category"""
        mycursor.execute(query)
        results = mycursor.fetchall()
        mycursor.close()
        myconnection.close()
        df = pd.DataFrame(results, columns=["Category", "Average Sale Price"])
        return df

    def Region_with_Highest_Average_Sale_Price():
        myconnection = get_db_connection()
        mycursor=myconnection.cursor(pymysql.cursors.SSCursor)
        query="""SELECT orders.region, AVG(products.sale_price) AS avg_sale_price FROM orders
        JOIN products ON orders.order_id = products.order_id GROUP BY orders.region 
        ORDER BY avg_sale_price DESC LIMIT 1"""
        mycursor.execute(query)
        results = mycursor.fetchall()
        mycursor.close()
        myconnection.close()
        df = pd.DataFrame(results, columns=["Region", "Average Sale Price"])
        return df

    def Total_Profit_Per_Category():
        myconnection = get_db_connection()
        mycursor=myconnection.cursor(pymysql.cursors.SSCursor)
        query="""SELECT orders.category, SUM(products.profit) AS total_profit FROM 
        orders JOIN products ON orders.order_id = products.order_id GROUP BY orders.category"""
        mycursor.execute(query)
        results = mycursor.fetchall()
        mycursor.close()
        myconnection.close()
        df = pd.DataFrame(results, columns=["Category", "Total Profit"])
        return df

    def Top_3_Segments_with_Highest_Quantityof_Orders():
        myconnection = get_db_connection()
        mycursor=myconnection.cursor(pymysql.cursors.SSCursor)
        query="""SELECT orders.segment, SUM(products.quantity) AS total_quantity FROM 
        orders JOIN products ON orders.order_id = products.order_id GROUP BY 
        orders.segment ORDER BY total_quantity DESC LIMIT 3"""
        mycursor.execute(query)
        results = mycursor.fetchall()
        mycursor.close()
        myconnection.close()
        df = pd.DataFrame(results, columns=["Segment", "Total Quantity"])
        return df

    def Average_Discount_Percentage_Given_per_Region():
        myconnection = get_db_connection()
        mycursor=myconnection.cursor(pymysql.cursors.SSCursor)
        query="""SELECT orders.region, AVG(products.discount_percent) AS avg_discount_percentage 
        FROM orders JOIN products ON orders.order_id = products.order_id GROUP BY orders.region;"""
        mycursor.execute(query)
        results = mycursor.fetchall()
        mycursor.close()
        myconnection.close()
        df = pd.DataFrame(results, columns=["Region", "Average Discount Percentage"])
        return df

    def Product_Category_with_Highest_Total_Profit():
        myconnection = get_db_connection()
        mycursor=myconnection.cursor(pymysql.cursors.SSCursor)
        query="""SELECT orders.category, SUM(products.profit) AS total_profit FROM 
        orders JOIN products ON orders.order_id = products.order_id GROUP BY orders.category ORDER BY 
        total_profit DESC LIMIT 1"""
        mycursor.execute(query)
        results = mycursor.fetchall()
        mycursor.close()
        myconnection.close()
        df = pd.DataFrame(results, columns=["Category", "Total Profit"])
        return df

    def Total_Revenue_Generated_Per_Year():
        myconnection = get_db_connection()
        mycursor=myconnection.cursor(pymysql.cursors.SSCursor)
        query="""SELECT YEAR(orders.order_date) AS year, SUM(products.sale_price * products.quantity) AS total_revenue 
        FROM orders JOIN products ON orders.order_id = products.order_id GROUP BY year ORDER BY year"""
        mycursor.execute(query)
        results = mycursor.fetchall()
        mycursor.close()
        myconnection.close()
        df = pd.DataFrame(results, columns=["Year", "Total Revenue"])
        return df

    def Top10_Subcategories_Revenue():
        myconnection = get_db_connection()
        mycursor=myconnection.cursor(pymysql.cursors.SSCursor)
        query="""SELECT sub_category, SUM(sale_price * quantity) AS total_revenue FROM products
        JOIN orders ON products.order_id = orders.order_id GROUP BY sub_category 
        ORDER BY total_revenue DESC LIMIT 10"""
        mycursor.execute(query)
        results = mycursor.fetchall()
        mycursor.close()
        myconnection.close()
        df = pd.DataFrame(results, columns=["Sub Category", "Total Revenue"])
        return df

    def Monthly_Revenue_Trends():
        myconnection = get_db_connection()
        mycursor=myconnection.cursor(pymysql.cursors.SSCursor)
        query="""SELECT DATE_FORMAT(order_date, '%Y-%m') AS month, SUM(sale_price * quantity) AS monthly_revenue
        FROM products JOIN orders ON products.order_id = orders.order_id GROUP BY month ORDER BY month"""
        mycursor.execute(query)
        results = mycursor.fetchall()
        mycursor.close()
        myconnection.close()
        df = pd.DataFrame(results, columns=["Month", "Monthly Revenue"])
        return df

    def Top_5_States_with_Highest_Discounts():
        myconnection = get_db_connection()
        mycursor=myconnection.cursor(pymysql.cursors.SSCursor)
        query="""SELECT state, SUM(discount) AS total_discount FROM products 
        JOIN orders ON products.order_id = orders.order_id GROUP BY state ORDER BY total_discount DESC LIMIT 5"""
        mycursor.execute(query)
        results = mycursor.fetchall()
        mycursor.close()
        myconnection.close()
        df = pd.DataFrame(results, columns=["State", "Total Discount"])
        return df

    def Yearly_Profit_Growth():
        myconnection = get_db_connection()
        mycursor=myconnection.cursor(pymysql.cursors.SSCursor)
        query="""SELECT YEAR(order_date) AS year, SUM(profit) AS yearly_profit FROM products 
        JOIN orders ON products.order_id = orders.order_id GROUP BY year ORDER BY year"""
        mycursor.execute(query)
        results = mycursor.fetchall()
        mycursor.close()
        myconnection.close()
        df = pd.DataFrame(results, columns=["Year", "Yearly Profit"])
        return df

    def City_with_Maximum_Orders():
        myconnection = get_db_connection()
        mycursor=myconnection.cursor(pymysql.cursors.SSCursor)
        query="""SELECT city, COUNT(DISTINCT order_id) AS total_orders FROM orders
        GROUP BY city ORDER BY total_orders DESC LIMIT 1"""
        mycursor.execute(query)
        results = mycursor.fetchall()
        mycursor.close()
        myconnection.close()
        df = pd.DataFrame(results, columns=["City", "Total Orders"])
        return df

    def Region_Wise_Quantity_Analysis():
        myconnection = get_db_connection()
        mycursor=myconnection.cursor(pymysql.cursors.SSCursor)
        query="""SELECT region, SUM(quantity) AS total_quantity FROM products 
        JOIN orders ON products.order_id = orders.order_id GROUP BY region ORDER BY total_quantity DESC"""
        mycursor.execute(query)
        results = mycursor.fetchall()
        mycursor.close()
        myconnection.close()
        df = pd.DataFrame(results, columns=["Region", "Total Quantity"])
        return df
    
    def Low_Performing_Products():
        myconnection = get_db_connection()
        mycursor=myconnection.cursor(pymysql.cursors.SSCursor)
        query="""SELECT product_id, (SUM(profit) / SUM(sale_price * quantity)) * 100 AS profit_margin
        FROM products GROUP BY product_id HAVING profit_margin < 10 ORDER BY profit_margin"""
        mycursor.execute(query)
        results = mycursor.fetchall()
        mycursor.close()
        myconnection.close()
        df = pd.DataFrame(results, columns=["Product Id", "Profit Margin"])
        return df

    def Total_Revenue_from_Discounted_Products():
        myconnection = get_db_connection()
        mycursor=myconnection.cursor(pymysql.cursors.SSCursor)
        query="""SELECT SUM(sale_price * quantity) AS discounted_revenue FROM products 
        WHERE discount_percent > 0"""
        mycursor.execute(query)
        results = mycursor.fetchall()
        mycursor.close()
        myconnection.close()
        df = pd.DataFrame(results, columns=["Discounted Revenue"])
        return df

    def Product_Categories_with_Low_Sales():
        myconnection = get_db_connection()
        mycursor=myconnection.cursor(pymysql.cursors.SSCursor)
        query="""SELECT category, SUM(sale_price * quantity) AS total_revenue FROM products 
        JOIN orders ON products.order_id = orders.order_id GROUP BY category
        HAVING total_revenue < (SELECT AVG(total_revenue) 
                        FROM (
                            SELECT SUM(sale_price * quantity) AS total_revenue
                            FROM products
                            JOIN orders ON products.order_id = orders.order_id
                            GROUP BY category) AS category_revenue)"""
        mycursor.execute(query)
        results = mycursor.fetchall()
        mycursor.close()
        myconnection.close()
        df = pd.DataFrame(results, columns=["Category", "Total Revenue"])
        return df

    def Impact_of_Discounts_on_Profit():
        myconnection = get_db_connection()
        mycursor=myconnection.cursor(pymysql.cursors.SSCursor)
        query="""SELECT CASE WHEN discount_percent > 20 THEN 'High Discount' 
        WHEN discount_percent BETWEEN 10 AND 20 THEN 'Medium Discount' 
        ELSE 'Low Discount' END AS discount_category, SUM(profit) AS total_profit,
        (SUM(profit) / SUM(sale_price * quantity)) * 100 AS profit_margin 
        FROM products GROUP BY discount_category"""
        mycursor.execute(query)
        results = mycursor.fetchall()
        mycursor.close()
        myconnection.close()
        df = pd.DataFrame(results, columns=["Discount Category", "Total Profit", "Profit Margin"])
        return df

    col1, col2= st.columns([3,5])

    with col1:
        st.markdown(' #### Unlock Insights: Explore Top Products, Categories, and Regional Trends')
    with col2:
        selected_question = st.selectbox("Select a Question to View Analysis", [
            "Find top 10 highest revenue generating products",
            "Find the top 5 cities with the highest profit margins",
            "Calculate the total discount given for each category",
            "Find the average sale price per product category",
            "Find the region with the highest average sale price",
            "Find the total profit per category",
            "Identify the top 3 segments with the highest quantity of orders",
            "Determine the average discount percentage given per region",
            "Find the product category with the highest total profit",
            "Calculate the total revenue generated per year"
        ])

        if selected_question == "Find top 10 highest revenue generating products":
            st.write(Top_10_Highest_Revenue_Products())
        elif selected_question == "Find the top 5 cities with the highest profit margins":
            st.write(Top_5_Cities_Highest_Profit())
        elif selected_question == "Calculate the total discount given for each category":
            st.write(Total_Discount_given_foreach_category())
        elif selected_question == "Find the average sale price per product category":
            st.write(Average_sale_price_per_product())
        elif selected_question == "Find the region with the highest average sale price":
            st.write(Region_with_Highest_Average_Sale_Price())
        elif selected_question == "Find the total profit per category":
            st.write(Total_Profit_Per_Category())
        elif selected_question == "Identify the top 3 segments with the highest quantity of orders":
            st.write(Top_3_Segments_with_Highest_Quantityof_Orders())
        elif selected_question == "Determine the average discount percentage given per region":
            st.write(Average_Discount_Percentage_Given_per_Region())
        elif selected_question == "Find the product category with the highest total profit":
            st.write(Product_Category_with_Highest_Total_Profit())
        elif selected_question == "Calculate the total revenue generated per year":
            st.write(Total_Revenue_Generated_Per_Year())

    colA, colB= st.columns([3,5])
        
    with colA:
        st.markdown(' #### Advanced Analytics: Uncover Key Insights on Discounts, Sales, and Product Performance')
    with colB:
        own_selected_question = st.selectbox("Select a Question to View Analysis", [
            "Find top 10 Subcategories by Revenue",
            "Find the Monthly Revenue Trends",
            "Identify the Top 5 States with Highest Discounts",
            "Calculate the Yearly Profit Growth",
            "Find the City with Maximum Orders",
            "Calculate the Region-Wise Quantity Analysis",
            "Identify the Low Performing Products",
            "Find the Total Revenue from Discounted Products",
            "Identify the Product Categories with Low Sales",
            "Calculate the Impact of Discounts on Profit"
        ])

        if own_selected_question == "Find top 10 Subcategories by Revenue":
            st.write(Top10_Subcategories_Revenue())
        elif own_selected_question == "Find the Monthly Revenue Trends":
            st.write(Monthly_Revenue_Trends())
        elif own_selected_question == "Identify the Top 5 States with Highest Discounts":
            st.write(Top_5_States_with_Highest_Discounts())
        elif own_selected_question == "Calculate the Yearly Profit Growth":
            st.write(Yearly_Profit_Growth())
        elif own_selected_question == "Find the City with Maximum Orders":
            st.write(City_with_Maximum_Orders())
        elif own_selected_question == "Calculate the Region-Wise Quantity Analysis":
            st.write(Region_Wise_Quantity_Analysis())
        elif own_selected_question == "Identify the Low Performing Products":
            st.write(Low_Performing_Products())
        elif own_selected_question == "Find the Total Revenue from Discounted Products":
            st.write(Total_Revenue_from_Discounted_Products())
        elif own_selected_question == "Identify the Product Categories with Low Sales":
            st.write(Product_Categories_with_Low_Sales())
        elif own_selected_question == "Calculate the Impact of Discounts on Profit":
            st.write(Impact_of_Discounts_on_Profit())
