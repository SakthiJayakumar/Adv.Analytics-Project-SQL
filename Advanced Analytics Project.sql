---Advanced Analytics Project---

--1.Changes Over Time Analysis

SELECT YEAR(order_date) AS Order_Year,MONTH(order_date) AS Order_Month,
SUM(sales_amount) AS Total_Sales,
COUNT(DISTINCT customer_key) AS total_customer,
SUM(quantity) AS Total_Quantity
FROM gold.fact_sales
WHERE  order_date IS NOT NULL
GROUP BY YEAR(order_date),MONTH(order_date)
ORDER BY YEAR(order_date),MONTH(order_date);

--Changes Over Time Analysis with DATETRUNC

SELECT DATETRUNC(month,order_date) AS Order_Month,
SUM(sales_amount) AS Total_Sales,
COUNT(DISTINCT customer_key) AS total_customer,
SUM(quantity) AS Total_Quantity
FROM gold.fact_sales
WHERE  order_date IS NOT NULL
GROUP BY DATETRUNC(month,order_date)
ORDER BY DATETRUNC(month,order_date),SUM(sales_amount);

--Changes Over Time Analysis with FORMAT


SELECT FORMAT(order_date,'yyyy-MMM')AS Order_Month,
 SUM(sales_amount) AS Total_Sales,
 COUNT(DISTINCT customer_key) AS total_customer,
 SUM(quantity) AS Total_Quantity
FROM gold.fact_sales
WHERE  order_date IS NOT NULL
GROUP BY FORMAT(order_date,'yyyy-MMM')
ORDER BY FORMAT(order_date,'yyyy-MMM'),SUM(sales_amount);

---2.Cumulative Analysis

---Calculate the total sales per month
---and running total of sales over time

SELECT * FROM gold.fact_sales;

SELECT Order_Month, Total_Sales,
       SUM(Total_Sales) OVER(ORDER BY Order_Month ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS Running_Total_Sale,
       Average_Price,
       AVG(Average_Price) OVER(ORDER BY Order_Month) AS Moving_Average
FROM (
    SELECT DATETRUNC(month,order_date) AS Order_Month, SUM(sales_amount) AS Total_Sales,
    AVG(price) AS Average_Price
    FROM gold.fact_sales
    WHERE order_date IS NOT NULL
    GROUP BY DATETRUNC(month,order_date)
) sale_by_order_month

--3.Perfomance Analysis

WITH YEARLY_PRODUCT_SALE AS (
SELECT YEAR(f.order_date) AS Order_Year,p.product_name,SUM(f.sales_amount) AS Total_Sales
FROM gold.fact_sales f
JOIN gold.dim_products p ON f.product_key=p.product_key
WHERE order_date IS NOT NULL
GROUP BY YEAR(f.order_date),p.product_name)

SELECT order_year,product_name,Total_Sales,
AVG(Total_Sales) OVER(PARTITION BY product_name) AS AVG_SALE,
Total_Sales - AVG(Total_Sales) OVER(PARTITION BY product_name) AS Diff_in_Avg,
CASE WHEN Total_Sales - AVG(Total_Sales) OVER(PARTITION BY product_name) <0 THEN 'Above Avg'
     WHEN Total_Sales - AVG(Total_Sales) OVER(PARTITION BY product_name) >0 THEN 'Below Avg'
     ELSE 'Avg'
END 'Avg Change',
LAG(Total_Sales) OVER(PARTITION BY product_name ORDER BY order_year) py_sales,
Total_Sales-LAG(Total_Sales) OVER(PARTITION BY product_name ORDER BY order_year) AS diff_py,
CASE WHEN Total_Sales-LAG(Total_Sales) OVER(PARTITION BY product_name ORDER BY order_year) <0 THEN 'Decrease'
     WHEN Total_Sales-LAG(Total_Sales) OVER(PARTITION BY product_name ORDER BY order_year) >0 THEN 'Increase'
     ELSE 'No Change'
     END py_change

FROM YEARLY_PRODUCT_SALE
ORDER BY product_name,Order_Year;

--4.Part To Whole Analysis
--Which Category contributes more to Total Sales

WITH Category_Sales AS(

SELECT p.category,SUM(f.sales_amount) AS Total_Sales
FROM gold.fact_sales f
JOIN gold.dim_products p ON f.product_key=p.product_key
GROUP BY p.category
)

SELECT category, Total_Sales,
SUM(Total_Sales) OVER() AS Overall_Sales,
CONCAT(ROUND((CAST(Total_Sales AS float)/SUM(Total_Sales) OVER())*100,3),'%') AS Per_Total
FROM Category_Sales
ORDER BY Total_Sales DESC;

---5.Data Segmentation
--Segment products into cost ranges and count how many products fall into each segment

WITH product_segements AS(
SELECT
        product_key,
        product_name,
        cost,
        CASE 
            WHEN cost < 100 THEN 'Below 100'
            WHEN cost BETWEEN 100 AND 500 THEN '100-500'
            WHEN cost BETWEEN 500 AND 1000 THEN '500-1000'
            ELSE 'Above 1000'
        END AS cost_range
    FROM gold.dim_products)

SELECT cost_range,COUNT(product_key) AS total_products
FROM product_segements
GROUP BY cost_range
ORDER BY total_products DESC;

/*Group customers into three segments based on their spending behavior:
	- VIP: Customers with at least 12 months of history and spending more than �5,000.
	- Regular: Customers with at least 12 months of history but spending �5,000 or less.
	- New: Customers with a lifespan less than 12 months.
And find the total number of customers by each group
*/

WITH customer_spending AS (
    SELECT
        c.customer_key,
        SUM(f.sales_amount) AS total_spending,
        MIN(order_date) AS first_order,
        MAX(order_date) AS last_order,
        DATEDIFF(month, MIN(order_date), MAX(order_date)) AS lifespan
    FROM gold.fact_sales f
    LEFT JOIN gold.dim_customers c
        ON f.customer_key = c.customer_key
    GROUP BY c.customer_key
)
SELECT 
    customer_segment,
    COUNT(customer_key) AS total_customers
FROM (
    SELECT 
        customer_key,
        CASE 
            WHEN lifespan >= 12 AND total_spending > 5000 THEN 'VIP'
            WHEN lifespan >= 12 AND total_spending <= 5000 THEN 'Regular'
            ELSE 'New'
        END AS customer_segment
    FROM customer_spending
) AS segmented_customers
GROUP BY customer_segment
ORDER BY total_customers DESC;

*
===============================================================================
Customer Report
===============================================================================
Purpose:
    - This report consolidates key customer metrics and behaviors

Highlights:
    1. Gathers essential fields such as names, ages, and transaction details.
	2. Segments customers into categories (VIP, Regular, New) and age groups.
    3. Aggregates customer-level metrics:
	   - total orders
	   - total sales
	   - total quantity purchased
	   - total products
	   - lifespan (in months)
    4. Calculates valuable KPIs:
	    - recency (months since last order)
		- average order value
		- average monthly spend
===============================================================================
*/

-- =============================================================================
-- Create Report: gold.report_customers
-- =============================================================================
IF OBJECT_ID('gold.report_customers', 'V') IS NOT NULL
    DROP VIEW gold.report_customers;
GO

CREATE VIEW gold.report_customers AS

WITH base_query AS(
/*---------------------------------------------------------------------------
1) Base Query: Retrieves core columns from tables
---------------------------------------------------------------------------*/
SELECT
f.order_number,
f.product_key,
f.order_date,
f.sales_amount,
f.quantity,
c.customer_key,
c.customer_number,
CONCAT(c.first_name, ' ', c.last_name) AS customer_name,
DATEDIFF(year, c.birthdate, GETDATE()) age
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
ON c.customer_key = f.customer_key
WHERE order_date IS NOT NULL)

, customer_aggregation AS (
/*---------------------------------------------------------------------------
2) Customer Aggregations: Summarizes key metrics at the customer level
---------------------------------------------------------------------------*/
SELECT 
	customer_key,
	customer_number,
	customer_name,
	age,
	COUNT(DISTINCT order_number) AS total_orders,
	SUM(sales_amount) AS total_sales,
	SUM(quantity) AS total_quantity,
	COUNT(DISTINCT product_key) AS total_products,
	MAX(order_date) AS last_order_date,
	DATEDIFF(month, MIN(order_date), MAX(order_date)) AS lifespan
FROM base_query
GROUP BY 
	customer_key,
	customer_number,
	customer_name,
	age
)
SELECT
customer_key,
customer_number,
customer_name,
age,
CASE 
	 WHEN age < 20 THEN 'Under 20'
	 WHEN age between 20 and 29 THEN '20-29'
	 WHEN age between 30 and 39 THEN '30-39'
	 WHEN age between 40 and 49 THEN '40-49'
	 ELSE '50 and above'
END AS age_group,
CASE 
    WHEN lifespan >= 12 AND total_sales > 5000 THEN 'VIP'
    WHEN lifespan >= 12 AND total_sales <= 5000 THEN 'Regular'
    ELSE 'New'
END AS customer_segment,
last_order_date,
DATEDIFF(month, last_order_date, GETDATE()) AS recency,
total_orders,
total_sales,
total_quantity,
total_products
lifespan,
-- Compuate average order value (AVO)
CASE WHEN total_sales = 0 THEN 0
	 ELSE total_sales / total_orders
END AS avg_order_value,
-- Compuate average monthly spend
CASE WHEN lifespan = 0 THEN total_sales
     ELSE total_sales / lifespan
END AS avg_monthly_spend
FROM customer_aggregation