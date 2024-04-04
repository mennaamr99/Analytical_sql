--total_sales of each month

WITH cte_year_sales AS (
    SELECT 
        TO_CHAR(TO_DATE(INVOICEDATE , 'MM/DD/YYYY HH24:MI'), 'yyyy') AS year,
        TO_CHAR(TO_DATE(INVOICEDATE , 'MM/DD/YYYY HH24:MI'), 'month') AS month,
        SUM(quantity * price) AS sales
    FROM tableretail
    GROUP BY TO_CHAR(TO_DATE(INVOICEDATE , 'MM/DD/YYYY HH24:MI'), 'yyyy'),
             TO_CHAR(TO_DATE(INVOICEDATE , 'MM/DD/YYYY HH24:MI'), 'month')
)
    SELECT year,
      (FIRST_VALUE(month) OVER (PARTITION BY sum(sales) ORDER BY month)) AS selling_month,
     sum(sales) total_sales
   from cte_year_sales
   group by year, month order by sum(sales); 


------------------------------------------------------------------
--least_sold_item 
WITH cte_year_sales AS (
    SELECT 
        TO_CHAR(TO_DATE(INVOICEDATE , 'MM/DD/YYYY HH24:MI'), 'yyyy') AS year,
        TO_CHAR(TO_DATE(INVOICEDATE , 'MM/DD/YYYY HH24:MI'), 'month') AS month,
        SUM(quantity * price) AS sales, 
        stockcode
    FROM tableretail
    GROUP BY TO_CHAR(TO_DATE(INVOICEDATE , 'MM/DD/YYYY HH24:MI'), 'yyyy'),
             TO_CHAR(TO_DATE(INVOICEDATE , 'MM/DD/YYYY HH24:MI'), 'month'),
             stockcode
),
cte_least_sales_month AS (
    SELECT year, month, sum(sales) total_sales, stockcode,
           ROW_NUMBER() OVER (PARTITION BY month ORDER BY sum(sales)) AS rn
    FROM cte_year_sales
    group by year, month, stockcode
),
cte_least_selling_items as(
    SELECT year, month, stockcode, total_sales,
           ROW_NUMBER() OVER (PARTITION BY rn ORDER BY total_sales) AS product_rn
    FROM cte_least_sales_month )
    select* from cte_least_selling_items where product_rn = 1;
---------------------------------------------------------------------------------
--least sold product of jan
WITH cte_stockcode AS (
    SELECT 
        stockcode AS product_id,
        SUM(quantity * price) AS sales,
        TO_CHAR(TO_DATE(INVOICEDATE , 'MM/DD/YYYY HH24:MI'), 'dd') AS month
    FROM tableretail
    GROUP BY stockcode, TO_CHAR(TO_DATE(INVOICEDATE , 'MM/DD/YYYY HH24:MI'), 'dd')
)
SELECT 
    product_id,
    month,
    SUM(sales) AS total_sales,
    DENSE_RANK() OVER (ORDER BY SUM(sales)) AS dr
FROM 
    cte_stockcode 
WHERE 
    month = '01'
GROUP BY 
    product_id, 
    month 
ORDER BY 
    SUM(sales);
--------------------------------------------------  

  -- most purchased products in Nov.
    SELECT
        stockcode AS product_id,
        SUM(quantity * price) AS total_sales,
        dense_rank() over(ORDER BY SUM(quantity) DESC) AS rank
    FROM tableretail
    WHERE EXTRACT(MONTH FROM TO_DATE(InvoiceDate, 'MM/DD/YYYY HH24:MI')) = 11 -- Filtering for November
    GROUP BY stockcode;

-------------------------------
--the most selling product not only in Nov
SELECT
        stockcode AS product_id,
        SUM(quantity * price) AS total_sales,
        dense_rank() over(order by sum(quantity) desc) as Rank
    FROM tableretail
    GROUP BY stockcode order by rank;
------------------------------------------------------------------------
--monthly customers growth rate
WITH customers_monthly AS (
    SELECT
        EXTRACT(YEAR FROM TO_DATE(InvoiceDate, 'MM/DD/YYYY HH24:MI')) AS year,
        EXTRACT(MONTH FROM TO_DATE(InvoiceDate, 'MM/DD/YYYY HH24:MI')) AS month,
       count(customer_id) AS customers
    FROM tableRetail
    GROUP BY EXTRACT(YEAR FROM TO_DATE(InvoiceDate, 'MM/DD/YYYY HH24:MI')), EXTRACT(MONTH FROM TO_DATE(InvoiceDate, 'MM/DD/YYYY HH24:MI'))
),
customers_previous_month AS (
    SELECT
        year,
        month,
        customers,
        LAG(customers) OVER (ORDER BY year, month) AS previous_customer
    FROM customers_monthly
)
SELECT
    year,
    month,
    ROUND(customers) AS customers,
    ROUND(previous_customer) AS previous_customer,
    CASE
        WHEN previous_customer IS NULL THEN 0
        ELSE ROUND(CAST((customers - previous_customer) / previous_customer * 100 AS numeric), 2)
    END AS customers_growth_rate
FROM customers_previous_month
ORDER BY year, month;
----------------------------------------------
--average time between purchases for customers
WITH cte_invoice_date AS (
    SELECT
        Customer_ID,
        TO_DATE(InvoiceDate, 'MM/DD/YYYY HH24:MI') as invoice_date
    FROM tableretail
),
PurchaseGaps AS (
    SELECT 
        customer_id, 
        invoice_date, 
        LAG(invoice_date) OVER (PARTITION BY Customer_ID ORDER BY invoice_date) AS PreviousPurchaseDate,
        (invoice_date - LAG(invoice_date) OVER (PARTITION BY Customer_ID ORDER BY invoice_date)) AS TimeBetweenPurchases
    FROM cte_invoice_date
),
CustomerProductSales AS (
    SELECT
        Customer_ID,
        stockcode as product_id,
        COUNT(*) AS ProductPurchaseCount
    FROM tableretail
    GROUP BY Customer_ID, stockcode
),
RankedProducts AS (
    SELECT
        Customer_ID,
        Product_ID,
        ProductPurchaseCount,
        RANK() OVER (PARTITION BY Customer_ID ORDER BY ProductPurchaseCount DESC) AS ProductRank
    FROM CustomerProductSales
)
SELECT
    pg.Customer_ID,
    ROUND(AVG(pg.TimeBetweenPurchases)) AS AverageTimeBetweenPurchases,
    rp.Product_ID AS MostSellingProduct,
    rp.ProductPurchaseCount
FROM PurchaseGaps pg
JOIN RankedProducts rp ON pg.Customer_ID = rp.Customer_ID AND rp.ProductRank = 1
WHERE pg.PreviousPurchaseDate IS NOT NULL
GROUP BY pg.Customer_ID, rp.Product_ID, rp.ProductPurchaseCount;
-------------------------------------------------------------------------------------------
-- relation between increasing sales and holidays
WITH holiday_sales AS (
    SELECT
        EXTRACT(YEAR FROM TO_DATE(InvoiceDate, 'MM/DD/YYYY HH24:MI')) AS sales_year,
        EXTRACT(MONTH FROM TO_DATE(InvoiceDate, 'MM/DD/YYYY HH24:MI')) AS sales_month,
        SUM(quantity * price) AS total_sales
    FROM
        tableretail
    WHERE
        EXTRACT(MONTH FROM TO_DATE(InvoiceDate, 'MM/DD/YYYY HH24:MI')) IN (12, 11)  
    GROUP BY
        EXTRACT(YEAR FROM TO_DATE(InvoiceDate, 'MM/DD/YYYY HH24:MI')),
        EXTRACT(MONTH FROM TO_DATE(InvoiceDate, 'MM/DD/YYYY HH24:MI'))
),
average_monthly_sales AS (
    SELECT
        EXTRACT(MONTH FROM TO_DATE(InvoiceDate, 'MM/DD/YYYY HH24:MI')) AS month,
        AVG(quantity * price) AS avg_sales
    FROM
        tableretail
    GROUP BY
        EXTRACT(MONTH FROM TO_DATE(InvoiceDate, 'MM/DD/YYYY HH24:MI'))
)
SELECT
    hs.sales_year,
    hs.sales_month,
    hs.total_sales,
    ams.avg_sales,
    CASE
        WHEN hs.total_sales > ams.avg_sales THEN 'Above Average'
        WHEN hs.total_sales < ams.avg_sales THEN 'Below Average'
        ELSE 'Equal to Average'
    END AS sales_comparison
FROM
    holiday_sales hs
JOIN
    average_monthly_sales ams ON hs.sales_month = ams.month
ORDER BY
    hs.sales_year, hs.sales_month;

--------------------------------------
--q2
WITH cte_customers AS (
    SELECT 
    customer_id,
round(   
 (SELECT MAX(TO_DATE(INVOICEDATE , 'MM/DD/YYYY HH24:MI')) FROM tableretail ) - 
    MAX(TO_DATE(INVOICEDATE , 'MM/DD/YYYY HH24:MI'))) AS RECENCY,
    COUNT(DISTINCT INVOICEdate) AS FREQUENCY,
    SUM(quantity * price) AS Monetary
FROM tableretail 
GROUP BY customer_id
),
cte_r_rm AS (
    SELECT 
        customer_id,
        RECENCY,
        FREQUENCY,
        Monetary,
        NTILE(5) OVER(ORDER BY Recency DESC) AS R_Score,
      round ((NTILE(5) OVER(ORDER BY AVG(frequency) DESC) + NTILE(5) OVER(ORDER BY AVG(Monetary) DESC))/2) AS F_M_Score
    FROM cte_customers
    group by customer_id, RECENCY, FREQUENCY, Monetary
)
SELECT 
    customer_id,
    RECENCY,
    FREQUENCY,
    Monetary,
    R_Score,
    F_M_Score,
    CASE 
            WHEN R_Score = 5 AND F_M_Score IN (5, 4) THEN 'Champions'
            WHEN R_Score = 4 AND F_M_Score = 5 THEN 'Champions'
            WHEN R_Score = 5 AND F_M_Score = 2 THEN 'Potential Loyalists'
            WHEN R_Score = 4 AND F_M_Score in (2 , 3) THEN 'Potential Loyalists'
            WHEN R_Score = 3 AND F_M_Score = 3 THEN 'Potential Loyalists'
            WHEN R_Score = 5 AND F_M_Score = 3 THEN 'Loyal Customers'
            WHEN R_Score = 4 AND F_M_Score = 4 THEN 'Loyal Customers'
            WHEN R_Score = 3 AND F_M_Score in (4 , 5) THEN 'Loyal Customers'
            WHEN R_Score = 5 AND F_M_Score = 1 THEN 'Recent Customers'
            WHEN R_Score = 4 AND F_M_Score = 1 THEN 'Promising'
            WHEN R_Score = 3 AND F_M_Score = 1 THEN 'Promising'
            WHEN R_Score = 3 AND F_M_Score = 2 THEN 'Customers Needing Attention'
            WHEN R_Score = 2 AND F_M_Score IN (2, 3) THEN 'Customers Needing Attention'
             WHEN R_Score = 1 AND F_M_Score = 3 THEN 'At Risk'
            WHEN R_Score = 2 AND F_M_Score IN (4, 5) THEN 'At Risk'
            WHEN R_Score = 1 AND F_M_Score = 2 THEN 'Hibernating'
            WHEN R_Score = 1 AND F_M_Score IN (4, 5) THEN 'Cant Lose Them'
            WHEN R_Score = 1 AND F_M_Score = 1 THEN 'Lost'
            ELSE 'Undefined'
    END AS Customer_Segment
FROM cte_r_rm
order by customer_id;
-----------------------------------------------
--q3(a)
WITH ranked_transactions AS (
    SELECT 
        cust_id, 
        Calendar_Dt, 
        ROW_NUMBER() OVER (PARTITION BY cust_id ORDER BY Calendar_Dt) AS rn
    FROM 
        customertransactions
),
transaction_diffs AS (
    SELECT 
        cust_id, 
        Calendar_Dt, 
        Calendar_Dt - rn AS date_diff
    FROM 
        ranked_transactions
)
SELECT 
    cust_id, 
    MAX(consecutive_days) AS max_consecutive_days
FROM (
    SELECT 
        cust_id, 
        COUNT(date_diff) AS consecutive_days
    FROM  
        transaction_diffs
    GROUP BY 
        cust_id, date_diff
)
GROUP BY 
    cust_id
ORDER BY 
    cust_id;
---------------------------------------------------------------------
--q3(b)
WITH customer_transactions_total AS (
    SELECT 
        cust_id, 
        calendar_dt,
        SUM(amt_le) OVER (PARTITION BY cust_id ORDER BY calendar_dt) AS total_spent
    FROM 
        customertransactions
),
low_spending_customers AS (
    SELECT  
        cust_id, 
        calendar_dt
    FROM 
        customer_transactions_total 
    WHERE 
        total_spent < 250
),
high_spending_customers AS (
    SELECT 
        cust_id, 
        calendar_dt, 
        total_spent
    FROM 
        customer_transactions_total
    WHERE 
        total_spent >= 250
),
low_spending_customer_days AS (
    SELECT  
        cust_id, 
        COUNT(calendar_dt) AS days 
    FROM 
        low_spending_customers
    GROUP BY 
        cust_id
)
SELECT  
   ROUND(AVG(days)) AS average_days 
FROM 
    low_spending_customer_days
WHERE 
    cust_id IN (SELECT cust_id FROM high_spending_customers);
