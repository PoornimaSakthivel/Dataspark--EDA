#1.Customer Classfication based on Gender
	select count(*) from customers where Gender='Male';
	select count(*) from customers where Gender='Female';
#2.Customer Classification based on Country
	SELECT country, COUNT(Name) AS customer_count
	FROM customers
	GROUP BY country;
#3.Customer Classification based on Age
	SELECT 
    COUNT(CASE WHEN age BETWEEN 18 AND 25 THEN 1 END) AS Age_18_to_25,
    COUNT(CASE WHEN age BETWEEN 26 AND 35 THEN 1 END) AS Age_26_to_35,
    COUNT(CASE WHEN age BETWEEN 36 AND 45 THEN 1 END) AS Age_36_to_45,
    COUNT(CASE WHEN age BETWEEN 46 AND 55 THEN 1 END) AS Age_46_to_55,
    COUNT(CASE WHEN age BETWEEN 56 AND 65 THEN 1 END) AS Age_56_to_65,
    COUNT(CASE WHEN age > 66 THEN 1 END) AS Age_Above_66,
    COUNT(CASE WHEN age < 18 THEN 1 END) AS Age_Under_18
	FROM 
    customers;
#4.Total Sales Based on Category
	Select Category,SUM(nofosalesofproduct) as total_Sales,count(*) from productsvssalesdata GROUP BY Category;
#5.Total Sales Based on SubCategory
	Select subcategory,SUM(nofosalesofproduct) as total_Sales from productsvssalesdata GROUP BY subcategory;
#6.Most Selled Products
	Select `Product Name`,SUM(Quantity) as total_Sales_of_product from salesproducts GROUP BY `Product Name` ORDER BY total_Sales_of_product  DESC LIMIT 5 ;
#7.Least Selled Products
	Select `Product Name`,SUM(Quantity) as total_Sales_of_product from salesproducts GROUP BY `Product Name` ORDER BY total_Sales_of_product  ASC LIMIT 5 ;

#8.Number of Stores in Country
	Select Country,count(*) As countsalesstores from salesstores GROUP BY Country  ORDER BY countsalesstores ;
#9.Sales based on the Store Square Meters
	Select `Square Meters`,count(*) As countsalesstores from salesstores GROUP BY `Square Meters`   ORDER BY countsalesstores ;
#10.Store Wise Sales
	Select StoreKey,count(*) As countsalesstores from salesstores GROUP BY StoreKey  ORDER BY countsalesstores;
#11.High Revenue products and it's sold quantity
	SELECT `Product Name`
	, ROUND(SUM(Quantity*`Unit Price USD`)) AS total_revenue,SUM(Quantity)
	FROM salesproducts
	GROUP BY `Product Name` Order By total_revenue DESC LIMIT 10 ;

#12.High Sold Products and it's revenue
	SELECT `Product Name`
	, ROUND(SUM(Quantity*`Unit Price USD`)) AS total_revenue,SUM(Quantity)
	FROM salesproducts
	GROUP BY `Product Name` Order By SUM(Quantity) DESC LIMIT 10;

#13.Year Wise Sales
	SELECT YEAR(`Order Date`) AS year, SUM(Quantity) AS total_sales
	FROM salesproducts
	GROUP BY YEAR(`Order Date`);