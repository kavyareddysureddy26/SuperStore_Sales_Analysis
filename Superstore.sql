use super_store;
Drop table if exists superstore_raw;

-- Create a table 
create table superstore_raw (
order_id VARCHAR(50),
order_date VARCHAR(50),
ship_date VARCHAR(50),
ship_mode VARCHAR(50),
customer_id	VARCHAR(50),
customer_Name VARCHAR(100),
segment	VARCHAR(50),
country	VARCHAR(50),
city VARCHAR(50),
state VARCHAR(50),
postal_code VARCHAR(50),
Region VARCHAR(50),
Product_ID VARCHAR(50),
Category VARCHAR(50),
Sub_Category VARCHAR(50),
Product_Name VARCHAR(50),
Sales DECIMAL(12,2),
Quantity INT,
Discount DECIMAL(5,2),
Profit DECIMAL(12,2)
);

Select count(*) from superstore_raw;

-- DATA VALIDATION
select 
	count(*) as total_rows,
    sum(order_id IS NULL) as null_order_id,
    sum(sales IS NULL) as null_sales,
    sum(profit IS NULL) as null_profit,
    sum(quantity IS NULL) as null_quantity
from superstore_raw;

select order_date 
from superstore_raw
limit 10;

-- checking duplicates
select order_id,count(*)
from superstore_raw
group by order_id
having count(*)>1;

/* by doing the above check we can see we have more than one row for some order_id it is 
because each orde can have multiple items so it is tagged with product_id and the sales col 
so in order to know that its really a duplicate or not we check the entire row using over 
and then check if its row count is more than 1*/
with row_counted as(
select *,count(*) over(partition by order_id,product_id,sales,quantity,profit) as row_count
from superstore_raw)
select * from row_counted where row_count>1;

-- Data Standardization
-- convert order_date into date type
update superstore_raw 
set order_date =str_to_date(order_date,'%m/%d/%Y');

update superstore_raw 
set ship_date =str_to_date(ship_date,'%m/%d/%Y');

alter table superstore_raw 
modify ship_date date; 

select order_date,ship_date 
from superstore_raw limit 5;

-- data error check 
select * from superstore_raw where ship_date<order_date;

-- create new clean table
create table superstore_clean as
select * from superstore_raw;

-- extract year
alter table superstore_clean
add column order_year int;

update superstore_clean 
set order_year=year(order_date);

-- extract month
alter table superstore_clean
add column order_month int;

update superstore_clean 
set order_month=month(order_date);

select order_year,order_month 
from superstore_clean limit 5;

-- knowing delivry time
alter table superstore_clean 
add column delivery_days int;

update superstore_clean 
set delivery_days=datediff(ship_date,order_date);

select order_date,ship_date,delivery_days
from superstore_clean limit 5;

-- Use Database
use super_store;
select * from superstore_clean;

-- avg delivery time
select avg(delivery_days) as avg_delivery_time
from superstore_clean;

select max(delivery_days) as slow_delivery,min(delivery_days) as fast_delivery
from superstore_clean;

select count(order_id) as total from superstore_clean;

with total_orders as(
select count(order_id) as total from superstore_clean),
deliver_stats as(
select delivery_days,count(order_id) as delivered_per_day
from superstore_clean 
group by delivery_days)
select
	d.delivery_days,
    d.delivered_per_day,
    round((d.delivered_per_day/t.total) *100,2) as percentage_delivery
from deliver_stats d
cross join total_orders t 
order by d.delivery_days;

with total_orders as(
select count(order_id) as total from superstore_clean),
deliver_stats as(
select delivery_days,count(order_id) as delivered_per_day
from superstore_clean 
group by delivery_days)
select 
	d.delivery_days,
    d.delivered_per_day,
    sum(d.delivered_per_day) over(order by d.delivery_days) as cumu_sum,
    round(sum(d.delivered_per_day) over(order by d.delivery_days)/t.total *100,2) as cum_percentage_delivery
from deliver_stats d
cross join total_orders t 
order by d.delivery_days;

-- EDA
select sum(sales) as Total_Sales
from superstore_clean;

select sum(profit) as Total_Profits
from superstore_clean;

select round(sum(profit)/sum(sales)*100,2) as Profit_Margin_Percentage
from superstore_clean;

-- yearly summary
select order_year,sum(sales) as total_sales,sum(profit) as Total_Profits,
round(sum(profit)/sum(sales)*100,2) as Profit_Margin_Percentage_yearly
from superstore_clean
group by order_year
order by order_year;

-- category wise summary
select order_year,category,sum(sales) as Total_Sales,sum(profit) as Total_Profit,
round(sum(profit)/sum(sales)*100,2) as Profit_Margin_Percentage
from superstore_clean 
group by category,order_year
order by order_year, Profit_Margin_Percentage desc;

-- Region wise Summary
select order_year,Region,sum(sales) as Total_Sales,sum(profit) as Total_Profit,
round(sum(profit)/sum(sales)*100,2) as Profit_Margin_Percentage
from superstore_clean 
group by region,order_year
order by order_year, Profit_Margin_Percentage desc;

select 
	category,
    sum(profit) as Total_Profit,
    sum(sales) as Toatl_sales,
    sum(discount) as Toatl_discount,
    count(order_id) as Total_orders,
    round(sum(profit)/sum(sales)*100,2) as Profit_Margin_Percentage
from superstore_clean 
where region="Central" and order_year=2014
group by category;
/* by this we can see that for every 100$ furniture is loosing $5 if we do some cahnges
to increase margin it may be profitable in futue like may be dercresing discount rates */

-- Further Analysis
-- yoy growth
use super_store;
with Yearly_growth as(
select order_year,sum(sales) as total_sales
from superstore_clean
group by order_year),
prev_year_sale as(
select order_year,total_sales,lag(total_sales) over(order by order_year) as previous_year_sales
from yearly_growth)
select *,
	round((total_sales-previous_year_sales)/previous_year_sales,2) as yoy_growth 
from prev_year_sale;

with cte as(
select order_year,region,sum(profit) as Total_Profit
from superstore_clean 
group by order_year,region),
region_rank as(
select *,dense_rank() over(partition by order_year order by Total_Profit desc) as d_rank
from cte )
select * from region_rank where d_rank<=2;

with region_profit as(
select order_year,region,sum(profit) as Total_Profit
from superstore_clean
group by order_year,region),
ranked_region as(
select 
	order_year,
	region,
    Total_Profit,
    round(Total_Profit*100/sum(total_profit) over(partition by order_year),2) as profit_percentage,
	dense_rank() over(partition by order_year order by total_profit desc) as d_rank
from region_profit
order by order_year,profit_percentage desc)
select 
	order_year,
    round(sum(profit_percentage),2) as top2_profit_percentage
from ranked_region 
where d_rank<=2
group by order_year
order by order_year;


