
--Tasks
--Credit Card Transaction Case Study
--Write queries to explore the dataset and solve the below questions:

--1) Write a query to print the top 5 cities with highest spends and their percentage contribution of total credit card spends.

with cte as (select top 5 city,sum(amount) as expense,(select sum(amount) from credit_card) as total_exp 
from credit_card group by city order by expense desc)
select city,expense,(expense*100)/total_exp as percentage_contribution from cte;

--2) Write a query to print highest spend month and amount spent in that month for each card type.

with cte as (select card_type,month(transaction_date) as mo,year(transaction_date) as yr,sum(amount) as total_amt from credit_card group by card_type,
	month(transaction_date),year(transaction_date))
	select card_type,mo,yr,total_amt from (select *,dense_rank() over(partition by card_type order by total_amt desc) as rnk from cte)x where rnk=1;


--3) Write a query to print the transaction details (all columns from the table) for each card type 
--when it reaches a cumulative of 1,000,000 total spends. (We should have 4 rows in the output, one for each card type.)

with cte as (select * from (select *,sum(amount) over(partition by card_type order by transaction_date,
transaction_id rows between unbounded preceding and current row) as cum_sum from credit_card)x where cum_sum>=1000000)
select transaction_id,city,transaction_date,card_type,exp_type,gender,amount,cum_sum from 
(select *,rank() over(partition by card_type order by cum_sum) as rnk  from cte)y where  rnk=1;


--4) Write a query to find city which had lowest percentage spend for gold card type.
with cte as (select city,sum(amount) as total_amt,(select sum(amount) from credit_card as cc where cc.city=credit_card.city) as overall_amt
from credit_card where card_type='Gold' group by city having sum(amount)>0)
select top 1 city,(total_amt*1.0)*100/overall_amt as per_cntri from cte order by per_cntri ;

--5) Write a query to print 3 columns: city, highest_expense_type, lowest_expense_type (example format: Delhi, bills, Fuel)
with cte as (select city,exp_type,sum(amount) as total_amt from Credit_card group by city,exp_type),cte2 as (
select *,rank() over(partition by city order by total_amt desc) as h_exp,rank() over(partition by city order by total_amt ) as l_exp from cte)
select city,max(case when h_exp=1 then exp_type end) as highest_expense_type,max(case when l_exp=1 then exp_type end) as lowest_expense_type
from cte2 group by city;

--6) Write a query to find percentage contribution of spends by females for each expense type.
with cte as (select exp_type,sum(amount) as amt,(select sum(amount) from Credit_card as cc where cc.exp_type=Credit_card.exp_type) as total_amt
from Credit_card where gender='F' group by exp_type)
select exp_type,amt,total_amt,(amt*1.0/total_Amt)*100 as female_contri_per from cte;

--7) Which card and expense type combination saw highest month-over-month growth in Jan-2014?
with cte as (select year(transaction_date) as yr,MONTH(transaction_date) as mn,
card_type,exp_type,sum(amount) as total_spnd from Credit_card where transaction_date >='2013-12-01' and transaction_date < '2014-02-01' group by
year(transaction_date),MONTH(transaction_date),card_type,exp_type),cte2 as (
select card_type,exp_type,yr,mn,total_spnd,total_spnd-LAG(total_spnd) over(partition by card_type,exp_type order by yr,mn) as mom_growth from cte)
SELECT TOP 1 card_type,exp_type,mom_growth AS highest_mom_growth FROM cte2 WHERE yr = 2014 AND mn = 1
ORDER BY mom_growth DESC;


--8) During weekends, which city has highest total spend to total number of transactions ratio?
select top 1 city,sum(amount)*1.0/count(*)  as ratio from Credit_card where DATEPART(weekday,transaction_date) in (1,7) group by city
order by ratio desc;

--9) Which city took least number of days to reach its 500th transaction after the first transaction in that city?
with cte as (select *,ROW_NUMBER() over(partition by city order by transaction_date,transaction_id) as rnk from Credit_card)
select top 1 city,min(transaction_date) as first_trn_date,max(transaction_date) as last_tran_date,
DATEDIFF(day,min(transaction_date),max(transaction_date)) as days_to_500 from cte where rnk in (1,500) group by city having count(*)=2 order  by days_to_500