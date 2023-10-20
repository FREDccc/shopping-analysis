-- Databricks notebook source
select * from shopping.brand

-- COMMAND ----------

select * from shopping.receipt

-- COMMAND ----------

select * from shopping.item_list

-- COMMAND ----------

select * from shopping.user_df_final

-- COMMAND ----------

--top 5 brands

-- COMMAND ----------

create or replace temporary view brand_ca
as 
select a.*, b.PurchaseDate, b.DateScanned
from shopping.item_list a join 
shopping.receipt b on a.receipt_id = b.receipt_id

-- COMMAND ----------

select max(DateScanned)
from brand_ca
where brandCode is not null

-- COMMAND ----------

--becasue 2021-02 only have few records, so we use records in 2021-01
select brandCode, count(brandCode)
from brand_ca
where left(DateScanned,7)='2021-02'
group by 1
order by 2 desc
limit 5

-- COMMAND ----------

--compared to previsous month
select brandCode, count(brandCode)
from brand_ca
where left(PurchaseDate,7)='2021-01'
group by 1
order by 2 desc
limit 5

-- COMMAND ----------

--rewardsReceiptStatus type
select distinct rewardsReceiptStatus
from shopping.receipt

-- COMMAND ----------

--average spend 'finished'
select avg(totalSpent)
from shopping.receipt
where rewardsReceiptStatus='FINISHED'

-- COMMAND ----------

--average spend 'REJECTED'
select avg(totalSpent)
from shopping.receipt
where rewardsReceiptStatus='REJECTED'

-- COMMAND ----------

--avg number of item in finished receipt
with cte as (SELECT COUNT(*) count_item,a.receipt_id
FROM shopping.item_list a join shopping.receipt b
on a.receipt_id=b.receipt_id
where rewardsReceiptStatus='FINISHED'
group by a.receipt_id)

select avg(count_item)
from cte


-- COMMAND ----------

--avg number of item in rejected receipt
with cte as (SELECT COUNT(*) count_item,a.receipt_id
FROM shopping.item_list a join shopping.receipt b
on a.receipt_id=b.receipt_id
where rewardsReceiptStatus='REJECTED'
group by a.receipt_id)

select avg(count_item)
from cte

-- COMMAND ----------

--customer spent most brand in recent 6 monthes
with cte1 as (select brandCode,sum(finalPrice) total
from brand_ca
where (PurchaseDate between '2020-08-01' and '2021-02-28') and (brandCode is not null)
group by 1
order by 2 desc)

select name,a.brandCode,total
from shopping.brand a join cte1 on a.brandCode=cte1.brandCode
order by 3 desc

-- COMMAND ----------

--most popular brand in recent 6 monthes
with cte1 as (select brandCode,count(*) count_brand
from brand_ca
where (PurchaseDate between '2020-08-01' and '2021-02-28') and (brandCode is not null)
group by 1
order by 2 desc)

select name,a.brandCode,count_brand
from shopping.brand a join cte1 on a.brandCode=cte1.brandCode
order by 3 desc


-- COMMAND ----------

--customer who spend most
select userId,sum(totalSpent) total
from shopping.receipt
group by 1
order by 2 desc

-- COMMAND ----------

--state which spent most
with cte2 as (select userId,sum(totalSpent) total
from shopping.receipt
group by 1
order by 2 desc)

select a.state, sum(total) state_total
from shopping.user_df_final a join cte2 on a.user_id=cte2.userId
where state is not null
group by a.state
order by 2 desc

-- COMMAND ----------


