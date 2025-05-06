-- =============================================================================
-- EDA : Exploratory Data Analysis: 
-- =============================================================================

-- Data Cleaning and Transformation: 

-- First table : 
select * from bronze.crm_cust_info

------- check for nulls or duplicated rows in primary key

select cst_id,count(*) 
from bronze.crm_cust_info
group by cst_id
having count(*) > 1 or cst_id is null

---- To see every duplicated PK and think for the solution : 
select * , row_number () over (partition by cst_id order by cst_create_date desc) as RN
from bronze.crm_cust_info
where cst_id = 29466            ---- this cst_id = 29466 is an example from the query before to think for the solution
---- when i make it order by the desc order : i only take the newest row form the duplicated data 
---- and only choose when the RN = 1 to make sure that the primary key is unique and there is not duplicated

select *
from(
	select * , row_number () over (partition by cst_id order by cst_create_date desc) as RN
	from bronze.crm_cust_info) t 
where RN = 1

-----           (perform these steps for each column that i want to check about)
---- check for the unwanted spaces : (perform this step for each column that i want to check about) 
select cst_firstname
from bronze.crm_cust_info
where cst_firstname != TRIM(cst_firstname)


--- check for data standardization & consistency:
select  distinct cst_gndr
from bronze.crm_cust_info

select distinct cst_marital_status
from bronze.crm_cust_info
--- i will change every f with female and m with male and null with N/A and i will do the same for the another columns

--------- so the full query for transformation for the first table is : 

select cst_id, cst_key,TRIM(cst_firstname) as cst_firstname, trim(cst_lastname) as cst_lastname , 
				case when upper(trim(cst_marital_status)) = 'M' then 'Married' when upper(trim(cst_marital_status)) = 'S' then 'Single' else 'N/A' end as cst_matrital_status,
				case when upper(trim(cst_gndr)) = 'M' then 'Male' when upper(trim(cst_gndr)) = 'F' then 'Female' else 'N/A' end as cst_gndr,
				cst_create_date
		from(
			select * , row_number () over (partition by cst_id order by cst_create_date desc) as RN
			from bronze.crm_cust_info) t 
		where RN = 1

--------------------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------------
-- Second table : 
select * from bronze.crm_prd_info

---- we check for the duplicated or null values in primary key
select prd_id,count(*)
from bronze.crm_prd_info
group by prd_id
having count(*) >1 or prd_id is null
---- we found that there is no duplicated or null

---- i will join the crm_prd_info ( prd_key column) with erp_px_cat_g1v2 (id column) :
--------- so i will spelt the prd_key column and change the (-) with (_)

select prd_id, prd_key , replace ( SUBSTRING(prd_key , 1,5),'-','_') as  cat_id, SUBSTRING(prd_key,7,len(prd_key)) as prd_key,
		prd_nm ,ISNULL(prd_cost, 0)as prd_cost ,
		case when upper(trim(prd_line)) = 'M' then 'Mountain' 
			 when upper(trim(prd_line)) = 'R' then 'Road' 
			 when upper(trim(prd_line)) = 'S' then 'Other Sales'
			 when upper(trim(prd_line)) = 'T' then 'Touring'
			 else 'N/A' end as prd_line,
		cast( prd_start_dt as date) as prd_start_dt,
		cast(lead(prd_start_dt) over (partition by prd_key order by prd_start_dt ) - 1 as date) as prd_end_dt
from bronze.crm_prd_info
----- filter the unmatched data after applying transformation : 
--  where  replace ( SUBSTRING(prd_key , 1,5),'-','_') not in (select distinct id from bronze.erp_px_cat_g1v2)
----- filter the the unmatched data after applying transformation : 
--  where SUBSTRING(prd_key,7,len(prd_key))   in (select sls_prd_key from bronze.crm_sales_details)

select * from bronze.crm_sales_details


-----
select prd_nm
from bronze.crm_prd_info
where prd_nm != TRIM(prd_nm)
--- i found that all row not have any spaces and all column is correct


---check for nulls or negative values: 
select *
from bronze.crm_prd_info
where prd_cost<0 or prd_cost is null
---- i found some null so i will change with zero 

---- check for prd_line column : i will change every letter with a word 
select distinct prd_line 
from bronze.crm_prd_info


---- check for invalid date orders : 
select * 
from bronze.crm_prd_info
where prd_end_dt < prd_start_dt

--- I found there is a problem  and i have two solution : 
---     1) to switch the start date column with end date column but i many found some problems also 
---     2) delete end date column and use lead() window functions to use the last date from the start as the end date in a new column and subtract 1 from it to get the previous date



--------------------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------------
select * from bronze.crm_sales_details

-- Check for null or dupllicated in sls_ord_num column : 
SELECT *
FROM bronze.crm_sales_details
WHERE sls_ord_num != TRIM(sls_ord_num) or sls_ord_num is null
--- every thing in this column is okay

--- check for unmatched data : 
select * 
from bronze.crm_sales_details
where sls_prd_key not in (select prd_key from silver.crm_prd_info)

select * 
from bronze.crm_sales_details
where sls_cust_id not in (select cst_id from silver.crm_cust_info)
--- every thing is okay in sls_prd_key and sls_cust_id columns 

--- check for invalid dates : 
select nullif(sls_order_dt ,0)
from bronze.crm_sales_details
where sls_order_dt <= 0 or len(sls_order_dt )!= 8 or sls_order_dt > 20500101 or sls_order_dt < 19000101

select nullif(sls_ship_dt ,0)
from bronze.crm_sales_details
where sls_ship_dt <= 0 or len(sls_ship_dt )!= 8 or sls_ship_dt > 20500101 or sls_ship_dt < 19000101

select nullif(sls_due_dt ,0)
from bronze.crm_sales_details
where sls_due_dt <= 0 or len(sls_due_dt )!= 8 or sls_due_dt > 20500101 or sls_due_dt < 19000101
 
--- check for invalid date orders : 
select * 
from bronze.crm_sales_details
where sls_order_dt > sls_ship_dt or sls_order_dt > sls_due_dt


---- values should not be null , zeros or negative 
--- Sales = quantity * price

SELECT DISTINCT sls_sales, sls_quantity, sls_price
FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
    OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
    OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price
---- so : if the sales is null or negative or zero then multiple the quantity by price 
---			if the price is zero or null then sales/quantity

SELECT
    sls_ord_num, sls_prd_key, sls_cust_id,
    case when sls_order_dt <= 0 or len(sls_order_dt) != 8 then null
	else cast(cast(sls_order_dt as varchar) as date) end as sls_order_dt,
    case when sls_ship_dt <= 0 or len(sls_ship_dt) != 8 then null
	else cast(cast(sls_ship_dt as varchar) as date) end as sls_ship_dt,
    case when sls_due_dt <= 0 or len(sls_due_dt) != 8 then null
	else cast(cast(sls_due_dt as varchar) as date) end as sls_due_dt,
    case when sls_sales is null or sls_sales <= 0  or sls_sales != sls_quantity*abs(sls_price) then sls_quantity* abs(sls_price)
	else sls_sales  end as sls_sales,
	sls_quantity,
    case when sls_price is null or sls_price = 0 then sls_sales/sls_quantity
	else sls_price end as sls_price
FROM bronze.crm_sales_details


select * from silver.crm_sales_details


--------------------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------------
select * from bronze.erp_cust_az12
select * from silver.crm_cust_info

--- check the cid column from erp_cust_az12 with cst_key from crm_cust_info : we found that we should remove NAS letter from the cid column

select case when cid like 'NAS%' then SUBSTRING(cid,4,len(cid))
		else cid end as cid,
		case when bdate > GETDATE() then null else bdate end as bdate,
		case when upper(trim(gen)) in ('F','FEMALE') then 'Female'
			 when upper(trim(gen)) in ('M','MALE') then 'Male'
			 else 'N/A' end as gen
from bronze.erp_cust_az12
---   define unmatched data with the cst_key 
--- where case when cid like 'NAS%' then SUBSTRING(cid,4,len(cid)) else cid end 
---		not in (select cst_key from silver.crm_cust_info)


--- identify out of range dates : 
select distinct bdate 
from bronze.erp_cust_az12
where bdate < '1924-01-01' or bdate > GETDATE()
---- so maybe some customer is old but ofcourse no one can be his birthday in the future so we will convert it to null


--- Data standarization & consistency : 
select distinct gen 
from bronze.erp_cust_az12




--------------------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------------
select * from bronze.erp_loc_a101
select * from silver.crm_cust_info


---- check for the cid in erp_loc_a101 table : we found that we should remove the (-) from the column to match the cst_key from the crm_cust_info
SELECT
    REPLACE(cid, '-', '') AS cid,
    CASE 
        WHEN TRIM(cntry) = 'DE' THEN 'Germany'
        WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
        WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
        ELSE TRIM(cntry)
    END AS cntry
FROM bronze.erp_loc_a101
--- to idenify the unmatched data : 
---  where replace (cid,'-','') not in (select cst_key from bronze.crm_cust_info)


--- Data standarization & consistency : 
select distinct cntry 
from bronze.erp_loc_a101



--------------------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------------
select * from bronze.erp_px_cat_g1v2
select * from silver.crm_prd_info

-- check for unmatched data : 
select * 
from bronze.erp_px_cat_g1v2
where id not in (select cat_id from silver.crm_prd_info)


--- check for unwanted spaces : 
SELECT * 
FROM bronze.erp_px_cat_g1v2
WHERE cat != TRIM(cat) OR subcat != TRIM(subcat) OR maintenance != TRIM(maintenance)

--- Data standarization & consistency : 
select distinct cat 
from bronze.erp_px_cat_g1v2

select distinct subcat 
from bronze.erp_px_cat_g1v2

select distinct maintenance 
from bronze.erp_px_cat_g1v2



--------------------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------------
--- Gold Layer : 

--- To make a customer table : 
--- when i join the table : i should check if there is any duplicates in the final table 
---						    when the result of query is empty so there is no duplicates in the final table
select * from silver.crm_cust_info

select cst_id, count(*) 
from (
		select ci.cst_id , ci.cst_key, ci.cst_firstname, ci.cst_lastname, ci.cst_marital_status, ci.cst_gndr, ci.cst_create_date, 
		ca.bdate, ca.gen, la.cntry
		from silver.crm_cust_info ci 
		left join silver.erp_cust_az12 ca 
		on		ci.cst_key = ca.cid
		left join silver.erp_loc_a101 la 
		on		ci.cst_key = la.cid
	) t 
	group by cst_id
	having count(*) >1


--- so when i use the same query without groupby to join table : i found there are two tables for gender so i will make data integration
select  ROW_NUMBER() over ( order by ci.cst_id) as RN,
		ci.cst_id ,
		ci.cst_key,
		ci.cst_firstname,
		ci.cst_lastname,
		ci.cst_marital_status,
		ci.cst_gndr,
		ci.cst_create_date,
		ca.bdate,
		ca.gen,
		la.cntry
		from silver.crm_cust_info ci 
		left join silver.erp_cust_az12 ca 
		on		ci.cst_key = ca.cid
		left join silver.erp_loc_a101 la 
		on		ci.cst_key = la.cid


--- to make data integration : i will make a new query for the gender columns only with distinct 
select  distinct ci.cst_gndr,  ca.gen
from silver.crm_cust_info ci 
left join silver.erp_cust_az12 ca 
on		ci.cst_key = ca.cid
left join silver.erp_loc_a101 la 
on		ci.cst_key = la.cid
order by 1,2

--- so i found there is no matched in some values for the two columns so i will suppose that the crm system data is more accurate than the erp

select  distinct ci.cst_gndr,  ca.gen,
		case when ci.cst_gndr != 'n/a' then ci.cst_gndr
		else coalesce (ca.gen,'n/a') end as new_gndr
from silver.crm_cust_info ci 
left join silver.erp_cust_az12 ca 
on		ci.cst_key = ca.cid
left join silver.erp_loc_a101 la 
on		ci.cst_key = la.cid
order by 1,2




---- when i make a Product table : note : i need only the current data not the historical data 
select * from silver.crm_prd_info
select * from silver.erp_px_cat_g1v2
select * from silver.crm_sales_details

select pn.prd_id,
		pn.cat_id,
		pn.prd_key,
		pn.prd_nm,
		pn.prd_cost,
		pn.prd_line,
		pn.prd_start_dt,
		pn.prd_end_dt,
		pc.cat,
		pc.subcat,
		pc.maintenance
from silver.crm_prd_info as pn
left join silver.erp_px_cat_g1v2 as pc
on		pn.cat_id = pc.id
where pn.prd_end_dt is null


---  when i join the table : i should check if there is any duplicates in the final table 
---						    when the result of query is empty so there is no duplicates in the final table
select prd_key , count(*)
from (
		select pn.prd_id, pn.cat_id, pn.prd_key, pn.prd_nm, pn.prd_cost, pn.prd_line, pn.prd_start_dt, pn.prd_end_dt,
				pc.cat, pc.subcat, pc.maintenance
		from silver.crm_prd_info as pn
		left join silver.erp_px_cat_g1v2 as pc
		on		pn.cat_id = pc.id
		where pn.prd_end_dt is null
	) t 
group by prd_key
having count(*) > 1




















































































