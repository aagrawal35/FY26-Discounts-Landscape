-- Databricks notebook source
select *
from serde_dwh.cg_cgcommerce_idp_evts_prd_7216_tax__tax_commerce_user1716878411_1
limit 100

-- COMMAND ----------

describe serde_dwh.cg_cgcommerce_idp_evts_prd_7216_tax__tax_commerce_user1716878411_1

-- COMMAND ----------

select distinct eventtype
from serde_dwh.cg_cgcommerce_idp_evts_prd_7216_tax__tax_commerce_user1716878411_1 tc
  where domainAttributes.taxYear = 2023
  and ((tc.year = 2023 AND tc.month = 12) OR (tc.year = 2024 and tc.month >= 01 AND tc.month < 05))

-- COMMAND ----------

select distinct eventtype
from serde_dwh.cg_cgcommerce_idp_evts_prd_7216_tax__tax_commerce_user1716878411_1 tc
  where domainAttributes.taxYear = 2024
  and ((tc.year = 2024 AND tc.month = 12) OR (tc.year = 2025 and tc.month >= 01 AND tc.month < 05))

-- COMMAND ----------



-- Create a pipeline for TY 25
-- dec 24 -Jan25 - Apr 25
-- dec 23 -Jan24 - Apr 24


-- Picks the first and last version for each auth
-- Maybe to identify changes over time in pricing_segments or productFamily 
-- For each auth,tax_year -> pricing segment, productFamily at latest and earliest date


CREATE TEMP VIEW commerce_pricing_segment_ty24raw as
Select
  domainAttributes.ownerId AS auth_id,
  cast(domainattributes.taxYear as int) AS tax_year,
  domainattributes.originalproductfamily,
  domainAttributes.productFamily AS productFamily,
  explode_outer(domainattributes.segments) as pricing_segments,
  domainattributes.pricingprioritycode,
  cast(from_unixtime(systemInfo.version/1000000) as timestamp) as event_datetime,
  row_number() over (
      partition by domainAttributes.ownerId
      order by systeminfo.version asc
    ) as min_rank,
  row_number() over (
      partition by domainAttributes.ownerId
      order by systeminfo.version desc
    ) as max_rank
FROM serde_dwh.cg_cgcommerce_idp_evts_prd_7216_tax__tax_commerce_user1716878411_1 tc
WHERE 1 = 1
  and domainAttributes.taxYear = 2024
  and tc.year = 2025 
  and (tc.month >= 01) AND (tc.month < 05) 
  and tc.domainattributes.productfamily IS NOT NULL
  and tc.domainattributes.segments is not null

create TEMP VIEW commerce_pricing_segment_ty24raw2 as

Select
  domainAttributes.ownerId AS auth_id,
  cast(domainattributes.taxYear as int) AS tax_year,
  domainattributes.originalproductfamily,
  domainAttributes.productFamily AS productFamily,
  explode_outer(domainattributes.segments) as pricing_segments,
  domainattributes.pricingprioritycode,
  cast(from_unixtime(systemInfo.version/1000000) as timestamp) as event_datetime,
  row_number() over (
      partition by domainAttributes.ownerId
      order by systeminfo.version asc
    ) as min_rank,
  row_number() over (
      partition by domainAttributes.ownerId
      order by systeminfo.version desc
    ) as max_rank
FROM serde_dwh.cg_cgcommerce_idp_evts_prd_7216_tax__tax_commerce_user1716878411_1 tc
WHERE 1 = 1
  and domainAttributes.taxYear = 2024
  and tc.year = 2024 
  and (tc.month = 12)
  and tc.domainattributes.productfamily IS NOT NULL
  and tc.domainattributes.segments is not null



create TEMP VIEW commerce_pricing_segment_ty23raw as

Select
  domainAttributes.ownerId AS auth_id,
  cast(domainattributes.taxYear as int) AS tax_year,
  domainattributes.originalproductfamily,
  domainAttributes.productFamily AS productFamily,
  explode_outer(domainattributes.segments) as pricing_segments,
  domainattributes.pricingprioritycode,
  cast(from_unixtime(systemInfo.version/1000000) as timestamp) as event_datetime,
  row_number() over (
      partition by domainAttributes.ownerId
      order by systeminfo.version asc
    ) as min_rank,
  row_number() over (
      partition by domainAttributes.ownerId
      order by systeminfo.version desc
    ) as max_rank
FROM serde_dwh.cg_cgcommerce_idp_evts_prd_7216_tax__tax_commerce_user1716878411_1 tc
WHERE 1 = 1
  and domainAttributes.taxYear = 2023
  and ((tc.year = 2023 AND tc.month = 12) OR (tc.year = 2024 and tc.month >= 01 AND tc.month < 05))
  and tc.domainattributes.productfamily IS NOT NULL
  and tc.domainattributes.segments is not null


create table if not exists cgan_ustax_ws.commerce_pricing_segment_fullbase as
select tax_year
  , auth_id
  , productFamily
  , pricing_segments.segment as pricing_segment
  , event_datetime
From commerce_pricing_segment_ty24raw
WHERE (min_rank = 1 or max_rank = 1)
Group by 1,2,3,4,5

Union all
select tax_year
  , auth_id
  , productFamily
  , pricing_segments.segment as pricing_segment
  , event_datetime
From commerce_pricing_segment_ty24raw2
WHERE (min_rank = 1 or max_rank = 1)
Group by 1,2,3,4,5

Union all
select tax_year
  , auth_id
  , productFamily
  , pricing_segments.segment as pricing_segment
  , event_datetime
From commerce_pricing_segment_ty23raw
WHERE (min_rank = 1 or max_rank = 1)
Group by 1,2,3,4,5
  

-- COMMAND ----------

-- DBTITLE 1,commerce_pricing_segment_ty24raw
-- For Tax Year 2024 where data was generated in Jan to Apr 2025

drop table if exists cgan_ustax_ws.commerce_pricing_segment_ty24raw;
create table if not exists cgan_ustax_ws.commerce_pricing_segment_ty24raw as



Select
  domainAttributes.ownerId AS auth_id,
  cast(domainattributes.taxYear as int) AS tax_year,
  domainattributes.originalproductfamily,
  domainAttributes.productFamily AS productFamily,
  explode_outer(domainattributes.segments) as pricing_segments,
  domainattributes.pricingprioritycode,
  cast(from_unixtime(systemInfo.version/1000000) as timestamp) as event_datetime,
  row_number() over (
      partition by domainAttributes.ownerId
      order by systeminfo.version asc
    ) as min_rank,
  row_number() over (
      partition by domainAttributes.ownerId
      order by systeminfo.version desc
    ) as max_rank
FROM serde_dwh.cg_cgcommerce_idp_evts_prd_7216_tax__tax_commerce_user1716878411_1 tc
WHERE 1 = 1
  and domainAttributes.taxYear = 2024
  and tc.year = 2025 
  and (tc.month >= 01) AND (tc.month < 05) 
  and tc.domainattributes.productfamily IS NOT NULL
  and tc.domainattributes.segments is not null
  
  ---and concat(year, month, day, hour) = 2025022500


-- COMMAND ----------

-- DBTITLE 1,commerce_pricing_segment_ty24raw2
-- For Tax Year 2024 where data was generated in Dec 2024
-- We can join combine this table with the previous one
drop table if exists cgan_ustax_ws.commerce_pricing_segment_ty24raw2;
create table if not exists cgan_ustax_ws.commerce_pricing_segment_ty24raw2 as

Select
  domainAttributes.ownerId AS auth_id,
  cast(domainattributes.taxYear as int) AS tax_year,
  domainattributes.originalproductfamily,
  domainAttributes.productFamily AS productFamily,
  explode_outer(domainattributes.segments) as pricing_segments,
  domainattributes.pricingprioritycode,
  cast(from_unixtime(systemInfo.version/1000000) as timestamp) as event_datetime,
  row_number() over (
      partition by domainAttributes.ownerId
      order by systeminfo.version asc
    ) as min_rank,
  row_number() over (
      partition by domainAttributes.ownerId
      order by systeminfo.version desc
    ) as max_rank
FROM serde_dwh.cg_cgcommerce_idp_evts_prd_7216_tax__tax_commerce_user1716878411_1 tc
WHERE 1 = 1
  and domainAttributes.taxYear = 2024
  and tc.year = 2024 
  and (tc.month = 12)
  and tc.domainattributes.productfamily IS NOT NULL
  and tc.domainattributes.segments is not null

-- COMMAND ----------

-- DBTITLE 1,commerce_pricing_segment_ty23raw

-- For Tax Year 2023 where data was generated in Dec 2023 or in Jan to Apr 2024
drop table if exists cgan_ustax_ws.commerce_pricing_segment_ty23raw;
create table if not exists cgan_ustax_ws.commerce_pricing_segment_ty23raw as

Select
  domainAttributes.ownerId AS auth_id,
  cast(domainattributes.taxYear as int) AS tax_year,
  domainattributes.originalproductfamily,
  domainAttributes.productFamily AS productFamily,
  explode_outer(domainattributes.segments) as pricing_segments,
  domainattributes.pricingprioritycode,
  cast(from_unixtime(systemInfo.version/1000000) as timestamp) as event_datetime,
  row_number() over (
      partition by domainAttributes.ownerId
      order by systeminfo.version asc
    ) as min_rank,
  row_number() over (
      partition by domainAttributes.ownerId
      order by systeminfo.version desc
    ) as max_rank
FROM serde_dwh.cg_cgcommerce_idp_evts_prd_7216_tax__tax_commerce_user1716878411_1 tc
WHERE 1 = 1
  and domainAttributes.taxYear = 2023
  and ((tc.year = 2023 AND tc.month = 12) OR (tc.year = 2024 and tc.month >= 01 AND tc.month < 05))
  and tc.domainattributes.productfamily IS NOT NULL
  and tc.domainattributes.segments is not null

-- COMMAND ----------

-- DBTITLE 1,commerce_pricing_segment_fullbase
drop table if exists cgan_ustax_ws.commerce_pricing_segment_fullbase;
create table if not exists cgan_ustax_ws.commerce_pricing_segment_fullbase as

select tax_year
  , auth_id
  , productFamily
  , pricing_segments.segment as pricing_segment
  , event_datetime
From cgan_ustax_ws.commerce_pricing_segment_ty24raw
WHERE (min_rank = 1 or max_rank = 1)
Group by 1,2,3,4,5

Union all
select tax_year
  , auth_id
  , productFamily
  , pricing_segments.segment as pricing_segment
  , event_datetime
From cgan_ustax_ws.commerce_pricing_segment_ty24raw2
WHERE (min_rank = 1 or max_rank = 1)
Group by 1,2,3,4,5

Union all
select tax_year
  , auth_id
  , productFamily
  , pricing_segments.segment as pricing_segment
  , event_datetime
From cgan_ustax_ws.commerce_pricing_segment_ty23raw
WHERE (min_rank = 1 or max_rank = 1)
Group by 1,2,3,4,5

-- COMMAND ----------

-- DBTITLE 1,auth_pricing_segment_base
-- complete and start metrics
-- Check for event_datetime
-- Validate -> Are we connecting the correct pricing segment to the correct auth id
-- tax_year, auth, pricing segment -> first_completed_date,completed_sku
-- -- tax_year, auth, pricing segment -> first_start_date,start_sku
-- Just briong in the max_rank and min_rank in here and join with start and complete

drop table if exists cgan_ustax_ws.auth_pricing_segment_base;
create table if not exists cgan_ustax_ws.auth_pricing_segment_base as

Select a.tax_year
  , 'complete' as metric_type
  , p.first_completed_date as event_datetime
  , a.auth_id
  , a.pricing_segment
  , p.completed_sku as sku
from cgan_ustax_ws.commerce_pricing_segment_fullbase a
join tax_rpt.product_analytics_master as p
  on a.tax_year = p.tax_year
  and a.auth_id = p.auth_id
  and p.first_completed_date BETWEEN (a.event_datetime - INTERVAL 1 DAY) AND (a.event_datetime + INTERVAL 1 DAY)
  and p.first_completed_date is not null and p.completed_sku is not null
GROUP BY 1,2,3,4,5,6
--- and a.pricing_segment is not null

Union all

Select a.tax_year
  , 'start' as metric_type
  , p.first_start_date as event_datetime
  , a.auth_id
  , a.pricing_segment
  , p.start_sku as sku
from cgan_ustax_ws.commerce_pricing_segment_fullbase a
join tax_rpt.product_analytics_master as p
  on a.tax_year = p.tax_year
  and a.auth_id = p.auth_id
  and p.first_start_date BETWEEN (a.event_datetime - INTERVAL 1 DAY) AND (a.event_datetime + INTERVAL 1 DAY)
  and p.first_start_date is not null and p.start_sku is not null
GROUP BY 1,2,3,4,5,6
--- and a.pricing_segment is not null
--- where auth_id = '123145875855004' --pricing_segment = 'FREEMOBILE' OR 
--- where auth_id = '100005463'

-- COMMAND ----------

drop table if exists cgan_ustax_ws.sku_list_price purge;
create table if not exists cgan_ustax_ws.sku_list_price 
select UPPER(pam.completed_sku) as sku
, pam.tax_year
, month (pam.first_completed_date) as price_start_month
, mode(mam.total_federal_revenue) as list_price
from tax_rpt.product_analytics_master pam left join tax_rpt.monetization_analytics_master mam
on pam.auth_id = mam.auth_id
and pam.tax_year = mam.tax_year
where pam.tax_year in (2023,2024,2025)
and pam.completed_sku is not null
and (pam.first_completed_date) is not null
group by 1,2,3
order by 1,2,3

-- COMMAND ----------

-- DBTITLE 1,Target State: discount_segments_base
DROP TABLE IF EXISTS cgan_ustax_ws.price_discount_master;
CREATE TABLE IF NOT EXISTS cgan_ustax_ws.price_discount_master USING PARQUET AS

WITH ama_base AS (
  SELECT a.tax_year,
      a.metric_id AS auth_id,
      a.metric_type,
      case when a.metric_type = 'start' then p.first_start_date
          else p.first_completed_date end as event_date, ---DATE(a.date) AS event_date,
      case when a.metric_type = 'start' then p.start_sku
          else p.completed_sku end as sku, ---a.product AS sku,
      case when a.metric_type = 'start' then p.start_app_type
          else p.first_complete_app_type end as app_type, ---a.product AS sku,
      a.channel_group,
      a.channel,
      a.price_priority_code,
      p.tto_segment_rollup,
      p.customer_type_rollup
  FROM tax_rpt.agg_marketing_analytics a
   LEFT JOIN tax_rpt.product_analytics_master p
    ON a.tax_year = p.tax_year
    AND a.metric_id = p.auth_id
  WHERE a.tax_year IN ('2024','2023')
    AND a.metric_type IN ('start', 'complete')
),


-- pcode_base AS (
--   SELECT 
--       priority_code,
--       pcmap.sku_id,
--       sr.display_sku_name_with_sort,
--       MAX(promotion_objective) AS promotion_objective,
--       DATE(MIN(promotion_price_start_date)) AS promotion_price_start_date,
--       DATE(MAX(promotion_price_end_date)) AS promotion_price_end_date,
--       MAX(list_price) AS list_price,
--       MAX(sale_price) AS sale_price,
--       MAX(discount_value) AS discount_value,
--       MAX(discount_type) AS discount_type
--   FROM cgan_ustax_published.pcode_promo_item_mapping pcmap
--   JOIN tax_dm.dim_sku_rollup sr 
--     ON pcmap.sku_id = sr.sku_id
--   WHERE DATE(promotion_price_start_date) >= '2023-12-01'
--     AND product_family_description LIKE '%FEDERAL%'
--     --- AND discount_value > 0
--     AND priority_code IS NOT NULL 
--     AND pricing_segment IS NULL 
--   GROUP BY 1,2,3
-- ),



pcode_base AS (
  SELECT 
      pcmap.priority_code,
      pcmap.sku_id,
      sr.display_sku_name_with_sort,
      pcmap.promotion_objective,
      DATE(pcmap.promotion_price_start_date) AS promotion_price_start_date,
      DATE(pcmap.promotion_price_end_date) AS promotion_price_end_date,
      pcmap.list_price,
      pcmap.sale_price,
      pcmap.discount_value,
      pcmap.discount_type
  FROM cgan_ustax_published.pcode_promo_item_mapping pcmap
  JOIN tax_dm.dim_sku_rollup sr 
    ON pcmap.sku_id = sr.sku_id
  WHERE DATE(pcmap.promotion_price_start_date) >= '2023-12-01'
    AND pcmap.product_family_description LIKE '%FEDERAL%'
    AND display_sku_name_with_sort not like '%FULL SERVICE%'
    --- AND discount_value > 0
    AND pcmap.priority_code IS NOT NULL 
    AND pcmap.pricing_segment IS NULL 
  GROUP BY 1,2,3
),



-- pricing_segment_base AS (
--   SELECT 
--       a.tax_year,
--       a.auth_id,
--       a.metric_type,
--       a.pricing_segment,
--       b.sku_id,
--       a.sku,
--       MAX(b.promotion_objective) AS promotion_objective,
--       DATE(MIN(b.promotion_price_start_date)) AS promotion_price_start_date,
--       DATE(MAX(b.promotion_price_end_date)) AS promotion_price_end_date,
--       DATE(a.event_datetime) AS event_datetime,
--       MAX(b.list_price) AS list_price,
--       MAX(b.sale_price) AS sale_price,
--       MAX(b.discount_value) AS discount_value,
--       MAX(b.discount_type) AS discount_type
--   FROM cgan_ustax_ws.auth_pricing_segment_base a
--   LEFT JOIN (SELECT pcmap.*, sr.display_sku_name_with_sort
--             FROM cgan_ustax_published.pcode_promo_item_mapping pcmap
--             LEFT JOIN tax_dm.dim_sku_rollup sr ON pcmap.sku_id = sr.sku_id
--             ) b
--               ON a.pricing_segment = b.pricing_segment
--               AND a.sku = b.display_sku_name_with_sort
--               AND DATE(b.promotion_price_start_date) >= '2023-12-01'
--               AND a.event_datetime BETWEEN b.promotion_price_start_date and b.promotion_price_end_date
--               --- AND b.discount_value > 0
--               AND b.priority_code IS NULL
--               AND b.pricing_segment IS NOT NULL
--   WHERE a.pricing_segment is not null
--   GROUP BY 1,2,3,4,5,6,10
-- ),



pricing_segment_base AS (
  SELECT 
      a.tax_year,
      a.auth_id,
      a.metric_type,
      a.pricing_segment,
      b.sku_id,
      a.sku,
      pcmap.promotion_objective,
      DATE(pcmap.promotion_price_start_date) AS promotion_price_start_date,
      DATE(pcmap.promotion_price_end_date) AS promotion_price_end_date,
      pcmap.list_price,
      pcmap.sale_price,
      pcmap.discount_value,
      pcmap.discount_type
  FROM cgan_ustax_ws.auth_pricing_segment_base a
  LEFT JOIN (SELECT pcmap.*, sr.display_sku_name_with_sort
            FROM cgan_ustax_published.pcode_promo_item_mapping pcmap
            LEFT JOIN tax_dm.dim_sku_rollup sr 
            ON pcmap.sku_id = sr.sku_id
            ) b
              ON a.pricing_segment = b.pricing_segment
              AND a.sku = b.display_sku_name_with_sort
              AND DATE(b.promotion_price_start_date) >= '2023-12-01'
              AND display_sku_name_with_sort not like '%FULL SERVICE%'
              AND a.event_datetime BETWEEN date_add(b.promotion_price_start_date,1) and b.promotion_price_end_date
              --- AND b.discount_value > 0
              AND b.priority_code IS NULL
              AND b.pricing_segment IS NOT NULL
  WHERE a.pricing_segment is not null
),

discount_base_table AS (
  SELECT 
      a.tax_year,
      a.auth_id,
      a.metric_type,
      date(a.event_date) as event_date, 
      a.sku,
      a.app_type,

      case when a.channel_group IN ('Affiliate','FI Channel') then 'Affiliate/FI'
        when a.channel_group IN ('Brand Digital','Digital Media','Social') then 'DM/Social'
        when (a.channel_group IN ('Paid Search - Brand','Paid Search - Generic','Organic Search','Non-Campaign','Mobile App') 
          OR a.channel_group is null) then 'Search/NC'
        when a.channel_group IN ('Credit Karma','Other') then 'CK'
        when a.channel_group IN ('CRM') then 'CRM' else null end as channel_group,  

      a.channel,
      a.tto_segment_rollup,
      a.customer_type_rollup,
      COALESCE(c.pricing_segment, a.price_priority_code) AS price_priority_code,
      COALESCE(c.promotion_objective, b.promotion_objective) AS promotion_objective,

      CASE WHEN a.metric_type = 'complete' and (m.total_federal_revenue <= d.list_price OR lower(b.promotion_objective) like '%willingness to pay model%') THEN d.list_price
            WHEN a.metric_type = 'complete' and m.total_federal_revenue > d.list_price THEN m.total_federal_revenue
            WHEN c.list_price IS NOT NULL THEN c.list_price
            WHEN b.list_price IS NOT NULL THEN b.list_price
            ELSE d.list_price END AS list_price,

      CASE WHEN a.metric_type = 'complete' THEN m.total_federal_revenue
           WHEN c.pricing_segment = 'FREEMOBILE' and c.sku_id IN (14, 16, 128) then 0
            WHEN c.sale_price IS NOT NULL THEN c.sale_price
            WHEN b.sale_price IS NOT NULL THEN b.sale_price
            ELSE NULL END AS sale_price,

      CASE WHEN a.metric_type = 'complete' THEN m.total_federal_revenue ELSE NULL END AS total_federal_revenue   

  FROM ama_base a
  LEFT JOIN pcode_base b
    ON a.price_priority_code = b.priority_code
    AND a.sku = b.display_sku_name_with_sort
    -- AND a.event_date BETWEEN b.promotion_price_start_date AND b.promotion_price_end_date
    AND a.event_datetime BETWEEN date_add(b.promotion_price_start_date,1) and b.promotion_price_end_date


-- This will only pull in auths that are a part of the agg_marketing_analytics table
  LEFT JOIN pricing_segment_base c
    ON a.tax_year = c.tax_year
    AND a.auth_id = c.auth_id
    AND a.metric_type = c.metric_type
    AND a.sku = c.sku
    AND a.event_date BETWEEN c.promotion_price_start_date AND c.promotion_price_end_date

  LEFT JOIN tax_rpt.monetization_analytics_master m
    ON a.auth_id = m.auth_id
    AND a.tax_year = m.tax_year

  LEFT JOIN cgan_ustax_ws.sku_list_price d
    ON a.tax_year = d.tax_year
    AND TRIM(LOWER(a.sku)) = TRIM(LOWER(d.sku))
    AND month(a.event_date) = d.price_start_month
    -- AND (date(a.event_date) BETWEEN d.price_start_date AND d.price_end_date
          -- OR (d.price_end_date IS NULL AND date(a.event_date) >= d.price_start_date)
        ) 
  

-- Final aggregation with safe discount calculation
SELECT 
    tax_year,
    auth_id,
    metric_type,
    event_date,
    sku,
    price_priority_code,
    MAX(promotion_objective) AS promotion_objective,
    MAX(list_price) AS list_price,
    MAX(sale_price) AS sale_price,
    CASE WHEN MAX(list_price) IS NULL OR MAX(sale_price) IS NULL THEN NULL ELSE (MAX(list_price) - MAX(sale_price)) END AS discount_value,
    CASE WHEN MAX(list_price) IS NULL OR MAX(sale_price) IS NULL THEN NULL 
      WHEN MAX(list_price) = 0 THEN 0
      ELSE ROUND(((MAX(list_price) - MAX(sale_price)) / MAX(list_price)) * 100, 0) 
      END AS discount_per,
    MAX(total_federal_revenue) AS total_federal_revenue,
    app_type, 
    channel_group,
    channel,
    tto_segment_rollup,
    customer_type_rollup
    
FROM discount_base_table
GROUP BY 
    tax_year, auth_id, metric_type, event_date, price_priority_code,
    sku, channel_group, channel, tto_segment_rollup, customer_type_rollup, app_type
ORDER BY 
    tax_year, auth_id, metric_type, event_date;


-- COMMAND ----------

-- DBTITLE 1,Validation Query
select pcmap.priority_code,
      pcmap.sku_id,
      sr.display_sku_name_with_sort,
      pcmap.date_range_name
  FROM cgan_ustax_published.pcode_promo_item_mapping pcmap
  JOIN tax_dm.dim_sku_rollup sr 
    ON pcmap.sku_id = sr.sku_id
    
    WHERE DATE(promotion_price_start_date) >= '2023-12-01'
    AND product_family_description LIKE '%FEDERAL%'
    AND pcmap.priority_code is not null
    AND display_sku_name_with_sort not like '%FULL SERVICE%'
    group by 1,2,3,4
    having count(distinct list_price)>1
    order by 1,2,3

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Specific Cohorts
