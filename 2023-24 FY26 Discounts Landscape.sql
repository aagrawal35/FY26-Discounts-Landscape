-- Databricks notebook source
-- DBTITLE 1,2024 - Dec

CREATE TABLE cgan_ustax_ws.commerce_pricing_segment_ty24_12 as
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
  and tc.domainattributes.segments is not null;


-- COMMAND ----------

-- DBTITLE 1,2025- Jan- Feb
CREATE TABLE cgan_ustax_ws.commerce_pricing_segment_ty24_1_2 as
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
  and (tc.month >= 01) AND (tc.month <= 02) 
  and tc.domainattributes.productfamily IS NOT NULL
  and tc.domainattributes.segments is not null;

-- COMMAND ----------

-- DBTITLE 1,2025-Mar
CREATE TABLE cgan_ustax_ws.commerce_pricing_segment_ty24_3 as
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
  and (tc.month = 3)
  and tc.domainattributes.productfamily IS NOT NULL
  and tc.domainattributes.segments is not null;

-- COMMAND ----------

-- DBTITLE 1,2025 - Jan to Mar

CREATE TABLE cgan_ustax_ws.commerce_pricing_segment_ty24_1_3 as
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
  and (tc.month >= 01) AND (tc.month <= 03) 
  and tc.domainattributes.productfamily IS NOT NULL
  and tc.domainattributes.segments is not null;

-- COMMAND ----------

-- DBTITLE 1,2025 - Apr to Jun

CREATE TABLE cgan_ustax_ws.commerce_pricing_segment_ty24_4_6 as
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
  and (tc.month >= 04) AND (tc.month <= 06) 
  and tc.domainattributes.productfamily IS NOT NULL
  and tc.domainattributes.segments is not null;

-- COMMAND ----------

-- DBTITLE 1,2025 - Jul to Sept
CREATE TABLE cgan_ustax_ws.commerce_pricing_segment_ty24_7_9 as
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
  and (tc.month >= 07) AND (tc.month <= 09) 
  and tc.domainattributes.productfamily IS NOT NULL
  and tc.domainattributes.segments is not null;

-- COMMAND ----------

-- DBTITLE 1,2025 - Oct
CREATE TABLE cgan_ustax_ws.commerce_pricing_segment_ty24_10 as
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
  and (tc.month = 10)
  and tc.domainattributes.productfamily IS NOT NULL
  and tc.domainattributes.segments is not null;

-- COMMAND ----------


CREATE TABLE commerce_pricing_segment_ty23 as
select tax_year
  , auth_id
  , productFamily
  , pricing_segments.segment as pricing_segment
  , event_datetime
  , min_rank
  , max_rank
From commerce_pricing_segment_ty24_12
WHERE (min_rank = 1 or max_rank = 1)
Group by 1,2,3,4,5,6,7

-- COMMAND ----------

-- DBTITLE 1,2023-Dec


CREATE TABLE cgan_ustax_ws.commerce_pricing_segment_ty23_12 as
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
  and tc.year = 2023
  and (tc.month = 12)
  and tc.domainattributes.productfamily IS NOT NULL
  and tc.domainattributes.segments is not null;



-- COMMAND ----------

-- DBTITLE 1,2024-Jan-Feb

CREATE TABLE cgan_ustax_ws.commerce_pricing_segment_ty23_1_2 as
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
  and tc.year = 2024
  and (tc.month >= 01) AND (tc.month <= 02) 
  and tc.domainattributes.productfamily IS NOT NULL
  and tc.domainattributes.segments is not null;



-- COMMAND ----------

-- DBTITLE 1,2024-Mar-Apr

CREATE TABLE cgan_ustax_ws.commerce_pricing_segment_ty23_3_4 as
Select
  domainAttributes.ownerId AS auth_id,
  cast(domainattributes.taxYear as int) AS tax_year,
  domainattributes.originalproductfamily,
  domainAttributes.productFamily AS productFamily,
  explode_outer(domainattributes.segments) as pricing_segments,
  domainattributes.pricingprioritycode,
  cast(from_unixtime(systemInfo.version/1000000) as timestamp) as event_datetime,
  rank() over (
      partition by domainAttributes.ownerId
      order by systeminfo.version asc
    ) as min_rank,
  rank() over (
      partition by domainAttributes.ownerId
      order by systeminfo.version desc
    ) as max_rank
FROM serde_dwh.cg_cgcommerce_idp_evts_prd_7216_tax__tax_commerce_user1716878411_1 tc
WHERE 1 = 1
  and domainAttributes.taxYear = 2023
  and tc.year = 2024 
  and (tc.month >= 03) AND (tc.month <= 04) 
  and tc.domainattributes.productfamily IS NOT NULL
  and tc.domainattributes.segments is not null;

 

-- COMMAND ----------

-- DBTITLE 1,2024-May-June

CREATE TABLE cgan_ustax_ws.commerce_pricing_segment_ty23_5_6 as
Select
  domainAttributes.ownerId AS auth_id,
  cast(domainattributes.taxYear as int) AS tax_year,
  domainattributes.originalproductfamily,
  domainAttributes.productFamily AS productFamily,
  explode_outer(domainattributes.segments) as pricing_segments,
  domainattributes.pricingprioritycode,
  cast(from_unixtime(systemInfo.version/1000000) as timestamp) as event_datetime,
  rank() over (
      partition by domainAttributes.ownerId
      order by systeminfo.version asc
    ) as min_rank,
  rank() over (
      partition by domainAttributes.ownerId
      order by systeminfo.version desc
    ) as max_rank
FROM serde_dwh.cg_cgcommerce_idp_evts_prd_7216_tax__tax_commerce_user1716878411_1 tc
WHERE 1 = 1
  and domainAttributes.taxYear = 2023
  and tc.year = 2024
  and (tc.month >= 05) AND (tc.month <= 06) 
  and tc.domainattributes.productfamily IS NOT NULL
  and tc.domainattributes.segments is not null;



-- COMMAND ----------

-- DBTITLE 1,2024-July-Aug

CREATE TABLE cgan_ustax_ws.commerce_pricing_segment_ty23_7_8 as
Select
  domainAttributes.ownerId AS auth_id,
  cast(domainattributes.taxYear as int) AS tax_year,
  domainattributes.originalproductfamily,
  domainAttributes.productFamily AS productFamily,
  explode_outer(domainattributes.segments) as pricing_segments,
  domainattributes.pricingprioritycode,
  cast(from_unixtime(systemInfo.version/1000000) as timestamp) as event_datetime,
  rank() over (
      partition by domainAttributes.ownerId
      order by systeminfo.version asc
    ) as min_rank,
  rank() over (
      partition by domainAttributes.ownerId
      order by systeminfo.version desc
    ) as max_rank
FROM serde_dwh.cg_cgcommerce_idp_evts_prd_7216_tax__tax_commerce_user1716878411_1 tc
WHERE 1 = 1
  and domainAttributes.taxYear = 2023
  and tc.year = 2024 
  and (tc.month >= 07) AND (tc.month <= 08) 
  and tc.domainattributes.productfamily IS NOT NULL
  and tc.domainattributes.segments is not null;

  

-- COMMAND ----------

-- DBTITLE 1,2024-Sept-Oct

CREATE TABLE cgan_ustax_ws.commerce_pricing_segment_ty23_9_10 as
Select
  domainAttributes.ownerId AS auth_id,
  cast(domainattributes.taxYear as int) AS tax_year,
  domainattributes.originalproductfamily,
  domainAttributes.productFamily AS productFamily,
  explode_outer(domainattributes.segments) as pricing_segments,
  domainattributes.pricingprioritycode,
  cast(from_unixtime(systemInfo.version/1000000) as timestamp) as event_datetime,
  rank() over (
      partition by domainAttributes.ownerId
      order by systeminfo.version asc
    ) as min_rank,
  rank() over (
      partition by domainAttributes.ownerId
      order by systeminfo.version desc
    ) as max_rank
FROM serde_dwh.cg_cgcommerce_idp_evts_prd_7216_tax__tax_commerce_user1716878411_1 tc
WHERE 1 = 1
  and domainAttributes.taxYear = 2023
  and tc.year = 2024 
  and (tc.month >= 09) AND (tc.month <= 10) 
  and tc.domainattributes.productfamily IS NOT NULL
  and tc.domainattributes.segments is not null;

-- COMMAND ----------

-- DBTITLE 1,2024-Nov
CREATE TABLE cgan_ustax_ws.commerce_pricing_segment_ty23_11 as
Select
  domainAttributes.ownerId AS auth_id,
  cast(domainattributes.taxYear as int) AS tax_year,
  domainattributes.originalproductfamily,
  domainAttributes.productFamily AS productFamily,
  explode_outer(domainattributes.segments) as pricing_segments,
  domainattributes.pricingprioritycode,
  cast(from_unixtime(systemInfo.version/1000000) as timestamp) as event_datetime,
  rank() over (
      partition by domainAttributes.ownerId
      order by systeminfo.version asc
    ) as min_rank,
  rank() over (
      partition by domainAttributes.ownerId
      order by systeminfo.version desc
    ) as max_rank
FROM serde_dwh.cg_cgcommerce_idp_evts_prd_7216_tax__tax_commerce_user1716878411_1 tc
WHERE 1 = 1
  and domainAttributes.taxYear = 2023
  and tc.year = 2024
  and (tc.month = 11)
  and tc.domainattributes.productfamily IS NOT NULL
  and tc.domainattributes.segments is not null;


-- COMMAND ----------

-- DBTITLE 1,UNION - 2023
CREATE TABLE cgan_ustax_ws.commerce_pricing_segment_ty23 as
select tax_year
  , auth_id
  , productFamily
  , pricing_segments.segment as pricing_segment
  , event_datetime
  , min_rank
  , max_rank
From cgan_ustax_ws.commerce_pricing_segment_ty23_12
WHERE (min_rank = 1 or max_rank = 1)
Group by 1,2,3,4,5,6,7

union  

select tax_year
  , auth_id
  , productFamily
  , pricing_segments.segment as pricing_segment
  , event_datetime
  , min_rank
  , max_rank
From cgan_ustax_ws.commerce_pricing_segment_ty23_1_2
WHERE (min_rank = 1 or max_rank = 1)
Group by 1,2,3,4,5,6,7

union  

select tax_year
  , auth_id
  , productFamily
  , pricing_segments.segment as pricing_segment
  , event_datetime
  , min_rank
  , max_rank
From cgan_ustax_ws.commerce_pricing_segment_ty23_3_4
WHERE (min_rank = 1 or max_rank = 1)
Group by 1,2,3,4,5,6,7

union  

select tax_year
  , auth_id
  , productFamily
  , pricing_segments.segment as pricing_segment
  , event_datetime
  , min_rank
  , max_rank
From cgan_ustax_ws.commerce_pricing_segment_ty23_5_6
WHERE (min_rank = 1 or max_rank = 1)
Group by 1,2,3,4,5,6,7

union  

select tax_year
  , auth_id
  , productFamily
  , pricing_segments.segment as pricing_segment
  , event_datetime
  , min_rank
  , max_rank
From cgan_ustax_ws.commerce_pricing_segment_ty23_7_8
WHERE (min_rank = 1 or max_rank = 1)
Group by 1,2,3,4,5,6,7

union  

select tax_year
  , auth_id
  , productFamily
  , pricing_segments.segment as pricing_segment
  , event_datetime
  , min_rank
  , max_rank
From cgan_ustax_ws.commerce_pricing_segment_ty23_9_10
WHERE (min_rank = 1 or max_rank = 1)
Group by 1,2,3,4,5,6,7

union  

select tax_year
  , auth_id
  , productFamily
  , pricing_segments.segment as pricing_segment
  , event_datetime
  , min_rank
  , max_rank
From cgan_ustax_ws.commerce_pricing_segment_ty23_11
WHERE (min_rank = 1 or max_rank = 1)
Group by 1,2,3,4,5,6,7

-- COMMAND ----------

-- DBTITLE 1,1 auth 2 rows - 2023
CREATE TABLE cgan_ustax_ws.commerce_pricing_segment_ty23_v2 as

SELECT
      tax_year,
      auth_id,
      productFamily,
      pricing_segment,
      event_datetime,

      ROW_NUMBER() OVER (
          PARTITION BY auth_id, tax_year
          ORDER BY event_datetime ASC
      ) AS min_rank,
      
      ROW_NUMBER() OVER (
          PARTITION BY auth_id, tax_year
          ORDER BY event_datetime DESC
      ) AS max_rank
  FROM cgan_ustax_ws.commerce_pricing_segment_ty23 ;

-- COMMAND ----------

-- DBTITLE 1,2023
CREATE table if not exists cgan_ustax_ws.commerce_pricing_segment_ty23 as
SELECT 
    tax_year,
    auth_id,
    productFamily,
    pricing_segment,
    event_datetime,
    min_rank,
    max_rank
FROM commerce_pricing_segment_ty23_v2
WHERE min_rank = 1 OR max_rank = 1

-- COMMAND ----------

CREATE table if not exists cgan_ustax_ws.commerce_pricing_segment_ty23_24 as
select 
    tax_year,
    auth_id,
    productFamily,
    pricing_segment,
    event_datetime,
    min_rank,
    max_rank
from cgan_ustax_ws.commerce_pricing_segment_ty23

union all 

select 
    tax_year,
    auth_id,
    productFamily,
    pricing_segment,
    event_datetime,
    min_rank,
    max_rank
from cgan_ustax_ws.commerce_pricing_segment_ty24

-- COMMAND ----------

-- DBTITLE 1,Commerce Pricing Segment Pipeline - 2023, 24, 25
-- Dec 24 - Apr 25
-- Dec 24 - Oct 25 
-- Until today
-- Write the goal of the query
-- Check if auth id has only 2 rows -> min_rank, max_rank


DROP VIEW IF EXISTS commerce_pricing_segment_ty24raw2;
CREATE TEMP VIEW commerce_pricing_segment_ty24raw2 as
Select
  domainAttributes.ownerId AS auth_id,
  cast(domainattributes.taxYear as int) AS tax_year,
  domainattributes.originalproductfamily,
  domainAttributes.productFamily AS productFamily,
  explode_outer(domainattributes.segments) as pricing_segments,
  domainattributes.pricingprioritycode,
  cast(from_unixtime(systemInfo.version/1000000) as timestamp) as event_datetime,
  rank() over (
      partition by domainAttributes.ownerId
      order by systeminfo.version asc
    ) as min_rank,
  rank() over (
      partition by domainAttributes.ownerId
      order by systeminfo.version desc
    ) as max_rank
FROM serde_dwh.cg_cgcommerce_idp_evts_prd_7216_tax__tax_commerce_user1716878411_1 tc
WHERE 1 = 1
  and domainAttributes.taxYear = 2024
  and tc.year = 2024 
  and (tc.month = 12)
  and tc.domainattributes.productfamily IS NOT NULL
  and tc.domainattributes.segments is not null;


DROP VIEW IF EXISTS commerce_pricing_segment_ty24raw;
CREATE TEMP VIEW commerce_pricing_segment_ty24raw as
Select
  domainAttributes.ownerId AS auth_id,
  cast(domainattributes.taxYear as int) AS tax_year,
  domainattributes.originalproductfamily,
  domainAttributes.productFamily AS productFamily,
  explode_outer(domainattributes.segments) as pricing_segments,
  domainattributes.pricingprioritycode,
  cast(from_unixtime(systemInfo.version/1000000) as timestamp) as event_datetime,
  rank() over (
      partition by domainAttributes.ownerId
      order by systeminfo.version asc
    ) as min_rank,
  rank() over (
      partition by domainAttributes.ownerId
      order by systeminfo.version desc
    ) as max_rank
FROM serde_dwh.cg_cgcommerce_idp_evts_prd_7216_tax__tax_commerce_user1716878411_1 tc
WHERE 1 = 1
  and domainAttributes.taxYear = 2024
  and tc.year = 2025 
  and (tc.month >= 01) AND (tc.month < 05) 
  and tc.domainattributes.productfamily IS NOT NULL
  and tc.domainattributes.segments is not null;

DROP VIEW IF EXISTS commerce_pricing_segment_ty24raw3;
  CREATE TEMP VIEW commerce_pricing_segment_ty24raw3 as
Select
  domainAttributes.ownerId AS auth_id,
  cast(domainattributes.taxYear as int) AS tax_year,
  domainattributes.originalproductfamily,
  domainAttributes.productFamily AS productFamily,
  explode_outer(domainattributes.segments) as pricing_segments,
  domainattributes.pricingprioritycode,
  cast(from_unixtime(systemInfo.version/1000000) as timestamp) as event_datetime,
  rank() over (
      partition by domainAttributes.ownerId
      order by systeminfo.version asc
    ) as min_rank,
  rank() over (
      partition by domainAttributes.ownerId
      order by systeminfo.version desc
    ) as max_rank
FROM serde_dwh.cg_cgcommerce_idp_evts_prd_7216_tax__tax_commerce_user1716878411_1 tc
WHERE 1 = 1
  and domainAttributes.taxYear = 2024
  and tc.year = 2025 
  and (tc.month >= 05) AND (tc.month < 8) 
  and tc.domainattributes.productfamily IS NOT NULL
  and tc.domainattributes.segments is not null;

  DROP VIEW IF EXISTS commerce_pricing_segment_ty24raw4;
  CREATE TEMP VIEW commerce_pricing_segment_ty24raw4 as
Select
  domainAttributes.ownerId AS auth_id,
  cast(domainattributes.taxYear as int) AS tax_year,
  domainattributes.originalproductfamily,
  domainAttributes.productFamily AS productFamily,
  explode_outer(domainattributes.segments) as pricing_segments,
  domainattributes.pricingprioritycode,
  cast(from_unixtime(systemInfo.version/1000000) as timestamp) as event_datetime,
  rank() over (
      partition by domainAttributes.ownerId
      order by systeminfo.version asc
    ) as min_rank,
  rank() over (
      partition by domainAttributes.ownerId
      order by systeminfo.version desc
    ) as max_rank
FROM serde_dwh.cg_cgcommerce_idp_evts_prd_7216_tax__tax_commerce_user1716878411_1 tc
WHERE 1 = 1
  and domainAttributes.taxYear = 2024
  and tc.year = 2025 
  and (tc.month >= 08) AND (tc.month < 11) 
  and tc.domainattributes.productfamily IS NOT NULL
  and tc.domainattributes.segments is not null;


-- Dec 23 - Apr 24 
-- Nov 24

DROP VIEW IF EXISTS commerce_pricing_segment_ty23raw;
CREATE TEMP VIEW commerce_pricing_segment_ty23raw as
Select
  domainAttributes.ownerId AS auth_id,
  cast(domainattributes.taxYear as int) AS tax_year,
  domainattributes.originalproductfamily,
  domainAttributes.productFamily AS productFamily,
  explode_outer(domainattributes.segments) as pricing_segments,
  domainattributes.pricingprioritycode,
  cast(from_unixtime(systemInfo.version/1000000) as timestamp) as event_datetime,
  rank() over (
      partition by domainAttributes.ownerId
      order by systeminfo.version asc
    ) as min_rank,
  rank() over (
      partition by domainAttributes.ownerId
      order by systeminfo.version desc
    ) as max_rank
FROM serde_dwh.cg_cgcommerce_idp_evts_prd_7216_tax__tax_commerce_user1716878411_1 tc
WHERE 1 = 1
  and domainAttributes.taxYear = 2023
  and ((tc.year = 2023 AND tc.month = 12) OR (tc.year = 2024 and tc.month >= 01 AND tc.month < 03))
  and tc.domainattributes.productfamily IS NOT NULL
  and tc.domainattributes.segments is not null;

DROP VIEW IF EXISTS commerce_pricing_segment_ty23raw2;
CREATE TEMP VIEW commerce_pricing_segment_ty23raw2 as
Select
  domainAttributes.ownerId AS auth_id,
  cast(domainattributes.taxYear as int) AS tax_year,
  domainattributes.originalproductfamily,
  domainAttributes.productFamily AS productFamily,
  explode_outer(domainattributes.segments) as pricing_segments,
  domainattributes.pricingprioritycode,
  cast(from_unixtime(systemInfo.version/1000000) as timestamp) as event_datetime,
  rank() over (
      partition by domainAttributes.ownerId
      order by systeminfo.version asc
    ) as min_rank,
  rank() over (
      partition by domainAttributes.ownerId
      order by systeminfo.version desc
    ) as max_rank
FROM serde_dwh.cg_cgcommerce_idp_evts_prd_7216_tax__tax_commerce_user1716878411_1 tc
WHERE 1 = 1
  and domainAttributes.taxYear = 2023
  and (tc.year = 2024 and tc.month >= 03 AND tc.month < 7)
  and tc.domainattributes.productfamily IS NOT NULL
  and tc.domainattributes.segments is not null;

  DROP VIEW IF EXISTS commerce_pricing_segment_ty23raw3;
CREATE TEMP VIEW commerce_pricing_segment_ty23raw3 as
Select
  domainAttributes.ownerId AS auth_id,
  cast(domainattributes.taxYear as int) AS tax_year,
  domainattributes.originalproductfamily,
  domainAttributes.productFamily AS productFamily,
  explode_outer(domainattributes.segments) as pricing_segments,
  domainattributes.pricingprioritycode,
  cast(from_unixtime(systemInfo.version/1000000) as timestamp) as event_datetime,
  rank() over (
      partition by domainAttributes.ownerId
      order by systeminfo.version asc
    ) as min_rank,
  rank() over (
      partition by domainAttributes.ownerId
      order by systeminfo.version desc
    ) as max_rank
FROM serde_dwh.cg_cgcommerce_idp_evts_prd_7216_tax__tax_commerce_user1716878411_1 tc
WHERE 1 = 1
  and domainAttributes.taxYear = 2023
  and (tc.year = 2024 and tc.month >= 07 AND tc.month < 12)
  and tc.domainattributes.productfamily IS NOT NULL
  and tc.domainattributes.segments is not null;


CREATE table if not exists cgan_ustax_ws.commerce_pricing_segment_ty23_24 as
select tax_year
  , auth_id
  , productFamily
  , pricing_segments.segment as pricing_segment
  , event_datetime
  , min_rank
  , max_rank
From commerce_pricing_segment_ty24raw
WHERE (min_rank = 1 or max_rank = 1)
Group by 1,2,3,4,5,6,7

Union all
select tax_year
  , auth_id
  , productFamily
  , pricing_segments.segment as pricing_segment
  , event_datetime
  , min_rank
  , max_rank
From commerce_pricing_segment_ty24raw2
WHERE (min_rank = 1 or max_rank = 1)
Group by 1,2,3,4,5,6,7

Union all
select tax_year
  , auth_id
  , productFamily
  , pricing_segments.segment as pricing_segment
  , event_datetime
  , min_rank
  , max_rank
From commerce_pricing_segment_ty24raw3
WHERE (min_rank = 1 or max_rank = 1)
Group by 1,2,3,4,5,6,7

Union all
select tax_year
  , auth_id
  , productFamily
  , pricing_segments.segment as pricing_segment
  , event_datetime
  , min_rank
  , max_rank
From commerce_pricing_segment_ty24raw4
WHERE (min_rank = 1 or max_rank = 1)
Group by 1,2,3,4,5,6,7

Union all
select tax_year
  , auth_id
  , productFamily
  , pricing_segments.segment as pricing_segment
  , event_datetime
  , min_rank
  , max_rank
From commerce_pricing_segment_ty23raw
WHERE (min_rank = 1 or max_rank = 1)
Group by 1,2,3,4,5,6,7

Union all
select tax_year
  , auth_id
  , productFamily
  , pricing_segments.segment as pricing_segment
  , event_datetime
  , min_rank
  , max_rank
From commerce_pricing_segment_ty23raw2
WHERE (min_rank = 1 or max_rank = 1)
Group by 1,2,3,4,5,6,7

Union all
select tax_year
  , auth_id
  , productFamily
  , pricing_segments.segment as pricing_segment
  , event_datetime
  , min_rank
  , max_rank
From commerce_pricing_segment_ty23raw3
WHERE (min_rank = 1 or max_rank = 1)
Group by 1,2,3,4,5,6,7
  

-- COMMAND ----------

-- DBTITLE 1,auth_pricing_segment_base

-- Check if auth id has only 2 rows -> start or complete
-- Write the goal of the query
-- Identify auths with completes but no start

drop table if exists cgan_ustax_ws.auth_pricing_segment_base;
create table if not exists cgan_ustax_ws.auth_pricing_segment_base as

Select a.tax_year
  , 'complete' as metric_type
  , p.first_completed_date as event_datetime
  , a.auth_id
  , a.pricing_segment
  , p.completed_sku as sku
from cgan_ustax_ws.commerce_pricing_segment_ty23_24 a
join tax_rpt.product_analytics_master as p
  on a.tax_year = p.tax_year
  and a.auth_id = p.auth_id
  and p.first_completed_date is not null 
  and p.completed_sku is not null
    WHERE a.pricing_segment is not null
      and a.max_rank = 1
GROUP BY 1,2,3,4,5,6

Union all

Select a.tax_year
  , 'start' as metric_type
  , p.first_start_date as event_datetime
  , a.auth_id
  , a.pricing_segment
  , p.start_sku as sku
from cgan_ustax_ws.commerce_pricing_segment_ty23_24 a
join tax_rpt.product_analytics_master as p
  on a.tax_year = p.tax_year
  and a.auth_id = p.auth_id
  and p.first_start_date is not null 
  and p.start_sku is not null
  WHERE a.pricing_segment is not null
    and a.min_rank = 1
GROUP BY 1,2,3,4,5,6

-- COMMAND ----------


-- Write the goal of the query
create TEMP VIEW sku_list_price 
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
;


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
      case when a.metric_type = 'complete' then p.customer_type_rollup else p.tto_segment_rollup end as new_returning

  FROM tax_rpt.agg_marketing_analytics a
   LEFT JOIN tax_rpt.product_analytics_master p
    ON a.tax_year = p.tax_year
    AND a.metric_id = p.auth_id
  WHERE a.tax_year IN ('2024','2023')
    AND a.metric_type IN ('start', 'complete')
),

-- pcode, sku -> list_price, sale_price
-- Validate again 1 row per sku for every auth
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
      pcmap.discount_type,
      'pcode' as promotion_type
  FROM cgan_ustax_published.pcode_promo_item_mapping pcmap
  JOIN tax_dm.dim_sku_rollup sr 
    ON pcmap.sku_id = sr.sku_id
  WHERE DATE(pcmap.promotion_price_start_date) >= '2023-12-01'
    AND pcmap.product_family_description LIKE '%FEDERAL%'
    AND display_sku_name_with_sort not like '%FULL SERVICE%'
    --- AND discount_value > 0
    AND pcmap.priority_code IS NOT NULL 
    AND pcmap.pricing_segment IS NULL 

),


-- auth_id, tax_year, pricing segment, sku -> list_price, sale_price
-- Observed many nulls for the pricing segment. Not sure if the table is incomplete
pricing_segment_base AS (
  SELECT 
      a.tax_year,
      a.auth_id,
      a.metric_type,
      a.pricing_segment,
      b.sku_id,
      a.sku,
      b.promotion_objective,
      DATE(b.promotion_price_start_date) AS promotion_price_start_date,
      DATE(b.promotion_price_end_date) AS promotion_price_end_date,
      b.list_price,
      b.sale_price,
      b.discount_value,
      b.discount_type,
      'pricing segment' as promotion_type
  FROM cgan_ustax_ws.auth_pricing_segment_base a
  INNER JOIN (SELECT pcmap.*, sr.display_sku_name_with_sort
            FROM cgan_ustax_published.pcode_promo_item_mapping pcmap
            LEFT JOIN tax_dm.dim_sku_rollup sr 
            ON pcmap.sku_id = sr.sku_id
            ) b
              ON a.pricing_segment = b.pricing_segment
              AND a.sku = b.display_sku_name_with_sort
              AND DATE(b.promotion_price_start_date) >= '2023-12-01'
              AND display_sku_name_with_sort not like '%FULL SERVICE%'
              AND b.product_family_description LIKE '%FEDERAL%'
              AND a.event_datetime BETWEEN date_sub(b.promotion_price_start_date,1) and b.promotion_price_end_date
              --- AND b.discount_value > 0
              AND b.priority_code IS NULL
              AND b.pricing_segment IS NOT NULL

),


-- Validate 1 auth has one row

pricing_ops_base as (
    select tax_year, 
            auth_id, 
            order_date,
            display_sku_name_with_sort as sku,
            appliedPcode_promotion_name as promotion_objective,
            coalesce(creditForSalePCode, segment_opa, arcc_vcoucher) as promotion_code,
            case when creditForSalePCode is not null then 'pcode'
                 when segment_opa is not null then 'pricing segment'
                 when voucher_category is not null or arcc_vcoucher is not null then 'voucher' end as promotion_type
            -- arcc_vcoucher,
            list_unit_price as list_price,	
            sale_unit_price as sale_price
from
(select pricing_ops.*, pcmap.product_family_description, pcmap.sku_id
    from mse_published.ssingha_cg_promotion_performance_ty17_ty24_detail as pricing_ops
    left join cgan_ustax_published.pcode_promo_item_mapping pcmap
    on pricing_ops.Offer_Id_or_sku = pcmap.offer_number)as a 
    where product_family_description like '%FEDERAL%'
     and tax_year IN ('2024','2023')
     and (creditForSalePCode is not null
     or segment_opa is not null
     or voucher_category is not null
     or arcc_vcoucher is not null)

    --  To be confirmed by SAM for the right values
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
      a.new_returning,


      CASE WHEN a.metric_type = 'complete' then COALESCE(c.pricing_segment, a.price_priority_code, s.promotion_code)
           WHEN  a.metric_type = 'start' THEN COALESCE(c.pricing_segment, a.price_priority_code) END AS promotion_code,

      CASE WHEN a.metric_type = 'complete' then COALESCE(c.promotion_objective, b.promotion_objective, s.promotion_objective) 
           WHEN a.metric_type = 'start' THEN COALESCE(c.promotion_objective, b.promotion_objective) END AS  promotion_objective
  

      CASE WHEN a.metric_type = 'complete' then COALESCE(c.promotion_type, b.promotion_type, s.promotion_type,
              CASE WHEN d.list_price != m.total_federal_revenue THEN 'unknown discount' END) 
           WHEN a.metric_type = 'start' then COALESCE(c.promotion_type, b.promotion_type) END AS promotion_type
-- Add SHAM  as third parameter in start
      

      CASE WHEN a.metric_type = 'complete' THEN COALESCE(d.list_price, b.list_price, c.list_prcie, s.list_price) 
           WHEN a.metric_type = 'start' THEN COALESCE(d.list_price, b.list_price, c.list_prcie) END AS list_price,

      CASE WHEN a.metric_type = 'complete'  THEN COALESCE(m.total_federal_revenue, b.sale_price, c.sale_prcie, s.sale_price)
           WHEN a.metric_type = 'start' THEN COALESCE(b.sale_price, c.sale_price) END AS sale_price

          --  Third parameter would be sham.price_recommended,



  FROM ama_base a
  LEFT JOIN pcode_base b
    ON a.price_priority_code = b.priority_code
    AND a.sku = b.display_sku_name_with_sort
    AND a.event_datetime BETWEEN date_sub(b.promotion_price_start_date,1) and b.promotion_price_end_date


  LEFT JOIN pricing_segment_base c
    ON a.tax_year = c.tax_year
    AND a.auth_id = c.auth_id
    AND a.metric_type = c.metric_type
    AND a.sku = c.sku
      AND a.event_date BETWEEN date_sub(c.promotion_price_start_date,1) and c.promotion_price_end_date

  LEFT JOIN sam
      on a.tax_year = sam.tax_year
      AND a.auth_id = sam.auth_id
      and a.sku = sam.display_sku_name_with_sort
      and a.event_date = sam.order_date


  LEFT JOIN tax_rpt.monetization_analytics_master m
    ON a.auth_id = m.auth_id
    AND a.tax_year = m.tax_year

  LEFT JOIN sku_list_price d
    ON a.tax_year = d.tax_year
    AND TRIM(LOWER(a.sku)) = TRIM(LOWER(d.sku))
    AND month(a.event_date) = d.price_start_month
  
)



SELECT *, 
    CASE WHEN list_price IS NULL OR sale_price IS NULL THEN NULL ELSE list_price - sale_price END AS discount_value,
    CASE WHEN list_price IS NULL OR sale_price IS NULL THEN NULL 
         WHEN list_price = 0 THEN 0
      ELSE ROUND((list_price - sale_price) / (list_price), 2) END AS discount_per,

FROM discount_base_table
ORDER BY 
    tax_year, auth_id, metric_type, event_date;

-- COMMAND ----------

-- DBTITLE 1,Target State: discount_segments_base


create TEMP VIEW sku_list_price 
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
;


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

-- pcode, sku -> list_price, sale_price

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
      pcmap.discount_type,
      'pcode' as promotion_type
  FROM cgan_ustax_published.pcode_promo_item_mapping pcmap
  JOIN tax_dm.dim_sku_rollup sr 
    ON pcmap.sku_id = sr.sku_id
  WHERE DATE(pcmap.promotion_price_start_date) >= '2023-12-01'
    AND pcmap.product_family_description LIKE '%FEDERAL%'
    AND display_sku_name_with_sort not like '%FULL SERVICE%'
    --- AND discount_value > 0
    AND pcmap.priority_code IS NOT NULL 
    AND pcmap.pricing_segment IS NULL 

),



auth_id, tax_year, pricing segment, sku -> list_price, sale_price
-- Observed many nulls for the pricing segment. Not sure if the table is incomplete
pricing_segment_base AS (
  SELECT 
      a.tax_year,
      a.auth_id,
      a.metric_type,
      a.pricing_segment,
      b.sku_id,
      a.sku,
      b.promotion_objective,
      DATE(b.promotion_price_start_date) AS promotion_price_start_date,
      DATE(b.promotion_price_end_date) AS promotion_price_end_date,
      b.list_price,
      b.sale_price,
      b.discount_value,
      b.discount_type,
      'pricing segment' as promotion_type
  FROM cgan_ustax_ws.auth_pricing_segment_base a
  INNER JOIN (SELECT pcmap.*, sr.display_sku_name_with_sort
            FROM cgan_ustax_published.pcode_promo_item_mapping pcmap
            LEFT JOIN tax_dm.dim_sku_rollup sr 
            ON pcmap.sku_id = sr.sku_id
            ) b
              ON a.pricing_segment = b.pricing_segment
              AND a.sku = b.display_sku_name_with_sort
              AND DATE(b.promotion_price_start_date) >= '2023-12-01'
              AND display_sku_name_with_sort not like '%FULL SERVICE%'
              AND b.product_family_description LIKE '%FEDERAL%'
              AND a.event_datetime BETWEEN date_sub(b.promotion_price_start_date,1) and b.promotion_price_end_date
              --- AND b.discount_value > 0
              AND b.priority_code IS NULL
              AND b.pricing_segment IS NOT NULL

),

sam as (
    select tax_year, 
            auth_id, 
            order_date,
            appliedPcode_promotion_name,
            display_sku_name_with_sort,
            appliedPcode_promotion_name as promotion_objective,
            creditForSalePCode as promotion_code,
            case when creditForSalePCode is not null then 'pcode'
                 when segment_opa is not null then 'pricing segment'
                 when voucher_category is not null or arcc_vcoucher is not null then 'voucher' end as promotion_type
            -- arcc_vcoucher,
            list_unit_price,	
            sale_unit_price
from
(select sam.*, pcmap.product_family_description, pcmap.sku_id
    from mse_published.ssingha_cg_promotion_performance_ty17_ty24_detail as sam
    left join cgan_ustax_published.pcode_promo_item_mapping pcmap
    on sam.Offer_Id_or_sku = pcmap.offer_number)as a 
    where product_family_description like '%FEDERAL%'
     and tax_year IN ('2024','2023')
    --  and creditForSalePCode is null
    --  and segment_opa is null
    --  and (voucher_category is not null
    --  or arcc_vcoucher is not null)

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
      case when  a.metric_type = 'start' then a.tto_segment_rollup 
           when a.metric_type = 'complete' then a.customer_type_rollup else a.tto_segment_rollup end as new_returning

-- 142, 143 to be changed after bringing in SAM's table
      COALESCE(c.pricing_segment, a.price_priority_code) AS promotion_code,
      COALESCE(c.promotion_objective, b.promotion_objective) AS promotion_objective,


-- Case statement applies only when metric_type = 'complete'. Nothing mentioned for metric_type = 'start'
-- Why the 1st case when?
      CASE WHEN a.metric_type = 'complete' and (m.total_federal_revenue <= d.list_price OR lower(b.promotion_objective) like '%willingness to pay model%') THEN d.list_price
            WHEN a.metric_type = 'complete' and m.total_federal_revenue > d.list_price THEN m.total_federal_revenue
            WHEN c.list_price IS NOT NULL THEN c.list_price
            WHEN b.list_price IS NOT NULL THEN b.list_price
            ELSE d.list_price END AS list_price,

      CASE WHEN a.metric_type = 'complete' THEN m.total_federal_revenue
          --  WHEN c.pricing_segment = 'FREEMOBILE' and c.sku_id IN (14, 16, 128) then 0
            WHEN c.sale_price IS NOT NULL THEN c.sale_price
            WHEN b.sale_price IS NOT NULL THEN b.sale_price
            ELSE NULL END AS sale_price,

      CASE WHEN a.metric_type = 'complete' THEN m.total_federal_revenue ELSE NULL END AS total_federal_revenue   

  FROM ama_base a
  LEFT JOIN pcode_base b
    ON a.price_priority_code = b.priority_code
    AND a.sku = b.display_sku_name_with_sort
    -- AND a.event_date BETWEEN b.promotion_price_start_date AND b.promotion_price_end_date
    AND a.event_datetime BETWEEN date_sub(b.promotion_price_start_date,1) and b.promotion_price_end_date


-- This will only pull in auths that are a part of the agg_marketing_analytics table
  LEFT JOIN pricing_segment_base c
    ON a.tax_year = c.tax_year
    AND a.auth_id = c.auth_id
    AND a.metric_type = c.metric_type
    AND a.sku = c.sku
      AND a.event_date BETWEEN date_sub(c.promotion_price_start_date,1) and c.promotion_price_end_date

      LEFT JOIN sam
      on a.tax_year = sam.tax_year
      AND a.auth_id = sam.auth_id
      and a.sku = sam.display_sku_name_with_sort


  LEFT JOIN tax_rpt.monetization_analytics_master m
    ON a.auth_id = m.auth_id
    AND a.tax_year = m.tax_year

  LEFT JOIN sku_list_price d
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

-- MAGIC %md
-- MAGIC Specific Cohorts
