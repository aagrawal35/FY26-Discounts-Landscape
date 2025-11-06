-- Databricks notebook source
-- DBTITLE 1,Commerce Pricing Segment Pipeline - 2025
-- Dec 25 - Oct 26
-- This query has two parts: first, it creates  temporary views for each month of tax year 2025 containing data such as auth_id, tax_year, pricing_segment, productFamily, and event_datetime; second, it creates a final table by unioning all these monthly views to capture only the earliest and latest event records for each auth ID.

DROP VIEW IF EXISTS commerce_pricing_segment_ty25_12;
CREATE TEMP VIEW commerce_pricing_segment_ty25_12 as
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
  and domainAttributes.taxYear = 2025
  and tc.year = 2025 
  and (tc.month = 12)
  and tc.domainattributes.productfamily IS NOT NULL
  and tc.domainattributes.segments is not null;

DROP VIEW IF EXISTS commerce_pricing_segment_ty25_1_2;
CREATE TEMP VIEW commerce_pricing_segment_ty25_1_2 as
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
  and domainAttributes.taxYear = 2025
  and tc.year = 2026 
  and (tc.month >= 01) AND (tc.month <= 02) 
  and tc.domainattributes.productfamily IS NOT NULL
  and tc.domainattributes.segments is not null;

DROP VIEW IF EXISTS commerce_pricing_segment_ty25_3_4;
CREATE TEMP VIEW commerce_pricing_segment_ty25_3_4 as
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
  and domainAttributes.taxYear = 2025
  and tc.year = 2026
  and (tc.month >= 03) AND (tc.month <= 04) 
  and tc.domainattributes.productfamily IS NOT NULL
  and tc.domainattributes.segments is not null;

DROP VIEW IF EXISTS commerce_pricing_segment_ty25_5_6;
CREATE TEMP VIEW commerce_pricing_segment_ty25_5_6 as
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
  and domainAttributes.taxYear = 2025
  and tc.year = 2026
  and (tc.month >= 05) AND (tc.month <= 06) 
  and tc.domainattributes.productfamily IS NOT NULL
  and tc.domainattributes.segments is not null;

DROP VIEW IF EXISTS commerce_pricing_segment_ty25_7_8;
CREATE TEMP VIEW commerce_pricing_segment_ty25_7_8 as
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
  and domainAttributes.taxYear = 2025
  and tc.year = 2026
  and (tc.month >= 07) AND (tc.month <= 08) 
  and tc.domainattributes.productfamily IS NOT NULL
  and tc.domainattributes.segments is not null;

  DROP VIEW IF EXISTS commerce_pricing_segment_ty25_9_10;
CREATE TEMP VIEW commerce_pricing_segment_ty25_9_10 as
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
  and domainAttributes.taxYear = 2025
  and tc.year = 2026
  and (tc.month >= 09) AND (tc.month <= 10) 
  and tc.domainattributes.productfamily IS NOT NULL
  and tc.domainattributes.segments is not null;

DROP VIEW IF EXISTS commerce_pricing_segment_ty25_v;
CREATE TEMP VIEW commerce_pricing_segment_ty25_v as
select tax_year
  , auth_id
  , productFamily
  , pricing_segments.segment as pricing_segment
  , event_datetime
  , min_rank
  , max_rank
From commerce_pricing_segment_ty25_12
WHERE (min_rank = 1 or max_rank = 1)
Group by 1,2,3,4,5,6,7

Union 
select tax_year
  , auth_id
  , productFamily
  , pricing_segments.segment as pricing_segment
  , event_datetime
  , min_rank
  , max_rank
From commerce_pricing_segment_ty25_1_2
WHERE (min_rank = 1 or max_rank = 1)
Group by 1,2,3,4,5,6,7

Union 
select tax_year
  , auth_id
  , productFamily
  , pricing_segments.segment as pricing_segment
  , event_datetime
  , min_rank
  , max_rank
From commerce_pricing_segment_ty25_3_4
WHERE (min_rank = 1 or max_rank = 1)
Group by 1,2,3,4,5,6,7


Union
select tax_year
  , auth_id
  , productFamily
  , pricing_segments.segment as pricing_segment
  , event_datetime
  , min_rank
  , max_rank
From commerce_pricing_segment_ty25_5_6
WHERE (min_rank = 1 or max_rank = 1)
Group by 1,2,3,4,5,6,7

Union
select tax_year
  , auth_id
  , productFamily
  , pricing_segments.segment as pricing_segment
  , event_datetime
  , min_rank
  , max_rank
From commerce_pricing_segment_ty25_7_8
WHERE (min_rank = 1 or max_rank = 1)
Group by 1,2,3,4,5,6,7

Union
select tax_year
  , auth_id
  , productFamily
  , pricing_segments.segment as pricing_segment
  , event_datetime
  , min_rank
  , max_rank
From commerce_pricing_segment_ty25_9_10
WHERE (min_rank = 1 or max_rank = 1)
Group by 1,2,3,4,5,6,7;


DROP VIEW IF EXISTS commerce_pricing_segment_ty25_v2;
CREATE TEMP VIEW commerce_pricing_segment_ty25_v2 as

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
  FROM commerce_pricing_segment_ty25_v;

CREATE table if not exists cgan_ustax_ws.commerce_pricing_segment_ty25 as
SELECT 
    tax_year,
    auth_id,
    productFamily,
    pricing_segment,
    event_datetime,
    min_rank,
    max_rank
FROM commerce_pricing_segment_ty25_v2
WHERE min_rank = 1 OR max_rank = 1


-- COMMAND ----------

-- DBTITLE 1,auth_pricing_segment_base
-- Query joins commerce_pricing_segments_ty23_24 with PAM to retrieve completed_sku and start_sku values from PAM for each auth and corresponding pricing_segment

drop table if exists cgan_ustax_ws.auth_pricing_segment_base_25;
create table if not exists cgan_ustax_ws.auth_pricing_segment_base_25 as

Select a.tax_year
  , 'complete' as metric_type
  , p.first_completed_date as event_datetime
  , a.auth_id
  , a.pricing_segment
  , p.completed_sku as sku
from cgan_ustax_ws.commerce_pricing_segment_ty25 a
join tax_rpt.product_analytics_master as p
  on a.tax_year = p.tax_year
  and a.auth_id = p.auth_id
  and p.first_completed_date BETWEEN (a.event_datetime - INTERVAL 1 DAY) AND (a.event_datetime + INTERVAL 1 DAY)
  and p.first_completed_date is not null 
  and p.completed_sku is not null
    WHERE a.pricing_segment is not null
      -- and a.max_rank = 1
GROUP BY 1,2,3,4,5,6

Union all

Select a.tax_year
  , 'start' as metric_type
  , p.first_start_date as event_datetime
  , a.auth_id
  , a.pricing_segment
  , p.start_sku as sku
from cgan_ustax_ws.commerce_pricing_segment_ty25 a
join tax_rpt.product_analytics_master as p
  on a.tax_year = p.tax_year
  and a.auth_id = p.auth_id
  and p.first_start_date BETWEEN (a.event_datetime - INTERVAL 1 DAY) AND (a.event_datetime + INTERVAL 1 DAY)
  and p.first_start_date is not null 
  and p.start_sku is not null
  WHERE a.pricing_segment is not null
    -- and a.min_rank = 1
GROUP BY 1,2,3,4,5,6

-- COMMAND ----------

-- DBTITLE 1,Creating promo_mapping_source
-- Query to create promo_mapping_source table
create table if not exists cgan_ustax_published.promo_mapping_source as
WITH pcode_promo_mapping_history AS (
  SELECT
    assigned_ts,
    priority_code,
    object_type,
    pcah.object_id,
    p.promotion_number AS promotion_id,
    name AS promotion_name,
    assigned_by,
    RANK() OVER (PARTITION BY pcah.priority_code ORDER BY assigned_ts DESC) AS rw_num
  FROM
    ued_ppm_dwh.priority_code_assignment_history pcah
      INNER JOIN ued_ppm_dwh.promotion p
        ON p.id = pcah.object_id
),
pcode_promo_mapping AS (
  SELECT
    assigned_ts,
    priority_code,
    object_type,
    promotion_id,
    promotion_name,
    object_id
  FROM
    pcode_promo_mapping_history
  where
    rw_num = 1
)
select distinct
  gi.gtm_item_number,
  gic.gtm_item_component_number as offer_number,
  pec.gtm_item_component_id,
  pe.name as product_name,
  dii.prod_fmly_dsc as product_family_description,
  dii.cross_reference as item_alias_code,
  fs.base_code as base_code,
  fs.code as featureset_code,
  pc.skuNumber as sku_id,
  spfm.product_family_name,
  pecc.list_price,
  pecc.max_price,
  pecc.discount_value,
  pecc.discount_type,
  pecc.override_price,
  pecc.sale_price,
  p.name as promotion_name,
  p.description as promotion_description,
  p.target_audience,
  p.campaign_type,
  p.promotion_availability,
  p.marketing_channel,
  p.promotion_objective,
  pled.date_range_name,
  pedr.effective_start_date as promotion_price_start_date,
  pedr.effective_end_date as promotion_price_end_date,
  p.user_entered_promotion_code,
  p.status,
  ppm.promotion_id,
  ppm.priority_code,
  ps.name as pricing_segment,
  ps.description as pricing_segment_description
from
  ued_ppm_dwh.promotion_entry_component_charge_element pecc
    inner join ued_ppm_dwh.promotion_entry_component pec
      on pecc.promotion_entry_component_id = pec.id
      and (pecc.is_primary != false or pecc.is_primary is null)
    inner join ued_ppm_dwh.promotion_entry pe
      on pec.promotion_entry_id = pe.id
    left join ued_ppm_dwh.promotion_entry_date_range pedr
      on pe.promotion_entry_date_range_id = pedr.id
    left join ued_ppm_dwh.price_list_entry_date_range pled
      on pedr.price_list_entry_date_range_id = pled.id
    inner join ued_ppm_dwh.promotion p
      on pe.promotion_id = p.id
    left join ued_ppm_dwh.pricing_segment_promotion psp
      on p.id = psp.promotion_id
    left join ued_ppm_dwh.pricing_segment ps
      on psp.pricing_segment_id = ps.id
    left join pcode_promo_mapping ppm
      ON p.id = ppm.object_id
    left join ued_ppm_dwh.gtm_component_fulfillment_info gcfi
      on gcfi.gtm_item_comp_id = pec.gtm_item_component_id
    left join ued_ppm_dwh.gtm_item_component gic
      on gcfi.gtm_item_comp_id = gic.id
    left join ued_ppm_dwh.gtm_item gi
      on gic.gtm_item_id = gi.id
    left join FINANCE_DM.DIM_INTUIT_ITEM dii
      on dii.cross_reference = gcfi.item_alias
    left join ued_ppm_dwh.featuresets fs
      on gic.feature_set_id = fs.id
    left join ued_ppm_dwh.products pr
      on fs.product_id = pr.id
    left join customergrowthandengagement_productpricingandconfiguration_productcatalog_pcs_delta.mse_productcatalog_featureset_tto_us pc
      on pc.code = fs.code
    left join tax_dm.sku_product_family_mapping spfm
      on spfm.sku_id = pc.skuNumber;

-- COMMAND ----------

-- DBTITLE 1,Target State: discount_segments_base_25

-- View that generates a table retrieving list_price values by tax_year, sku, and month
DROP VIEW IF EXISTS sku_list_price;
create TEMP VIEW sku_list_price AS
select pam.tax_year
, UPPER(pam.completed_sku) as sku
, month(pam.first_completed_date) as price_start_month
, mode(mam.total_federal_revenue) as list_price
from tax_rpt.product_analytics_master pam 
join tax_rpt.monetization_analytics_master mam
  on pam.auth_id = mam.auth_id
  and pam.tax_year = mam.tax_year
join tax_rpt.agg_marketing_analytics as aggm
  on pam.auth_id = aggm.metric_id
  and pam.tax_year = aggm.tax_year
  and aggm.metric_type = 'complete'
  and aggm.price_priority_code = '3468337910' --- Default pcode
where pam.tax_year IN ('2025')
and pam.completed_sku is not null
and (pam.first_completed_date) is not null
group by 1,2,3
order by 1,2,3
;

-- Query to create a price discount master table mapping each auth_id and SKU to its promotion details and pricing.

DROP TABLE IF EXISTS cgan_ustax_ws.price_discount_master_25;
CREATE TABLE IF NOT EXISTS cgan_ustax_ws.price_discount_master_25 USING PARQUET AS

WITH ama_base AS (
  SELECT a.tax_year,
      a.metric_id AS auth_id,
      a.metric_type,
      case when a.metric_type = 'start' then p.first_start_date
          else p.first_completed_date end as event_date, ---DATE(a.date) AS event_date,
      case when a.metric_type = 'start' then p.start_sku
          else p.completed_sku end as sku, ---a.product AS sku,

      case when a.metric_type = 'start' then p.start_sku_rollup_id
          else p.completed_sku_rollup_id end as sku_rollup_id, ---a.product AS sku,

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
  WHERE a.tax_year IN ('2025')
    AND a.metric_type IN ('start', 'complete')
),


complete_pcodes as (
Select ft.tax_year
  , ft.auth_id
  , p.completed_sku as sku
  , p.completed_sku_rollup_id as sku_rollup_id
  , p.first_completed_date 
  , dpc.priority_code as complete_pcode
  , dpd.discount_value
  , dpd.list_price
  , dpd.sale_price
  , dpd.promotion_name as promotion_objective
  , 'PCODE' as promotion_type
from tax_dm.fact_taxorder ft 
join tax_rpt.product_analytics_master p 
on p.tax_year = ft.tax_year 
and p.auth_id = ft.auth_id 
and p.first_completed_date = ft.order_timestamp 

join tax_dm.dim_product_alias dpa 
on ft.product_alias_id = dpa.product_alias_id

join tax_dm.dim_product dp 
on dpa.product_id = dp.product_id 
and dp.product_family_description like '%FEDERAL%'

join tax_dm.dim_priority_code dpc 
on ft.price_priority_id = dpc.priority_id

left join cgan_ustax_published.promo_mapping_source dpd 
    on dpc.priority_code = dpd.priority_code 
    and dpa.item_alias_code = dpd.item_alias_code
    and date(ft.order_timestamp) between dpd.promotion_price_start_date and dpd.promotion_price_end_date
    and dpd.product_family_description like '%FEDERAL%'

  
Where ft.tax_year in (2025) and ft.order_amount is not null
and p.first_completed_date_adj is not null and p.completed_sku is not null

Group by 1,2,3,4,5,6,7,8,9, 10,11
Order by 1,2,3,4,5,6,7,8,9, 10,11

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
      sr.sku_rollup_id,
      'PCODE' as promotion_type
  FROM cgan_ustax_published.promo_mapping_source pcmap
  JOIN tax_dm.dim_sku_rollup sr 
    ON pcmap.sku_id = sr.sku_id
  WHERE DATE(pcmap.promotion_price_start_date) >= '2025-12-01'
    AND pcmap.product_family_description LIKE '%FEDERAL%'
    AND display_sku_name_with_sort not like '%FULL SERVICE%'
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
      a.sku,
      b.sku_id,
      b.sku_rollup_id,
      max(b.promotion_objective) as promotion_objective,
      max(b.discount_type) as discount_type,
      DATE(b.promotion_price_start_date) AS promotion_price_start_date,
      DATE(b.promotion_price_end_date) AS promotion_price_end_date,
      max(b.list_price) as list_price,
      max(b.sale_price) as sale_price,
      max(b.discount_value) as discount_value,
      'PRICING_SEGMENT' as promotion_type
  FROM cgan_ustax_ws.auth_pricing_segment_base_25 a
  INNER JOIN (SELECT pcmap.*, sr.display_sku_name_with_sort, sr.sku_rollup_id
            FROM cgan_ustax_published.promo_mapping_source pcmap
            LEFT JOIN tax_dm.dim_sku_rollup sr 
            ON pcmap.sku_id = sr.sku_id
            where pcmap.promotion_name not like '%DO NOT USE%' AND pcmap.promotion_name not like '%ARCHIVE%'
            ) b
              ON a.pricing_segment = b.pricing_segment
              AND a.sku = b.display_sku_name_with_sort
              AND DATE(b.promotion_price_start_date) >= '2025-12-01'
              AND display_sku_name_with_sort not like '%FULL SERVICE%'
              AND b.product_family_description LIKE '%FEDERAL%'
              AND date(a.event_datetime) BETWEEN date(b.promotion_price_start_date) and date(date_sub(b.promotion_price_end_date,1))
              AND b.priority_code IS NULL
              AND b.pricing_segment IS NOT NULL
  GROUP BY 1,2,3,4,5,6,7,10,11,15
),


-- Validate 1 auth has one row per sku

pricing_ops_base as (
    select tax_year, 
            auth_id, 
            order_date,
            display_sku_name_with_sort,
            appliedPcode_promotion_name as promotion_objective,
            coalesce(creditForSalePCode, segment_opa, arcc_voucher) as promotion_code,
            case when creditForSalePCode is not null then 'PCODE'
                 when segment_opa is not null then 'PRICING_SEGMENT'
                 when voucher_category is not null or arcc_voucher is not null then 'VOUCHER' end as promotion_type,
            -- arcc_vcoucher,
            list_unit_price as list_price,	
            sale_unit_price as sale_price
from
(select pricing_ops.*, pcmap.product_family_description, pcmap.sku_id, sr.display_sku_name_with_sort
    from mse_published.ssingha_cg_promotion_performance_ty17_ty24_detail as pricing_ops
    left join cgan_ustax_published.promo_mapping_source pcmap
    on pricing_ops.Offer_Id_or_sku = pcmap.offer_number
    JOIN tax_dm.dim_sku_rollup sr 
    ON pcmap.sku_id = sr.sku_id
    and pricing_ops.Offer_Id_or_sku is not null
    )as a 
    where product_family_description like '%FEDERAL%'
     and tax_year IN ('2025')
     and (creditForSalePCode is not null
     or segment_opa is not null
     or voucher_category is not null
     or arcc_voucher is not null)

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


      CASE WHEN a.metric_type = 'complete' then COALESCE(cp.complete_pcode, c.pricing_segment, a.price_priority_code, s.promotion_code)
           WHEN  a.metric_type = 'start' THEN COALESCE(c.pricing_segment, a.price_priority_code) END AS promotion_code,

      -- CASE WHEN a.metric_type = 'complete' then COALESCE(c.promotion_objective, b.promotion_objective, s.promotion_objective) 
      --      WHEN a.metric_type = 'start' THEN COALESCE(c.promotion_objective, b.promotion_objective) END AS  promotion_objective,


          CASE WHEN a.metric_type = 'complete' AND (a.price_priority_code IS NOT NULL OR cp.complete_pcode IS NOT NULL) THEN b.promotion_objective
           WHEN a.metric_type = 'complete' AND (a.price_priority_code IS NULL AND cp.complete_pcode IS NULL) AND c.pricing_segment IS NOT NULL THEN c.promotion_objective
           WHEN a.metric_type = 'complete' AND (a.price_priority_code IS NULL AND cp.complete_pcode IS NULL) AND c.pricing_segment IS NULL THEN s.promotion_objective
           WHEN a.metric_type = 'start' AND (a.price_priority_code IS NOT NULL OR cp.complete_pcode IS NOT NULL) THEN b.promotion_objective
           WHEN a.metric_type = 'start' AND (a.price_priority_code IS NULL AND cp.complete_pcode IS NULL) AND c.pricing_segment IS NOT NULL THEN c.promotion_objective END AS promotion_objective,
  
  

      -- CASE WHEN a.metric_type = 'complete' then COALESCE(cp.promotion_type ,c.promotion_type, b.promotion_type, s.promotion_type, CASE WHEN d.list_price != m.total_federal_revenue THEN 'UNKNOWN_DISCOUNT' END) 
      --      WHEN a.metric_type = 'start' then COALESCE(c.promotion_type, b.promotion_type) END AS promotion_type,
-- Add SHAM  as third parameter in start


       CASE WHEN a.metric_type = 'complete' AND (a.price_priority_code IS NOT NULL OR cp.complete_pcode IS NOT NULL) THEN coalesce(cp.promotion_type,b.promotion_type)
           WHEN a.metric_type = 'complete' AND (a.price_priority_code IS NULL AND cp.complete_pcode IS NULL) AND c.pricing_segment IS NOT NULL THEN c.promotion_type
           WHEN a.metric_type = 'complete' AND (a.price_priority_code IS NULL AND cp.complete_pcode IS NULL) AND c.pricing_segment IS NULL THEN s.promotion_type
           WHEN a.metric_type = 'complete' AND d.list_price != m.total_federal_revenue THEN 'UNKNOWN_DISCOUNT' 
           WHEN a.metric_type = 'start' AND (a.price_priority_code IS NOT NULL OR cp.complete_pcode IS NOT NULL) THEN b.promotion_type
           WHEN a.metric_type = 'start' AND (a.price_priority_code IS NULL AND cp.complete_pcode IS NULL) AND c.pricing_segment IS NOT NULL THEN c.promotion_type END AS promotion_type,


      -- CASE WHEN a.metric_type = 'complete' THEN COALESCE(d.list_price, b.list_price, c.list_price, s.list_price) 
      --      WHEN a.metric_type = 'start' THEN COALESCE(d.list_price, b.list_price, c.list_price) END AS list_price,

      CASE WHEN a.metric_type = 'complete' AND (a.price_priority_code IS NOT NULL OR cp.complete_pcode IS NOT NULL) THEN COALESCE(d.list_price, b.list_price)
           WHEN a.metric_type = 'complete' AND (a.price_priority_code IS NULL AND cp.complete_pcode IS NULL) AND c.pricing_segment IS NOT NULL THEN COALESCE(d.list_price, c.list_price)
           WHEN a.metric_type = 'complete' AND (a.price_priority_code IS NULL AND cp.complete_pcode IS NULL) AND c.pricing_segment IS NULL THEN COALESCE(d.list_price, s.list_price)
           WHEN a.metric_type = 'start' AND (a.price_priority_code IS NOT NULL OR cp.complete_pcode IS NOT NULL) THEN COALESCE(d.list_price, b.list_price)
           WHEN a.metric_type = 'start' AND (a.price_priority_code IS NULL AND cp.complete_pcode IS NULL) AND c.pricing_segment IS NOT NULL THEN COALESCE(d.list_price, c.list_price) END AS list_price,



-- c.sale_price can bring in more than 1 values
-- How to deal with c.sale_price when metric_type = 'start'. Currently, picking the max(sale_price)
      -- CASE WHEN a.metric_type = 'complete'  THEN COALESCE(m.total_federal_revenue, b.sale_price,c.sale_price, s.sale_price)
      --      WHEN a.metric_type = 'start' THEN COALESCE(b.sale_price, c.sale_price) END AS sale_price

          --  Third parameter would be sham.price_recommended,

      CASE WHEN a.metric_type = 'complete' AND (a.price_priority_code IS NOT NULL OR cp.complete_pcode IS NOT NULL) THEN COALESCE(m.total_federal_revenue, b.sale_price,s.sale_price)
           WHEN a.metric_type = 'complete' AND (a.price_priority_code IS NULL AND cp.complete_pcode IS NULL) AND c.pricing_segment IS NOT NULL THEN COALESCE(m.total_federal_revenue, c.sale_price,s.sale_price)
           WHEN a.metric_type = 'complete' AND (a.price_priority_code IS NULL AND cp.complete_pcode IS NULL) AND c.pricing_segment IS NULL THEN COALESCE(m.total_federal_revenue, s.sale_price)
           WHEN a.metric_type = 'start' AND (a.price_priority_code IS NOT NULL OR cp.complete_pcode IS NOT NULL) THEN b.sale_price
           WHEN a.metric_type = 'start' AND (a.price_priority_code IS NULL AND cp.complete_pcode IS NULL) AND c.pricing_segment IS NOT NULL THEN c.sale_price END AS sale_price



  FROM ama_base a

  LEFT JOIN complete_pcodes cp
    ON a.auth_id = cp.auth_id
    AND a.tax_year = cp.tax_year
    AND a.sku = cp.sku
    AND a.sku_rollup_id = cp.sku_rollup_id
    AND a.event_date = cp.first_completed_date


  LEFT JOIN pcode_base b
    ON COALESCE(a.price_priority_code, cp.complete_pcode) = b.priority_code
    AND a.sku = b.display_sku_name_with_sort
    AND a.sku_rollup_id = b.sku_rollup_id
    AND date(a.event_date) BETWEEN b.promotion_price_start_date and date_sub(b.promotion_price_end_date,1)

  LEFT JOIN pricing_segment_base c
    ON a.tax_year = c.tax_year
    AND a.auth_id = c.auth_id
    AND a.metric_type = c.metric_type
    AND a.sku = c.sku
    AND a.sku_rollup_id = c.sku_rollup_id
      AND date(a.event_date) BETWEEN c.promotion_price_start_date and date_sub(c.promotion_price_end_date,1)

  LEFT JOIN pricing_ops_base s
      on a.tax_year = s.tax_year
      AND a.auth_id = s.auth_id
      and a.sku = s.display_sku_name_with_sort
      and date(s.order_date) BETWEEN (date(a.event_date) - INTERVAL 1 DAY) AND (date(a.event_date) + INTERVAL 1 DAY)


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
      ELSE ROUND((list_price - sale_price) / (list_price), 2) END AS discount_per

FROM discount_base_table
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16
ORDER BY 
    tax_year, auth_id, metric_type, event_date;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Specific Cohorts
