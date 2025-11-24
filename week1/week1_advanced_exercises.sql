-- =========================================================
-- Advanced SQL interview-style exercises
-- Dataset: marketing_campaigns, leads
-- =========================================================


-- =========================================================
-- 1) Top 3 campaigns by ROI
--    Goal: CTE + window function (RANK)
-- =========================================================

WITH campaign_roi AS (
    SELECT
        campaign_name,
        spend_usd,
        revenue_usd,
        (revenue_usd - spend_usd)::numeric / NULLIF(spend_usd, 0) AS roi
    FROM marketing_campaigns
)
SELECT
    campaign_name,
    spend_usd,
    revenue_usd,
    roi,
    RANK() OVER (
        ORDER BY roi DESC
    ) AS ranking
FROM campaign_roi
ORDER BY ranking
LIMIT 3;


-- =========================================================
-- 2) Conversion rate by state with ranking
--    Goal: GROUP BY + CTE + window function (RANK)
-- =========================================================

WITH conv_state AS (
    SELECT 
        state,
        COUNT(*) AS total_leads,
        SUM(converted) AS conversions,
        SUM(converted)::numeric / NULLIF(COUNT(lead_id), 0) AS conversion_rate
    FROM leads
    GROUP BY state
)
SELECT
    state,
    total_leads,
    conversions,
    conversion_rate,
    RANK() OVER (
        ORDER BY conversion_rate DESC
    ) AS conv_ranking
FROM conv_state
ORDER BY conv_ranking;


-- =========================================================
-- 3) Campaigns with CPA above (or equal to) average CPA
--    Goal: CTE + global average + CROSS JOIN
-- =========================================================

WITH camp_cpa AS (
    SELECT
        campaign_id AS camp_id,
        SUM(lead_cost)::numeric / NULLIF(SUM(converted)::numeric, 0) AS cpa
    FROM leads lds
    GROUP BY campaign_id
),
average_cpa AS (
    SELECT
        AVG(cpa) AS avg_cpa
    FROM camp_cpa
)
SELECT 
    mcs.campaign_name,
    c.cpa AS cpa,
    a.avg_cpa AS average
FROM camp_cpa c
JOIN marketing_campaigns mcs 
    ON c.camp_id = mcs.campaign_id
CROSS JOIN average_cpa a
WHERE c.cpa >= a.avg_cpa
ORDER BY c.cpa DESC;


-- =========================================================
-- 4) Running total of conversions per campaign
--    Goal: Window SUM() OVER (PARTITION BY ... ORDER BY ...)
-- =========================================================

SELECT
    campaign_id,
    "timestamp",
    converted,
    SUM(converted) OVER (
        PARTITION BY campaign_id
        ORDER BY "timestamp"
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS running_conversions
FROM leads
ORDER BY campaign_id, "timestamp";


-- =========================================================
-- 5) First lead per campaign
--    Goal: ROW_NUMBER() OVER (PARTITION BY ... ORDER BY ...)
-- =========================================================

WITH leads_by_row AS (
    SELECT 
        lead_id,
        campaign_id,
        state,
        lead_cost,
        "timestamp",
        converted,
        ROW_NUMBER() OVER (
            PARTITION BY campaign_id
            ORDER BY "timestamp"
        ) AS r_number
    FROM leads
)
SELECT
    lead_id,
    campaign_id,
    state,
    lead_cost,
    "timestamp",
    converted
FROM leads_by_row
WHERE r_number = 1
ORDER BY campaign_id;


-- =========================================================
-- 6) Daily cohorts by first appearance
--    cohort_day = first day a lead was seen
--    Goal: MIN(timestamp) + DATE() + aggregation
-- =========================================================

WITH min_day AS (
    SELECT
        lead_id,
        DATE(MIN("timestamp")) AS cohort_day
    FROM leads
    GROUP BY lead_id
)
SELECT
    cohort_day,
    COUNT(*) AS lead_count
FROM min_day
GROUP BY cohort_day
ORDER BY cohort_day;


-- =========================================================
-- 7) Campaign performance classification by profit
--    Performance buckets:
--      - High Performer: profit > 10000
--      - Medium:        5000 <= profit <= 10000
--      - Low:           profit < 5000
--    Goal: Aggregation + CASE expression
-- =========================================================

WITH campaign_profit AS (
    SELECT
        mcs.campaign_id AS campaign_id,
        mcs.campaign_name AS campaign_name,
        SUM(lds.lead_cost) AS total_cost,
        MAX(mcs.revenue_usd) AS revenue_usd,
        MAX(mcs.revenue_usd) - SUM(lds.lead_cost) AS profit
    FROM marketing_campaigns mcs
    JOIN leads lds 
        ON mcs.campaign_id = lds.campaign_id
    GROUP BY mcs.campaign_id, mcs.campaign_name
)
SELECT
    campaign_id,
    campaign_name,
    total_cost,
    revenue_usd,
    profit,
    CASE 
        WHEN profit > 10000 THEN 'High Performer'
        WHEN profit >= 5000 AND profit <= 10000 THEN 'Medium'
        ELSE 'Low Performer'
    END AS performance
FROM campaign_profit
ORDER BY profit DESC;


-- =========================================================
-- 8) Combined score: conversion_rate + ROI
--    Goal: Multiple CTEs + join + ranking
-- =========================================================

WITH conv_campaign AS (
    SELECT
        campaign_id,
        SUM(converted)::numeric / NULLIF(COUNT(*), 0) AS conversion_rate
    FROM leads
    GROUP BY campaign_id
),
roi_campaign AS (
    SELECT 
        campaign_id,
        campaign_name,
        (revenue_usd - spend_usd)::numeric / NULLIF(spend_usd, 0) AS roi
    FROM marketing_campaigns
)
SELECT
    conv.campaign_id,
    roi.campaign_name,
    conv.conversion_rate,
    roi.roi,
    (conv.conversion_rate * 0.7) + (roi.roi * 0.3) AS score,
    RANK() OVER (
        ORDER BY (conv.conversion_rate * 0.7) + (roi.roi * 0.3) DESC
    ) AS ranking
FROM conv_campaign conv
JOIN roi_campaign roi 
    ON conv.campaign_id = roi.campaign_id
ORDER BY ranking;


-- =========================================================
-- 9) Conversion outliers by state
--    States with conversion_rate < 10% or > 40%
--    Goal: CTE + numeric cast + simple outlier rules
-- =========================================================

WITH conv_rate AS (
    SELECT
        state,
        COUNT(*) AS total_leads,
        SUM(converted) AS conversions,
        SUM(converted)::numeric / NULLIF(COUNT(*), 0) AS conversion_rate
    FROM leads
    GROUP BY state
)
SELECT
    state,
    total_leads,
    conversions,
    conversion_rate
FROM conv_rate
WHERE conversion_rate < 0.10
   OR conversion_rate > 0.40
ORDER BY conversion_rate;


-- =========================================================
-- 10) Revenue share by source
--     Goal: Aggregation + CROSS JOIN + numeric cast
-- =========================================================

WITH total_rev AS (
    SELECT
        SUM(revenue_usd) AS total_revenue
    FROM marketing_campaigns
),
rev_by_source AS (
    SELECT
        source,
        SUM(revenue_usd) AS source_revenue
    FROM marketing_campaigns
    GROUP BY source
)
SELECT
    rbs.source AS source,
    rbs.source_revenue AS source_revenue,
    tr.total_revenue AS total_revenue,
    rbs.source_revenue::numeric / NULLIF(tr.total_revenue, 0) AS percent_share
FROM rev_by_source rbs
CROSS JOIN total_rev tr
ORDER BY percent_share DESC;
