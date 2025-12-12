drop table raw_ecommerce_orders_raw_table;
CREATE TABLE raw_ecommerce_orders_raw_table (
    -
   
	);
alter table raw_ecommerce_orders_raw_table
    ADD COLUMN last_updated_at integer;
select * from raw_ecommerce_orders_raw_table;

SELECT COUNT(*)
FROM raw_ecommerce_orders_raw_table
WHERE order_date IS NULL;

SELECT *
FROM raw_ecommerce_orders_raw_table
WHERE quantity <= 0 OR unit_price <= 0;

SELECT *
FROM raw_ecommerce_orders_raw_table
WHERE discount_pct < 0 OR discount_pct > 100;

SELECT COUNT(*) AS crm_nulls
FROM crm_customers_raw_table
WHERE city IS NULL OR state IS NULL;

SELECT COUNT(*) AS orders_nulls
FROM raw_ecommerce_orders_raw_table
WHERE city IS NULL OR state IS NULL;

CREATE VIEW orders_normalized AS
SELECT *,
       LOWER(TRIM(platform)) AS normalized_platform
FROM raw_ecommerce_orders_raw_table;

DROP VIEW IF EXISTS clean_orders;
CREATE VIEW clean_orders AS
SELECT *,
       CASE
           WHEN discount_pct < 0 THEN 0
           WHEN discount_pct > 80 THEN 80
           ELSE discount_pct
       END AS clean_discount,
       quantity * unit_price AS gross_amount,
       quantity * unit_price * (1 - (CASE
           WHEN discount_pct < 0 THEN 0
           WHEN discount_pct > 80 THEN 80
           ELSE discount_pct
       END / 100)) AS net_amount
FROM raw_ecommerce_orders_raw_table;

SELECT customer_id,
       MIN(order_date) AS first_order,
       MAX(order_date) AS last_order,
       COUNT(*) AS total_orders
FROM raw_ecommerce_orders_raw_table
GROUP BY customer_id;

DROP VIEW IF EXISTS orders_enriched;
CREATE VIEW orders_enriched AS
SELECT *,
       EXTRACT(YEAR FROM order_date) AS order_year,
       EXTRACT(MONTH FROM order_date) AS order_month,
       EXTRACT(DAY FROM order_date) AS order_day,
FROM raw_ecommerce_orders_raw_table;

SELECT city, SUM(net_amount) AS total_revenue
FROM raw_ecommerce_orders_raw_table
GROUP BY city
ORDER BY total_revenue DESC
LIMIT 10;

SELECT category,
       STRFTIME('%m', order_date) AS month,
       SUM(net_amount) AS monthly_revenue
FROM raw_ecommerce_orders_raw_table
WHERE STRFTIME('%Y', order_date) = '2025'
GROUP BY category, month
ORDER BY category, month;

-- Overall AOV
SELECT AVG(net_amount) AS overall_aov
FROM raw_ecommerce_orders_raw_table;

-- AOV per platform
SELECT platform, AVG(net_amount) AS aov
FROM raw_ecommerce_orders_raw_table
GROUP BY platform;

-- AOV comparison
SELECT
  AVG(CASE WHEN discount_pct = 0 THEN net_amount END) AS aov_no_discount,
  AVG(CASE WHEN discount_pct > 0 THEN net_amount END) AS aov_with_discount
FROM raw_ecommerce_orders_raw_table;

-- Avg discount per category
SELECT category, AVG(discount_pct) AS avg_discount
FROM raw_ecommerce_orders_raw_table
GROUP BY category;

SELECT c.loyalty_tier,
       COUNT(DISTINCT c.crm_customer_id) AS num_customers,
       SUM(o.net_amount) AS total_revenue,
       ROUND(COUNT(o.order_id) * 1.0 / COUNT(DISTINCT c.crm_customer_id), 2) AS avg_orders_per_customer
FROM crm_customers_raw_table c
LEFT JOIN raw_ecommerce_orders_raw_table o ON c.crm_customer_id = o.customer_id
GROUP BY c.loyalty_tier;

SELECT *
FROM crm_customers_raw_table c
WHERE signup_date <= DATE('now', '-1 year')
  AND crm_customer_id NOT IN (
    SELECT customer_id
    FROM raw_ecommerce_orders_raw_table
    WHERE order_date >= DATE('now', '-6 months')
  )
  AND email IN (
    SELECT customer_email
    FROM marketing_events_raw_table
    WHERE event_time >= DATE('now', '-6 months')
  );

SELECT customer_id, order_id, order_date,
       ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_date) AS order_rank
FROM raw_ecommerce_orders_raw_table;

CREATE VIEW IF NOT EXISTS vw_monthly_sales_summary AS
SELECT
    strftime('%Y-%m-01', order_date) AS month_start,
    strftime('%Y', order_date)       AS order_year,
    strftime('%m', order_date)       AS order_month,
    platform,
    category,
    COUNT(DISTINCT order_id)         AS total_orders,
    SUM(net_amount)                  AS total_revenue,
    AVG(net_amount)                  AS avg_order_value
FROM raw_ecommerce_orders_raw_table
WHERE order_date IS NOT NULL
GROUP BY
    strftime('%Y-%m-01', order_date),
    strftime('%Y', order_date),
    strftime('%m', order_date),
    platform,
    category;

DROP VIEW IF EXISTS vw_loyalty_revenue;

CREATE VIEW vw_loyalty_revenue AS
SELECT
    c.loyalty_tier,
    c.crm_customer_id              AS customer_id,
    COUNT(DISTINCT o.order_id)     AS total_orders,
    COALESCE(SUM(o.net_amount), 0) AS total_revenue,
    AVG(o.net_amount)              AS avg_order_value
FROM crm_customers_raw_table c
LEFT JOIN raw_ecommerce_orders_raw_table o
       ON o.customer_id = c.crm_customer_id
GROUP BY
    c.loyalty_tier,
    c.crm_customer_id;


DROP VIEW IF EXISTS vw_geo_sales_summary;

CREATE VIEW vw_geo_sales_summary AS
SELECT
    city,
    state,
    COUNT(DISTINCT order_id) AS total_orders,
    SUM(net_amount)          AS total_revenue,
    AVG(net_amount)          AS avg_order_value
FROM raw_ecommerce_orders_raw_table
GROUP BY
    city,
    state;

DROP VIEW IF EXISTS vw_platform_payment_summary;

CREATE VIEW vw_platform_payment_summary AS
SELECT
    platform,
    payment_method,
    COUNT(DISTINCT order_id) AS total_orders,
    SUM(net_amount)          AS total_revenue,
    AVG(net_amount)          AS avg_order_value
FROM raw_ecommerce_orders_raw_table
GROUP BY
    platform,
    payment_method;


DROP VIEW IF EXISTS vw_new_vs_returning;

CREATE VIEW vw_new_vs_returning AS
SELECT
    o.order_id,
    o.customer_id,
    o.order_date,
    strftime('%Y-%m-01', o.order_date) AS month_start,
    CASE
        WHEN strftime('%Y-%m', o.order_date) = strftime('%Y-%m', fo.first_order_date)
        THEN 'New'
        ELSE 'Returning'
    END AS customer_type,
    o.net_amount
FROM raw_ecommerce_orders_raw_table o
JOIN (
    SELECT
        customer_id,
        MIN(order_date) AS first_order_date
    FROM raw_ecommerce_orders_raw_table
    GROUP BY customer_id
) fo
  ON o.customer_id = fo.customer_id;



DROP VIEW IF EXISTS vw_customer_activity_wide;

CREATE VIEW vw_customer_activity_wide AS
WITH
order_agg AS (
    SELECT
        customer_id,
        COUNT(DISTINCT order_id) AS total_orders,
        COALESCE(SUM(net_amount), 0) AS total_revenue,
        AVG(net_amount) AS avg_order_value,
        MIN(order_date) AS first_order_date,
        MAX(order_date) AS last_order_date
    FROM raw_ecommerce_orders_raw_table
    WHERE order_date IS NOT NULL
    GROUP BY customer_id
),
marketing_agg AS (
    SELECT
        customer_email,
        COUNT(*) AS total_marketing_events,
        SUM(CASE WHEN LOWER(event_type) = 'open' THEN 1 ELSE 0 END) AS opens,
        SUM(CASE WHEN LOWER(event_type) = 'click' THEN 1 ELSE 0 END) AS clicks,
        SUM(CASE WHEN LOWER(event_type) = 'bounce' THEN 1 ELSE 0 END) AS bounces,
        SUM(CASE WHEN LOWER(event_type) = 'unsubscribe' THEN 1 ELSE 0 END) AS unsubscribes,
        MIN(event_time) AS first_marketing_event,
        MAX(event_time) AS last_marketing_event
    FROM marketing_events_raw_table
    WHERE event_time IS NOT NULL
    GROUP BY customer_email
),
ticket_agg AS (
    SELECT
        customer_email,
        COUNT(*) AS total_tickets,
        SUM(CASE WHEN status = 'Open' THEN 1 ELSE 0 END) AS open_tickets,
        SUM(CASE WHEN status = 'In Progress' THEN 1 ELSE 0 END) AS in_progress_tickets,
        SUM(CASE WHEN status = 'Resolved' THEN 1 ELSE 0 END) AS resolved_tickets,
        SUM(CASE WHEN status = 'Closed' THEN 1 ELSE 0 END) AS closed_tickets,
        MIN(created_at) AS first_ticket_date,
        MAX(created_at) AS last_ticket_date
    FROM support_tickets_raw_table
    WHERE created_at IS NOT NULL
    GROUP BY customer_email
)
SELECT
    c.crm_customer_id AS customer_id,
    c.first_name,
    c.last_name,
    c.first_name || ' ' || c.last_name AS full_name,
    c.email,
    c.phone,
    c.gender,
    c.dob,
    c.city AS customer_city,
    c.state AS customer_state,
    c.signup_date,
    c.preferred_channel,
    c.loyalty_tier,
    oa.total_orders,
    oa.total_revenue,
    oa.avg_order_value,
    oa.first_order_date,
    oa.last_order_date,
    CASE
        WHEN oa.last_order_date IS NULL THEN NULL
        ELSE julianday('now') - julianday(oa.last_order_date)
    END AS recency_days,
    ma.total_marketing_events,
    ma.opens,
    ma.clicks,
    ma.bounces,
    ma.unsubscribes,
    ma.first_marketing_event,
    ma.last_marketing_event,
    CASE
        WHEN ma.total_marketing_events > 0 THEN CAST(ma.clicks AS FLOAT) / ma.total_marketing_events
        ELSE NULL
    END AS click_rate,
    CASE
        WHEN ma.total_marketing_events > 0 THEN CAST(ma.opens AS FLOAT) / ma.total_marketing_events
        ELSE NULL
    END AS open_rate,
    ta.total_tickets,
    ta.open_tickets,   
    ta.in_progress_tickets,
    ta.resolved_tickets,
    ta.closed_tickets,
    ta.first_ticket_date,
    ta.last_ticket_date,
    eo.order_id,
    eo.order_date,
    eo.platform,
    eo.category,
    eo.sub_category,
    eo.payment_method,
    eo.quantity,
    eo.unit_price,
    eo.discount_pct,
    eo.city AS order_city,
    eo.state AS order_state,
    eo.gross_amount,
    eo.net_amount
FROM crm_customers_raw_table c
LEFT JOIN order_agg oa ON oa.customer_id = c.crm_customer_id
LEFT JOIN marketing_agg ma ON ma.customer_email = c.email
LEFT JOIN ticket_agg ta ON ta.customer_email = c.email
LEFT JOIN raw_ecommerce_orders_raw_table eo ON eo.customer_id = c.crm_customer_id;

select* from vw_customer_activity_wide;




DROP VIEW IF EXISTS vw_customer_churn;

CREATE VIEW vw_customer_churn AS
WITH 
orders_agg AS (
    SELECT
        customer_id,
        COUNT(*) AS total_orders,
        SUM(net_amount) AS total_revenue,
        MAX(order_date) AS last_order_date,
        CAST(julianday('now') - julianday(MAX(order_date)) AS INT) AS recency_days
    FROM raw_ecommerce_orders_raw_table
    GROUP BY customer_id
),
marketing_agg AS (
    SELECT
        customer_email,
        COUNT(*) AS total_events,
        SUM(CASE WHEN event_type = 'click' THEN 1 ELSE 0 END) AS total_clicks,
        SUM(CASE WHEN event_type = 'open' THEN 1 ELSE 0 END) AS total_opens,
        CASE 
            WHEN SUM(CASE WHEN event_type = 'open' THEN 1 ELSE 0 END) = 0 THEN 0
            ELSE ROUND(
                1.0 * SUM(CASE WHEN event_type = 'click' THEN 1 ELSE 0 END) /
                SUM(CASE WHEN event_type = 'open' THEN 1 ELSE 0 END), 
            3)
        END AS click_rate
    FROM marketing_events_raw_table
    GROUP BY customer_email
),
support_agg AS (
    SELECT
        customer_email,
        COUNT(*) AS total_tickets,
        SUM(CASE WHEN status = 'unresolved' THEN 1 ELSE 0 END) AS unresolved_tickets
    FROM support_tickets_raw_table
    GROUP BY customer_email
)

SELECT
    c.crm_customer_id,
    c.email,
    c.signup_date,
    c.loyalty_tier,

    COALESCE(o.total_orders, 0) AS total_orders,
    COALESCE(o.total_revenue, 0) AS total_revenue,
    o.recency_days,

    COALESCE(m.total_clicks, 0) AS clicks,
    COALESCE(m.total_opens, 0) AS opens,
    COALESCE(m.click_rate, 0) AS click_rate,

    COALESCE(s.total_tickets, 0) AS total_tickets,
    COALESCE(s.unresolved_tickets, 0) AS unresolved_tickets,

    CASE 
        WHEN o.last_order_date IS NULL THEN 1
        WHEN o.recency_days >= 60 THEN 1
        ELSE 0
    END AS churn_label

FROM crm_customers_raw_table c
LEFT JOIN orders_agg o ON c.crm_customer_id = o.customer_id
LEFT JOIN marketing_agg m ON c.email = m.customer_email
LEFT JOIN support_agg s ON c.email = s.customer_email;
select * from vw_customer_churn;


DROP VIEW IF EXISTS vw_next_purchase_features;
CREATE VIEW vw_next_purchase_features AS
WITH
-- 1) Last 3 order amounts per customer
last3 AS (
    SELECT
        customer_id,
        GROUP_CONCAT(net_amount) AS last_3_order_values
    FROM (
        SELECT 
            customer_id,
            net_amount
        FROM raw_ecommerce_orders_raw_table
        ORDER BY order_date DESC
        LIMIT 3
    )
    GROUP BY customer_id
),
-- 2) Order frequency (total orders)
freq AS (
    SELECT
        customer_id,
        COUNT(*) AS order_frequency
    FROM raw_ecommerce_orders_raw_table
    GROUP BY customer_id
),
-- 3) Discount history
discount_hist AS (
    SELECT
        customer_id,
        AVG(discount_pct) AS avg_discount_pct
    FROM raw_ecommerce_orders_raw_table
    GROUP BY customer_id
),
-- 4) Category affinity (most ordered category)
cat_pref AS (
    SELECT customer_id, category
    FROM (
        SELECT
            customer_id,
            category,
            COUNT(*) AS cnt
        FROM raw_ecommerce_orders_raw_table
        GROUP BY customer_id, category
        ORDER BY cnt DESC
    )
    GROUP BY customer_id
),
-- 5) Next order amount (label)
next_order AS (
    SELECT 
        customer_id,
        order_id,
        LEAD(net_amount) OVER (
            PARTITION BY customer_id 
            ORDER BY order_date
        ) AS next_order_amount
    FROM raw_ecommerce_orders_raw_table
)
-- Final dataset
SELECT
    o.customer_id,
    l3.last_3_order_values,
    f.order_frequency,
    d.avg_discount_pct,
    cp.category AS category_affinity,
    n.next_order_amount AS label_next_purchase_amount
FROM raw_ecommerce_orders_raw_table o
LEFT JOIN last3 l3 ON o.customer_id = l3.customer_id
LEFT JOIN freq f ON o.customer_id = f.customer_id
LEFT JOIN discount_hist d ON o.customer_id = d.customer_id
LEFT JOIN cat_pref cp ON o.customer_id = cp.customer_id
LEFT JOIN next_order n ON o.order_id = n.order_id
GROUP BY o.customer_id;


SELECT name FROM sqlite_master WHERE type='table';

DROP VIEW IF EXISTS vw_campaign_response_features;

CREATE VIEW vw_campaign_response_features AS
WITH past_engagement AS (
    SELECT
        me.customer_email,
        COUNT(CASE WHEN LOWER(me.event_type) = 'open' THEN 1 END) AS past_opens,
        COUNT(CASE WHEN LOWER(me.event_type) = 'click' THEN 1 END) AS past_clicks,
        MAX(me.event_time) AS last_engagement_time
    FROM marketing_events_raw_table me
    GROUP BY me.customer_email
),
order_stats AS (
    SELECT
        o.customer_id,
        SUM(o.net_amount) AS total_order_value
    FROM raw_ecommerce_orders_raw_table o
    GROUP BY o.customer_id
),
ticket_stats AS (
    SELECT
        st.customer_email,
        COUNT(*) AS total_tickets
    FROM support_tickets_raw_table st
    GROUP BY st.customer_email
)
SELECT
    c.crm_customer_id AS customer_id,
    -- Removed category_affinity
    c.email,
    COALESCE(pe.past_opens, 0) AS past_opens,
    COALESCE(pe.past_clicks, 0) AS past_clicks,
    COALESCE(os.total_order_value, 0) AS past_order_value,
    CASE
        WHEN pe.last_engagement_time IS NOT NULL
        THEN ROUND((julianday('now') - julianday(pe.last_engagement_time)), 1)
        ELSE NULL
    END AS days_since_last_engagement,
    COALESCE(ts.total_tickets, 0) AS total_tickets,
    CASE
        WHEN me.event_type IS NOT NULL AND LOWER(me.event_type) = 'click'
        THEN 1 ELSE 0
    END AS clicked_flag
FROM crm_customers_raw_table c
LEFT JOIN past_engagement pe
       ON pe.customer_email = c.email
LEFT JOIN order_stats os
       ON os.customer_id = c.crm_customer_id
LEFT JOIN ticket_stats ts
       ON ts.customer_email = c.email
LEFT JOIN marketing_events_raw_table me
       ON me.customer_email = c.email;
select * from vw_campaign_response_features;

PRAGMA table_info(support_tickets_raw_table);
PRAGMA table_info(crm_customers_raw_table);


