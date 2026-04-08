-- =========================================
-- SQL Pipeline: Cohort + Retention + Revenue
-- Данные: sales_10k (~1M rows) + stores_10k
-- Метрики: retention, churn, revenue, ARPU,
--          MRR, avg lifetime, recovered users,
--          customer-level metrics
-- =========================================

-- Indexes for acceleration
CREATE INDEX IF NOT EXISTS idx_sales_customer_date
    ON sales_10k(customer_id, order_date);
CREATE INDEX IF NOT EXISTS idx_sales_customer
    ON sales_10k(customer_id);
CREATE INDEX IF NOT EXISTS idx_sales_store
    ON sales_10k(store_id);

-- =========================================
-- STEP 1: Clean data
-- Убираем строки с нулевой выручкой и NULL
-- =========================================
DROP TABLE IF EXISTS clean_sales_temp;
CREATE TEMP TABLE clean_sales_temp AS
SELECT *
FROM sales_10k
WHERE revenue > 0
  AND customer_id IS NOT NULL
  AND order_date IS NOT NULL;

-- =========================================
-- STEP 2: First orders
-- Первый заказ каждого покупателя определяет
-- его когорту (cohort_month) и атрибуты
-- =========================================
DROP TABLE IF EXISTS first_orders_temp;
CREATE TEMP TABLE first_orders_temp AS
SELECT
    c.customer_id,
    c.order_date AS first_order_date,
    DATE_TRUNC('month', c.order_date)::date AS cohort_month,
    s.country,
    s.store_type
FROM (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY customer_id
            ORDER BY order_date, order_id
        ) AS rn
    FROM clean_sales_temp
) c
JOIN stores_10k s USING(store_id)
WHERE rn = 1;

-- =========================================
-- STEP 3: Lifetime orders
-- Все заказы покупателя + номер месяца
-- относительно его cohort_month
-- =========================================
DROP TABLE IF EXISTS lifetime_temp;
CREATE TEMP TABLE lifetime_temp AS
SELECT
    o.customer_id,
    o.order_date,
    o.revenue,
    f.cohort_month,
    f.country,
    f.store_type,
    (DATE_PART('year', o.order_date) - DATE_PART('year', f.cohort_month)) * 12
      + (DATE_PART('month', o.order_date) - DATE_PART('month', f.cohort_month)) AS month_number
FROM clean_sales_temp o
JOIN first_orders_temp f USING(customer_id);

-- =========================================
-- STEP 4: Cohort size
-- Количество уникальных покупателей
-- в каждой когорте (country + store_type)
-- =========================================
DROP TABLE IF EXISTS cohort_size_temp;
CREATE TEMP TABLE cohort_size_temp AS
SELECT
    cohort_month,
    country,
    store_type,
    COUNT(DISTINCT customer_id) AS cohort_size
FROM first_orders_temp
GROUP BY cohort_month, country, store_type;

-- =========================================
-- STEP 5: Retention
-- Доля покупателей когорты, вернувшихся
-- в месяц N после первого заказа
-- =========================================
DROP TABLE IF EXISTS retention_temp;
CREATE TEMP TABLE retention_temp AS
SELECT
    l.cohort_month,
    l.country,
    l.store_type,
    l.month_number,
    c.cohort_size,
    ROUND(COUNT(DISTINCT l.customer_id)::numeric / c.cohort_size, 2) AS retention_rate
FROM lifetime_temp l
JOIN cohort_size_temp c
  ON l.cohort_month = c.cohort_month
 AND l.country = c.country
 AND l.store_type = c.store_type
GROUP BY l.cohort_month, l.country, l.store_type, l.month_number, c.cohort_size;

-- =========================================
-- STEP 6: Revenue по когортам
-- =========================================
DROP TABLE IF EXISTS revenue_temp;
CREATE TEMP TABLE revenue_temp AS
SELECT
    cohort_month,
    country,
    store_type,
    month_number,
    SUM(revenue) AS revenue
FROM lifetime_temp
GROUP BY cohort_month, country, store_type, month_number;

-- =========================================
-- STEP 7: Итоговые метрики по когортам
-- =========================================
DROP TABLE IF EXISTS metrics_temp;
CREATE TEMP TABLE metrics_temp AS
SELECT
    r.cohort_month,
    r.country,
    r.store_type,
    r.month_number,
    r.cohort_size,
    r.retention_rate,
    rev.revenue,
    ROUND(rev.revenue * 1.0 / r.cohort_size, 2) AS arpu,
    1 - r.retention_rate AS churn_rate
FROM retention_temp r
LEFT JOIN revenue_temp rev
  ON r.cohort_month = rev.cohort_month
 AND r.country = rev.country
 AND r.store_type = rev.store_type
 AND r.month_number = rev.month_number
ORDER BY r.cohort_month, r.country, r.store_type, r.month_number;

-- =========================================
-- STEP 8: Avg Lifetime по покупателю
-- Разница в месяцах между последним
-- и первым заказом (>0 = повторные покупки)
-- =========================================
DROP TABLE IF EXISTS lifetime_span_temp;
CREATE TEMP TABLE lifetime_span_temp AS
SELECT
    f.customer_id,
    f.cohort_month,
    f.country,
    f.store_type,
    (DATE_PART('year', MAX(o.order_date)) - DATE_PART('year', f.cohort_month)) * 12
      + (DATE_PART('month', MAX(o.order_date)) - DATE_PART('month', f.cohort_month)) AS lifetime_months
FROM first_orders_temp f
JOIN clean_sales_temp o USING(customer_id)
GROUP BY f.customer_id, f.cohort_month, f.country, f.store_type;

-- =========================================
-- STEP 9: Recovered users
-- Покупатель считается "recovered" если
-- пропустил хотя бы 1 месяц, а потом
-- вернулся (gap в activity_months > 1)
-- =========================================
DROP TABLE IF EXISTS recovered_temp;
CREATE TEMP TABLE recovered_temp AS
WITH monthly_activity AS (
    SELECT
        o.customer_id,
        DATE_TRUNC('month', o.order_date)::date AS activity_month,
        f.country,
        f.store_type
    FROM clean_sales_temp o
    JOIN first_orders_temp f USING(customer_id)
    GROUP BY o.customer_id, DATE_TRUNC('month', o.order_date)::date, f.country, f.store_type
),
with_prev AS (
    SELECT
        customer_id,
        activity_month,
        country,
        store_type,
        LAG(activity_month) OVER (
            PARTITION BY customer_id
            ORDER BY activity_month
        ) AS prev_month
    FROM monthly_activity
)
SELECT
    activity_month,
    country,
    store_type,
    COUNT(DISTINCT customer_id) AS recovered_users
FROM with_prev
-- gap больше 1 месяца = пропуск = recovered
WHERE (DATE_PART('year', activity_month) - DATE_PART('year', prev_month)) * 12
    + (DATE_PART('month', activity_month) - DATE_PART('month', prev_month)) > 1
GROUP BY activity_month, country, store_type
ORDER BY activity_month, country, store_type;


-- =========================================
-- FINAL DATASETS FOR POWER BI
-- =========================================

-- [1] COHORT METRICS (основная таблица)
SELECT * FROM metrics_temp;

-- [2] PIVOT: Retention heatmap
SELECT
    cohort_month,
    country,
    store_type,
    MAX(cohort_size)                                              AS cohort_size,
    MAX(CASE WHEN month_number = 0 THEN retention_rate END)      AS m0_ret,
    MAX(CASE WHEN month_number = 1 THEN retention_rate END)      AS m1_ret,
    MAX(CASE WHEN month_number = 2 THEN retention_rate END)      AS m2_ret,
    MAX(CASE WHEN month_number = 3 THEN retention_rate END)      AS m3_ret,
    SUM(CASE WHEN month_number = 0 THEN revenue END)             AS ltv_m0,
    SUM(CASE WHEN month_number = 1 THEN revenue END)             AS ltv_m1,
    SUM(CASE WHEN month_number = 2 THEN revenue END)             AS ltv_m2,
    SUM(CASE WHEN month_number = 3 THEN revenue END)             AS ltv_m3,
    MAX(CASE WHEN month_number = 0 THEN arpu END)                AS arpu_m0,
    MAX(CASE WHEN month_number = 1 THEN arpu END)                AS arpu_m1,
    MAX(CASE WHEN month_number = 2 THEN arpu END)                AS arpu_m2,
    MAX(CASE WHEN month_number = 3 THEN arpu END)                AS arpu_m3,
    MAX(CASE WHEN month_number = 0 THEN churn_rate END)          AS churn_m0,
    MAX(CASE WHEN month_number = 1 THEN churn_rate END)          AS churn_m1,
    MAX(CASE WHEN month_number = 2 THEN churn_rate END)          AS churn_m2,
    MAX(CASE WHEN month_number = 3 THEN churn_rate END)          AS churn_m3
FROM metrics_temp
GROUP BY cohort_month, country, store_type
ORDER BY cohort_month, country, store_type;

-- [3] DAU
SELECT
    DATE_TRUNC('day', order_date)::date AS activity_date,
    COUNT(DISTINCT customer_id)         AS dau
FROM clean_sales_temp
GROUP BY 1
ORDER BY 1;

-- [4] MAU
SELECT
    DATE_TRUNC('month', order_date)::date AS activity_month,
    COUNT(DISTINCT customer_id)           AS mau
FROM clean_sales_temp
GROUP BY 1
ORDER BY 1;

-- [5] AVG LIFETIME (для карточки на дашборде)
SELECT
    cohort_month,
    country,
    store_type,
    ROUND(AVG(lifetime_months), 1) AS avg_lifetime_months
FROM lifetime_span_temp
GROUP BY cohort_month, country, store_type
ORDER BY cohort_month, country, store_type;

-- [6] RECOVERED USERS (для карточки User Growth)
SELECT * FROM recovered_temp;

-- [7] AOV (Average Order Value)
-- Средний чек SUM(revenue) / COUNT(orders) по месяцу
SELECT
    DATE_TRUNC('month', o.order_date)::date    AS month,
    s.country,
    s.store_type,
    ROUND(SUM(o.revenue), 2)                   AS monthly_revenue,
    COUNT(o.order_id)                          AS orders_count,
    ROUND(SUM(o.revenue) / COUNT(o.order_id), 2) AS aov,
    COUNT(DISTINCT o.customer_id)              AS active_customers
FROM clean_sales_temp o
JOIN stores_10k s USING(store_id)
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3;

-- [8] CUSTOMER-LEVEL METRICS
-- Гранулярность: один покупатель = одна строка
-- Использовать для карточек: Total Users, New Users,
-- ARPU на уровне покупателя, Avg Order Value,
-- а также для drill-through на дашборде
SELECT
    f.customer_id,
    f.cohort_month,
    f.country,
    f.store_type,
    f.first_order_date,
    MAX(o.order_date)             AS last_order_date,
    COUNT(DISTINCT o.order_id)    AS total_orders,
    ROUND(SUM(o.revenue), 2)      AS total_revenue,
    ROUND(AVG(o.revenue), 2)      AS avg_order_value,
    ls.lifetime_months
FROM first_orders_temp f
JOIN clean_sales_temp o USING(customer_id)
JOIN lifetime_span_temp ls USING(customer_id)
GROUP BY
    f.customer_id,
    f.cohort_month,
    f.country,
    f.store_type,
    f.first_order_date,
    ls.lifetime_months
ORDER BY f.customer_id;