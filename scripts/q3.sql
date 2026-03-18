-- Full‑text search to expand recommendations based on category_code of top products
WITH
user_id_param AS (
    SELECT '548885063' AS uid   -- replace with an active user
),
-- Products the user has already interacted with
user_products AS (
    SELECT DISTINCT e.product_id
    FROM event e
    JOIN session s ON e.user_session = s.user_session
    CROSS JOIN user_id_param p
    WHERE s.user_id = p.uid
),
-- User's top 3 categories by interaction count
user_category_prefs AS (
    SELECT
        e.category_id,
        COUNT(*) AS interaction_count
    FROM event e
    JOIN session s ON e.user_session = s.user_session
    CROSS JOIN user_id_param p
    WHERE s.user_id = p.uid
    GROUP BY e.category_id
    ORDER BY interaction_count DESC
    LIMIT 3
),
-- Popular products in those categories
popular_products_in_cat AS (
    SELECT
        e.category_id,
        e.product_id,
        COUNT(*) AS total_interactions,
        ROW_NUMBER() OVER (PARTITION BY e.category_id ORDER BY COUNT(*) DESC) AS rn
    FROM event e
    WHERE e.category_id IN (SELECT category_id FROM user_category_prefs)
    GROUP BY e.category_id, e.product_id
),
-- Personalised recommendations for the user
recommended_products AS (
    SELECT
        pp.product_id,
        pp.category_id,
        pp.rn
    FROM popular_products_in_cat pp
    WHERE NOT EXISTS (
        SELECT 1 FROM user_products up WHERE up.product_id = pp.product_id
    )
    AND pp.rn <= 5
),
-- FALLBACK: if no personalised recommendations, use the most popular product overall
fallback_product AS (
    SELECT
        e.product_id,
        e.category_id,
        1 AS rn
    FROM event e
    GROUP BY e.product_id, e.category_id
    ORDER BY COUNT(*) DESC
    LIMIT 1
),
-- Combine personalised and fallback
base_products AS (
    SELECT product_id, category_id, rn FROM recommended_products
    UNION ALL
    SELECT product_id, category_id, rn FROM fallback_product
    WHERE NOT EXISTS (SELECT 1 FROM recommended_products)  -- only if empty
),
-- Build product dimension with cleaned category_code for full‑text search
product_info AS (
    SELECT DISTINCT ON (e.product_id)
        e.product_id,
        e.category_id,
        c.category_code,
        e.brand,
        -- Clean category_code: replace all non‑alphanumeric with space
        regexp_replace(c.category_code, '[^a-zA-Z0-9]+', ' ', 'g') AS clean_code
    FROM event e
    JOIN categories c ON e.category_id = c.category_id
    WHERE e.product_id IS NOT NULL AND c.category_code IS NOT NULL
    ORDER BY e.product_id, e.event_time
),
-- For each base product, find similar ones using full‑text search
similar_products AS (
    SELECT
        bp.product_id AS original_product_id,
        pi.clean_code AS original_clean_code,
        sim.product_id AS similar_product_id,
        sim.category_code AS similar_category_code,
        sim.brand AS similar_brand,
        -- Use websearch_to_tsquery if available (PostgreSQL 11+), else plainto_tsquery
        ts_rank(
            to_tsvector('english', sim.clean_code),
            websearch_to_tsquery('english', pi.clean_code)
        ) AS rank,
        ROW_NUMBER() OVER (
            PARTITION BY bp.product_id
            ORDER BY ts_rank(
                to_tsvector('english', sim.clean_code),
                websearch_to_tsquery('english', pi.clean_code)
            ) DESC
        ) AS rn
    FROM base_products bp
    JOIN product_info pi ON bp.product_id = pi.product_id
    JOIN product_info sim ON sim.product_id != bp.product_id
        AND to_tsvector('english', sim.clean_code) @@
            websearch_to_tsquery('english', pi.clean_code)
        AND sim.product_id NOT IN (SELECT product_id FROM base_products)
)
-- Output the top 5 similar products for each original base product
SELECT
    original_product_id,
    original_clean_code AS original_keywords,
    similar_product_id,
    similar_category_code,
    similar_brand,
    rank
FROM similar_products
WHERE rn <= 5
ORDER BY rn, original_product_id
LIMIT 15;