-- Top personalized product recommendations for a given user
WITH
user_id_param AS (
    SELECT '548885063' AS uid   -- replace with desired user_id
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
-- Overall popular products in those categories
popular_products_in_cat AS (
    SELECT
        e.category_id,
        e.product_id,
        COUNT(*) AS total_interactions,
        ROW_NUMBER() OVER (PARTITION BY e.category_id ORDER BY COUNT(*) DESC) AS rn
    FROM event e
    WHERE e.category_id IN (SELECT category_id FROM user_category_prefs)
    GROUP BY e.category_id, e.product_id
)
-- For each category, take top 5 products not yet interacted by the user
SELECT
    p.uid AS user_id,
    pp.category_id,
    pp.product_id,
    pp.total_interactions,
    pp.rn AS rank_in_category
FROM popular_products_in_cat pp
CROSS JOIN user_id_param p
WHERE NOT EXISTS (
    SELECT 1 FROM user_products up WHERE up.product_id = pp.product_id
)
AND pp.rn <= 5
ORDER BY pp.rn, pp.category_id
LIMIT 10;