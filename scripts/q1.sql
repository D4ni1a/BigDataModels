-- Analyze campaign effectiveness and the role of social networks
WITH
-- Messages with user_id and purchase flag
messages_with_user AS (
    SELECT
        m.campaign_id,
        m.client_id,
        fp.user_id,
        m.is_purchased
    FROM messages m
    JOIN first_purchase fp ON m.client_id = fp.client_id
    WHERE m.is_purchased IS NOT NULL
),
-- Per campaign: total sent and total purchased
campaign_stats AS (
    SELECT
        campaign_id,
        COUNT(DISTINCT client_id) AS total_sent,
        COUNT(DISTINCT CASE WHEN is_purchased THEN client_id END) AS total_purchased
    FROM messages_with_user
    GROUP BY campaign_id
),
-- Clients who purchased and have at least one friend who also purchased from the same campaign
client_friend_purchased AS (
    SELECT DISTINCT
        m1.campaign_id,
        m1.client_id
    FROM messages_with_user m1
    JOIN first_purchase fp1 ON m1.client_id = fp1.client_id
    JOIN friends f ON fp1.user_id = f.friend1 OR fp1.user_id = f.friend2
    JOIN first_purchase fp2 ON (f.friend1 = fp2.user_id OR f.friend2 = fp2.user_id)
    JOIN messages_with_user m2 ON fp2.client_id = m2.client_id
        AND m2.campaign_id = m1.campaign_id
        AND m2.is_purchased = true
    WHERE m1.is_purchased = true
)
-- Final output
SELECT
    cs.campaign_id,
    cs.total_sent,
    cs.total_purchased,
    ROUND(100.0 * cs.total_purchased / NULLIF(cs.total_sent, 0), 2) AS conversion_rate_pct,
    COUNT(DISTINCT cfp.client_id) AS purchasers_with_friend_purchased,
    ROUND(100.0 * COUNT(DISTINCT cfp.client_id) / NULLIF(cs.total_purchased, 0), 2) AS pct_of_purchasers_with_friend
FROM campaign_stats cs
LEFT JOIN client_friend_purchased cfp ON cs.campaign_id = cfp.campaign_id
GROUP BY cs.campaign_id, cs.total_sent, cs.total_purchased
ORDER BY conversion_rate_pct DESC
LIMIT 10;