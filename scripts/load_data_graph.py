from neo4j import GraphDatabase
import pandas as pd
import os
import re

def to_bool(val):
    if pd.isna(val):
        return None
    if isinstance(val, bool):
        return val
    if isinstance(val, str):
        return val.lower() in ('true', 't', 'yes', 'y', '1')
    return bool(val)

CLEANED_DATA_DIR = "../data/f11_cleaned"

# 1. Load cleaned data
campaigns = pd.read_csv(os.path.join(CLEANED_DATA_DIR, 'campaigns.csv'))
first_purchase = pd.read_csv(os.path.join(CLEANED_DATA_DIR, 'client_first_purchase_date.csv'))
events = pd.read_csv(os.path.join(CLEANED_DATA_DIR, 'events.csv'))
friends = pd.read_csv(os.path.join(CLEANED_DATA_DIR, 'friends.csv'))
messages = pd.read_csv(os.path.join(CLEANED_DATA_DIR, 'messages.csv'))

uri = "bolt://localhost:7687"
username = ""
password = ""

driver = GraphDatabase.driver(uri, auth=(username, password))

# Clear database
with driver.session() as session:
    session.run("MATCH (n) DETACH DELETE n")
    session.run("DROP CONSTRAINT ON (c:Campaign) ASSERT c.campaign_id IS UNIQUE")
    session.run("DROP CONSTRAINT ON (u:User) ASSERT u.user_id IS UNIQUE")
    session.run("DROP CONSTRAINT ON (cat:Category) ASSERT cat.category_id IS UNIQUE")

# Create constraints and indexes
with driver.session() as session:
    # session.run("CREATE CONSTRAINT ON (u:User) ASSERT u.user_id IS UNIQUE")
    # session.run("CREATE CONSTRAINT ON (cat:Category) ASSERT cat.category_id IS UNIQUE")
    
    session.run("CREATE INDEX ON :User(user_id)")
    session.run("CREATE INDEX ON :Campaign(campaign_id, campaign_type)")
    session.run("CREATE INDEX ON :Category(category_id)")
    # session.run("CREATE TEXT INDEX ON :Category(category_code)")

# 1. USER Nodes
print("USER Nodes")
# 1.1. Collect all unique user IDs from all sources
user_ids = set()
# 1.1.1. from first_purchase
user_ids.update(first_purchase['user_id'].dropna().unique())

# 1.1.2. from events
user_ids.update(events['user_id'].dropna().unique())

# 1.1.3. from messages
user_ids.update(messages['user_id'].dropna().unique())

# 1.1.4. from friends (both columns)
user_ids.update(friends['friend1'].dropna().unique())
user_ids.update(friends['friend2'].dropna().unique())

# 1.2. Create User Node
with driver.session() as session:
    for user_id in user_ids:
        session.run("CREATE (u:User {user_id: $user_id})", user_id=str(user_id))

# 2. CAMPAIGN Nodes
print("CAMPAIGN Nodes")
with driver.session() as session:
    for _, row in campaigns.iterrows():
        if pd.notna(row['id']):           
            session.run(
                "CREATE (c:Campaign {campaign_id: $campaign_id, campaign_type: $campaign_type, "
                "channel: $channel, topic: $topic, started_at: $started_at, finished_at: $finished_at, "
                "total_count: $total_count, ab_test: $ab_test, warmup_mode: $warmup_mode, "
                "hour_limit: $hour_limit, subject_length: $subject_length, "
                "subject_with_personalization: $subject_with_personalization, "
                "subject_with_deadline: $subject_with_deadline, subject_with_emoji: $subject_with_emoji, "
                "subject_with_bonuses: $subject_with_bonuses, subject_with_discount: $subject_with_discount, "
                "subject_with_saleout: $subject_with_saleout, is_test: $is_test, position: $position})",
                campaign_id=int(row['id']),
                campaign_type=row['campaign_type'] if pd.notna(row['campaign_type']) else None,
                channel=row['channel'] if pd.notna(row['channel']) else None,
                topic=row['topic'] if pd.notna(row['topic']) else None,
                started_at=str(row['started_at']) if pd.notna(row['started_at']) else None,
                finished_at=str(row['finished_at']) if pd.notna(row['finished_at']) else None,
                total_count=int(row['total_count']) if pd.notna(row['total_count']) else None,
                ab_test=to_bool(row['ab_test']),
                warmup_mode=to_bool(row['warmup_mode']),
                hour_limit=float(row['hour_limit']) if pd.notna(row['hour_limit']) else None,
                subject_length=float(row['subject_length']) if pd.notna(row['subject_length']) else None,
                subject_with_personalization=to_bool(row['subject_with_personalization']),
                subject_with_deadline=to_bool(row['subject_with_deadline']),
                subject_with_emoji=to_bool(row['subject_with_emoji']),
                subject_with_bonuses=to_bool(row['subject_with_bonuses']),
                subject_with_discount=to_bool(row['subject_with_discount']),
                subject_with_saleout=to_bool(row['subject_with_saleout']),
                is_test=to_bool(row['is_test']),
                position=int(row['position']) if pd.notna(row['position']) else None
            )

# 3. CATEGORY Nodes
print("CATEGORY Nodes")
unique_categories = events[['category_id', 'category_code']].drop_duplicates(subset=['category_id'])

with driver.session() as session:
    for _, row in unique_categories.iterrows():
        if pd.notna(row['category_id']):
            session.run(
                "MERGE (cat:Category {category_id: $category_id}) "
                "SET cat.category_code = $category_code",
                category_id=int(row['category_id']),
                category_code=row['category_code'] if pd.notna(row['category_code']) else None
            )

# 4. FRIEND Relationships (User -> User)
print("FRIEND Relationships")
with driver.session() as session:
    for _, row in friends.iterrows():
        if pd.notna(row['friend1']) and pd.notna(row['friend2']):
            u1, u2 = str(row['friend1']), str(row['friend2'])
            session.run(
                "MATCH (u1:User {user_id: $user1}) "
                "MATCH (u2:User {user_id: $user2}) "
                "CREATE (u1)-[:FRIEND]->(u2) "
                "CREATE (u2)-[:FRIEND]->(u1)",
                user1=u1, user2=u2
            )

# 5. EVENT Relationships (VIEWED, PURCHASED) - User -> Category
print("EVENT Relationships")
with driver.session() as session:
    for _, row in events.iterrows():
        if pd.notna(row['user_id']) and pd.notna(row['category_id']):
            rel_type = row['event_type'].upper() if pd.notna(row['event_type']) else 'EVENT'
            
            session.run(
                f"MATCH (u:User {{user_id: $user_id}}) "
                f"MATCH (cat:Category {{category_id: $category_id}}) "
                f"CREATE (u)-[:{rel_type} {{"
                f"event_time: $event_time, session_id: $session_id, "
                f"product_id: $product_id, brand: $brand, price: $price"
                f"}}]->(cat)",
                user_id=str(row['user_id']),
                category_id=int(row['category_id']),
                event_time=str(row['event_time']) if pd.notna(row['event_time']) else None,
                session_id=row['user_session'] if pd.notna(row['user_session']) else None,
                product_id=int(row['product_id']) if pd.notna(row['product_id']) else None,
                brand=row['brand'] if pd.notna(row['brand']) else None,
                price=float(row['price']) if pd.notna(row['price']) else None
            )

# 6. RECEIVED Relationships (User -> Campaign) - Match on both campaign_id AND campaign_type
print("RECEIVED Relationships")
with driver.session() as session:
    for _, row in messages.iterrows():
        if pd.notna(row['client_id']) and pd.notna(row['campaign_id']):
            client_id = str(row['client_id'])
            user_id = str(row['user_id'])
            device_id = str(row['user_device_id'])
            # message_type in messages corresponds to campaign_type in campaigns
            message_type = row['message_type'] if pd.notna(row['message_type']) else None
            
            if user_id:
                session.run(
                    "MATCH (u:User {user_id: $user_id}) "
                    "MATCH (c:Campaign {campaign_id: $campaign_id, campaign_type: $campaign_type}) "
                    "CREATE (u)-[:RECEIVED {"
                    "message_id: $message_id, "
                    "original_message_id: $original_message_id, "
                    "client_id: $client_id, "
                    "user_device_id: $user_device_id, "
                    "message_type: $message_type, "
                    "channel: $channel, "
                    "platform: $platform, "
                    "email_provider: $email_provider, "
                    "stream: $stream, "
                    "date: $date, "
                    "sent_at: $sent_at, "
                    "is_opened: $is_opened, "
                    "opened_first_time_at: $opened_first_time_at, "
                    "opened_last_time_at: $opened_last_time_at, "
                    "is_clicked: $is_clicked, "
                    "clicked_first_time_at: $clicked_first_time_at, "
                    "clicked_last_time_at: $clicked_last_time_at, "
                    "is_unsubscribed: $is_unsubscribed, "
                    "unsubscribed_at: $unsubscribed_at, "
                    "is_hard_bounced: $is_hard_bounced, "
                    "hard_bounced_at: $hard_bounced_at, "
                    "is_soft_bounced: $is_soft_bounced, "
                    "soft_bounced_at: $soft_bounced_at, "
                    "is_complained: $is_complained, "
                    "complained_at: $complained_at, "
                    "is_blocked: $is_blocked, "
                    "blocked_at: $blocked_at, "
                    "is_purchased: $is_purchased, "
                    "purchased_at: $purchased_at, "
                    "created_at: $created_at, "
                    "updated_at: $updated_at"
                    "}]->(c)",
                    user_id=user_id,
                    campaign_id=int(row['campaign_id']),
                    campaign_type=message_type,  # This links message_type to campaign_type
                    message_id=str(row['id']),
                    original_message_id=row['message_id'] if pd.notna(row['message_id']) else None,
                    client_id=client_id,
                    user_device_id=device_id,
                    message_type=message_type,
                    channel=row['channel'] if pd.notna(row['channel']) else None,
                    platform=row['platform'] if pd.notna(row['platform']) else None,
                    email_provider=row['email_provider'] if pd.notna(row['email_provider']) else None,
                    stream=row['stream'] if pd.notna(row['stream']) else None,
                    date=str(row['date']) if pd.notna(row['date']) else None,
                    sent_at=str(row['sent_at']) if pd.notna(row['sent_at']) else None,
                    is_opened=bool(row['is_opened']) if pd.notna(row['is_opened']) else None,
                    opened_first_time_at=str(row['opened_first_time_at']) if pd.notna(row['opened_first_time_at']) else None,
                    opened_last_time_at=str(row['opened_last_time_at']) if pd.notna(row['opened_last_time_at']) else None,
                    is_clicked=bool(row['is_clicked']) if pd.notna(row['is_clicked']) else None,
                    clicked_first_time_at=str(row['clicked_first_time_at']) if pd.notna(row['clicked_first_time_at']) else None,
                    clicked_last_time_at=str(row['clicked_last_time_at']) if pd.notna(row['clicked_last_time_at']) else None,
                    is_unsubscribed=bool(row['is_unsubscribed']) if pd.notna(row['is_unsubscribed']) else None,
                    unsubscribed_at=str(row['unsubscribed_at']) if pd.notna(row['unsubscribed_at']) else None,
                    is_hard_bounced=bool(row['is_hard_bounced']) if pd.notna(row['is_hard_bounced']) else None,
                    hard_bounced_at=str(row['hard_bounced_at']) if pd.notna(row['hard_bounced_at']) else None,
                    is_soft_bounced=bool(row['is_soft_bounced']) if pd.notna(row['is_soft_bounced']) else None,
                    soft_bounced_at=str(row['soft_bounced_at']) if pd.notna(row['soft_bounced_at']) else None,
                    is_complained=bool(row['is_complained']) if pd.notna(row['is_complained']) else None,
                    complained_at=str(row['complained_at']) if pd.notna(row['complained_at']) else None,
                    is_blocked=bool(row['is_blocked']) if pd.notna(row['is_blocked']) else None,
                    blocked_at=str(row['blocked_at']) if pd.notna(row['blocked_at']) else None,
                    is_purchased=bool(row['is_purchased']) if pd.notna(row['is_purchased']) else None,
                    purchased_at=str(row['purchased_at']) if pd.notna(row['purchased_at']) else None,
                    created_at=str(row['created_at']) if pd.notna(row['created_at']) else None,
                    updated_at=str(row['updated_at']) if pd.notna(row['updated_at']) else None
                )

# 7. FIRST_PURCHASE Relationships (self-loop on User)
print("FIRST_PURCHASE Relationships")
with driver.session() as session:
    for _, row in first_purchase.iterrows():
        if pd.notna(row['client_id']):
            client_id = str(row['client_id'])
            user_id = str(row['user_id'])
            device_id = str(row['user_device_id'])
            
            if user_id:
                session.run(
                    "MATCH (u:User {user_id: $user_id}) "
                    "CREATE (u)-[:FIRST_PURCHASE {"
                    "client_id: $client_id, "
                    "first_purchase_date: $first_purchase_date, "
                    "user_device_id: $user_device_id"
                    "}]->(u)",
                    user_id=user_id,
                    client_id=client_id,
                    first_purchase_date=str(row['first_purchase_date']) if pd.notna(row['first_purchase_date']) else None,
                    user_device_id=device_id
                )

# 8. Create additional indexes for performance
with driver.session() as session:
    session.run("CREATE INDEX ON :RECEIVED(sent_at)")
    session.run("CREATE INDEX ON :RECEIVED(is_purchased)")
    session.run("CREATE INDEX ON :VIEWED(event_time)")
    session.run("CREATE INDEX ON :PURCHASED(event_time)")

driver.close()