import psycopg2
import pandas as pd
from psycopg2 import sql
import os

def none_converter(value):
    if pd.isna(value):
        return None
    return value

CLEANED_DATA_DIR = "../data/f11_cleaned"

# 1. Load cleaned data
campaigns = pd.read_csv(os.path.join(CLEANED_DATA_DIR, 'campaigns.csv'))
first_purchase = pd.read_csv(os.path.join(CLEANED_DATA_DIR, 'client_first_purchase_date.csv'))
events = pd.read_csv(os.path.join(CLEANED_DATA_DIR, 'events.csv'))
friends = pd.read_csv(os.path.join(CLEANED_DATA_DIR, 'friends.csv'))
messages = pd.read_csv(os.path.join(CLEANED_DATA_DIR, 'messages.csv'))

conn = psycopg2.connect(
    dbname='bd_a2',
    user='postgres',
    password='postgres',
    host='localhost',
    port='5432'
)
cur = conn.cursor()

print("Building tables")
# 2. Create tables
# 2.1. Drop tables (CASCADE to remove dependencies)
cur.execute("""
DROP TABLE IF EXISTS messages, friends, campaign, first_purchase, event, session, categories, users CASCADE;
""")

# 2.2. Create
cur.execute("""
-- 1. Users: will contain all user_ids from all sources
CREATE TABLE users (
    user_id TEXT PRIMARY KEY
);

-- 2. Categories: from events
CREATE TABLE categories (
    category_id BIGINT PRIMARY KEY,
    category_code TEXT
);

-- 3. Session: user_session is PK, references users
CREATE TABLE session (
    user_session TEXT PRIMARY KEY,
    user_id TEXT NOT NULL REFERENCES users(user_id)
);

-- 4. Event: references session and categories
CREATE TABLE event (
    event_time TIMESTAMP,
    event_type TEXT,
    product_id BIGINT,
    category_id BIGINT NOT NULL REFERENCES categories(category_id),
    brand TEXT,
    price NUMERIC,
    user_session TEXT NOT NULL REFERENCES session(user_session)
);

-- 5. First purchase: client_id is PK, references users
CREATE TABLE first_purchase (
    client_id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL REFERENCES users(user_id),
    user_device_id TEXT,
    first_purchase_date TIMESTAMP
);

-- 6. Campaign
CREATE TABLE campaign (
    id INT PRIMARY KEY,
    campaign_type TEXT,
    channel TEXT,
    topic TEXT,
    started_at TIMESTAMP,
    finished_at TIMESTAMP,
    total_count INT,
    ab_test BOOLEAN,
    warmup_mode BOOLEAN,
    hour_limit NUMERIC,
    subject_length NUMERIC,
    subject_with_personalization BOOLEAN,
    subject_with_deadline BOOLEAN,
    subject_with_emoji BOOLEAN,
    subject_with_bonuses BOOLEAN,
    subject_with_discount BOOLEAN,
    subject_with_saleout BOOLEAN,
    is_test BOOLEAN,
    position INT
);

-- 7. Friends: undirected edges
CREATE TABLE friends (
    friend1 TEXT NOT NULL REFERENCES users(user_id),
    friend2 TEXT NOT NULL REFERENCES users(user_id),
    CHECK (friend1 < friend2),
    PRIMARY KEY (friend1, friend2)
);

-- 8. Messages
CREATE TABLE messages (
    id INT PRIMARY KEY,
    message_id TEXT,
    campaign_id INT NOT NULL REFERENCES campaign(id),
    message_type TEXT,
    client_id TEXT NOT NULL REFERENCES first_purchase(client_id),
    channel TEXT,
    category TEXT,
    platform TEXT,
    email_provider TEXT,
    stream TEXT,
    date DATE,
    sent_at TIMESTAMP,
    is_opened BOOLEAN,
    opened_first_time_at TIMESTAMP,
    opened_last_time_at TIMESTAMP,
    is_clicked BOOLEAN,
    clicked_first_time_at TIMESTAMP,
    clicked_last_time_at TIMESTAMP,
    is_unsubscribed BOOLEAN,
    unsubscribed_at TIMESTAMP,
    is_hard_bounced BOOLEAN,
    hard_bounced_at TIMESTAMP,
    is_soft_bounced BOOLEAN,
    soft_bounced_at TIMESTAMP,
    is_complained BOOLEAN,
    complained_at TIMESTAMP,
    is_blocked BOOLEAN,
    blocked_at TIMESTAMP,
    is_purchased BOOLEAN,
    purchased_at TIMESTAMP,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
);
            
-- Users table indexes
CREATE INDEX idx_users_user_id ON users(user_id);

-- Categories table indexes
CREATE INDEX idx_categories_category_id ON categories(category_id);

-- Session table indexes
CREATE INDEX idx_session_user_id ON session(user_id);

-- Event table indexes (foreign keys)
CREATE INDEX idx_event_category_id ON event(category_id);
CREATE INDEX idx_event_user_session ON event(user_session);

-- First purchase indexes (foreign key)
CREATE INDEX idx_first_purchase_user_id ON first_purchase(user_id);

-- Friends indexes (for bidirectional lookups)
CREATE INDEX idx_friends_friend2 ON friends(friend2);

-- Messages indexes (foreign keys)
CREATE INDEX idx_messages_campaign_id ON messages(campaign_id);
CREATE INDEX idx_messages_client_id ON messages(client_id);
""")
conn.commit()

print("Users table")
# 3. Users: collect all unique user IDs from all sources
user_ids = set()
# 3.1. from first_purchase
user_ids.update(first_purchase['user_id'].dropna().unique())

# 3.2. from events
user_ids.update(events['user_id'].dropna().unique())

# 3.3. from messages
user_ids.update(messages['user_id'].dropna().unique())

# 3.4. from friends (both columns)
user_ids.update(friends['friend1'].dropna().unique())
user_ids.update(friends['friend2'].dropna().unique())

# 3.5. Insert into users table
for uid in user_ids:
    cur.execute("INSERT INTO users (user_id) VALUES (%s) ON CONFLICT DO NOTHING", (str(uid),))
conn.commit()

print("Categories table")
# 4. Categories: distinct (category_id, category_code) from events
# category_id should not be NULL as PK
categories = events[['category_id', 'category_code']].drop_duplicates().dropna(subset=['category_id'])
for _, row in categories.iterrows():
    cur.execute("""
        INSERT INTO categories (category_id, category_code)
        VALUES (%s, %s) ON CONFLICT (category_id) DO NOTHING
    """, (row['category_id'], row['category_code']))
conn.commit()

print("Session table")
# 5. Session: distinct (user_session, user_id) from events
# user_session should not be NULL as PK
sessions = events[['user_session', 'user_id']].drop_duplicates().dropna(subset=['user_session'])
for _, row in sessions.iterrows():
    cur.execute("""
        INSERT INTO session (user_session, user_id)
        VALUES (%s, %s) ON CONFLICT (user_session) DO NOTHING
    """, (row['user_session'], row['user_id']))
conn.commit()

print("Event table")
# 6. Event: all rows from events
for _, row in events.iterrows():
    # Insert event (user_session must exist)
    cur.execute("""
        INSERT INTO event (event_time, event_type, product_id, category_id, brand, price, user_session)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """, (row['event_time'], row['event_type'], row['product_id'],
          row['category_id'], row['brand'], row['price'], row['user_session']))
conn.commit()

print("First purchase table")
# 7. First purchase
for _, row in first_purchase.iterrows():    
    cur.execute("""
        INSERT INTO first_purchase (client_id, user_id, user_device_id, first_purchase_date)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (client_id) DO NOTHING
    """, (row['client_id'], row['user_id'], row['user_device_id'], none_converter(row['first_purchase_date'])))
conn.commit()

print("Campaign table")
# 8. Campaign
for _, row in campaigns.iterrows():
    cur.execute("""
        INSERT INTO campaign VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        ) ON CONFLICT (id) DO NOTHING
    """, tuple(none_converter(row[col]) for col in campaigns.columns))
conn.commit()

print("Friends table")
# 9. Friends: ensure friend1 < friend2 and both exist
for _, row in friends.iterrows():
    u1, u2 = row['friend1'], row['friend2']
    if u1 is None or u2 is None:
        continue
    if u1 < u2:
        f1, f2 = u1, u2
    else:
        f1, f2 = u2, u1
    cur.execute("""
        INSERT INTO friends (friend1, friend2) VALUES (%s, %s) ON CONFLICT DO NOTHING
    """, (str(f1), str(f2)))
conn.commit()

print("Messages table")
# 10. Messages
for idx, row in messages.iterrows():
    cur.execute("""
        INSERT INTO messages (
            id, message_id, campaign_id, message_type, client_id, channel, 
            category, platform, email_provider, stream, date, sent_at,
            is_opened, opened_first_time_at, opened_last_time_at,
            is_clicked, clicked_first_time_at, clicked_last_time_at,
            is_unsubscribed, unsubscribed_at,
            is_hard_bounced, hard_bounced_at,
            is_soft_bounced, soft_bounced_at,
            is_complained, complained_at,
            is_blocked, blocked_at,
            is_purchased, purchased_at,
            created_at, updated_at
        ) VALUES (
            %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s,
            %s, %s, %s,
            %s, %s, %s,
            %s, %s,
            %s, %s,
            %s, %s,
            %s, %s,
            %s, %s,
            %s, %s,
            %s, %s
        )
    """, (
        none_converter(row['id']),
        str(none_converter(row['message_id'])),
        none_converter(row['campaign_id']),
        str(none_converter(row['message_type'])),
        none_converter(row['client_id']),
        str(none_converter(row['channel'])),
        str(none_converter(row['category'])),
        str(none_converter(row['platform'])),
        str(none_converter(row['email_provider'])),
        str(none_converter(row['stream'])),
        none_converter(row['date']),
        none_converter(row['sent_at']),
        none_converter(row['is_opened']),
        none_converter(row['opened_first_time_at']),
        none_converter(row['opened_last_time_at']),
        none_converter(row['is_clicked']),
        none_converter(row['clicked_first_time_at']),
        none_converter(row['clicked_last_time_at']),
        none_converter(row['is_unsubscribed']),
        none_converter(row['unsubscribed_at']),
        none_converter(row['is_hard_bounced']),
        none_converter(row['hard_bounced_at']),
        none_converter(row['is_soft_bounced']),
        none_converter(row['soft_bounced_at']),
        none_converter(row['is_complained']),
        none_converter(row['complained_at']),
        none_converter(row['is_blocked']),
        none_converter(row['blocked_at']),
        none_converter(row['is_purchased']),
        none_converter(row['purchased_at']),
        none_converter(row['created_at']),
        none_converter(row['updated_at'])
    ))
conn.commit()

cur.close()
conn.close()