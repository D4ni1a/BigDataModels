from arango import ArangoClient
import pandas as pd
import os

CLEANED_DATA_DIR = "../data/f11_cleaned"

# 1. Load cleaned data
campaigns = pd.read_csv(os.path.join(CLEANED_DATA_DIR, 'campaigns.csv'))
first_purchase = pd.read_csv(os.path.join(CLEANED_DATA_DIR, 'client_first_purchase_date.csv'))
events = pd.read_csv(os.path.join(CLEANED_DATA_DIR, 'events.csv'))
friends = pd.read_csv(os.path.join(CLEANED_DATA_DIR, 'friends.csv'))
messages = pd.read_csv(os.path.join(CLEANED_DATA_DIR, 'messages.csv'))

# Helper function to handle NaN -> None
def none_converter(value):
    if pd.isna(value):
        return None
    return value

def to_bool(val):
    v = none_converter(val)
    if v is None:
        return None
    if isinstance(v, bool):
        return v
    if isinstance(v, str):
        return v.lower() in ('true', 't', 'yes', 'y', '1')
    return bool(v)

# Connect to ArangoDB (default: localhost:8529)
client = ArangoClient(hosts='http://localhost:8529')
sys_db = client.db('_system', username='root', password='test')

# Create a new database (drop if exists for clean run)
db_name = 'bd_a2'
if sys_db.has_database(db_name):
    sys_db.delete_database(db_name)
sys_db.create_database(db_name)

# sys_db.update_permission(username='root', database=db_name, permission='rw')

# Switch to the new database
db = client.db(db_name, username='root', password='test')

print("Creating collections...")

# Users collection (document collection)
users_col = db.create_collection('users')
# No need to define schema; we'll insert documents with _key = user_id

# Categories collection
categories_col = db.create_collection('categories')

# Campaign collection
campaign_col = db.create_collection('campaign')

# First purchase collection
first_purchase_col = db.create_collection('first_purchase')

# Events collection (instead of events_by_user table)
events_col = db.create_collection('events')
# Create index on user_id for faster queries
events_col.add_hash_index(fields=['user_id'], unique=False)

# Friends edge collection (graph representation)
friends_col = db.create_collection('friends', edge=True)

# Messages collection
messages_col = db.create_collection('messages')
# Create indexes on frequently queried fields
messages_col.add_hash_index(fields=['campaign_id'], unique=False)
messages_col.add_hash_index(fields=['client_id'], unique=False)

print("Users collection")
# Build friends mapping
friends_map = {}
for _, row in friends.iterrows():
    u1, u2 = none_converter(row['friend1']), none_converter(row['friend2'])
    if u1 is not None and u2 is not None:
        u1_str, u2_str = str(u1), str(u2)
        if u1_str not in friends_map:
            friends_map[u1_str] = set()
        friends_map[u1_str].add(u2_str)
        if u2_str not in friends_map:
            friends_map[u2_str] = set()
        friends_map[u2_str].add(u1_str)

# Collect all unique user IDs
user_ids = set()
user_ids.update(first_purchase['user_id'].dropna().unique())
user_ids.update(events['user_id'].dropna().unique())
user_ids.update(messages['user_id'].dropna().unique())
user_ids.update(friends['friend1'].dropna().unique())
user_ids.update(friends['friend2'].dropna().unique())

# Insert user documents
for uid in user_ids:
    uid_str = str(uid)
    users_col.insert({
        '_key': uid_str,
        'friends': list(friends_map.get(uid_str, set()))
    }, overwrite=True)

print("Categories collection")
categories = events[['category_id', 'category_code']].drop_duplicates().dropna(subset=['category_id'])
for _, row in categories.iterrows():
    categories_col.insert({
        '_key': str(int(row['category_id'])),
        'category_code': none_converter(row['category_code'])
    }, overwrite=True)

print("Campaign collection")
for _, row in campaigns.iterrows():
    if none_converter(row['id']):
        campaign_col.insert({
            '_key': str(int(row['id'])),
            'campaign_type': none_converter(row['campaign_type']),
            'channel': none_converter(row['channel']),
            'topic': none_converter(row['topic']),
            'started_at': none_converter(row['started_at']),
            'finished_at': none_converter(row['finished_at']),
            'total_count': none_converter(row['total_count']),
            'ab_test': to_bool(row['ab_test']),
            'warmup_mode': to_bool(row['warmup_mode']),
            'hour_limit': none_converter(row['hour_limit']),
            'subject_length': none_converter(row['subject_length']),
            'subject_with_personalization': to_bool(row['subject_with_personalization']),
            'subject_with_deadline': to_bool(row['subject_with_deadline']),
            'subject_with_emoji': to_bool(row['subject_with_emoji']),
            'subject_with_bonuses': to_bool(row['subject_with_bonuses']),
            'subject_with_discount': to_bool(row['subject_with_discount']),
            'subject_with_saleout': to_bool(row['subject_with_saleout']),
            'is_test': to_bool(row['is_test']),
            'position': none_converter(row['position'])
        }, overwrite=True)

print("First purchase collection")
for _, row in first_purchase.iterrows():
    first_purchase_col.insert({
        '_key': str(row['client_id']),
        'user_id': str(row['user_id']),
        'user_device_id': none_converter(row['user_device_id']),
        'first_purchase_date': none_converter(row['first_purchase_date'])
    }, overwrite=True)

print("Events collection")
# Insert events in batches (ArangoDB can handle many inserts, but we'll do batch for performance)
batch = []
batch_size = 500
for _, row in events.iterrows():
    user_id = none_converter(row['user_id'])
    if user_id:
        doc = {
            'user_id': str(user_id),
            'event_time': none_converter(row['event_time']),
            'event_type': none_converter(row['event_type']),
            'session_id': none_converter(row['user_session']),
            'product_id': none_converter(row['product_id']),
            'category_id': none_converter(row['category_id']),
            'brand': none_converter(row['brand']),
            'price': float(row['price']) if none_converter(row['price']) else None
        }
        batch.append(doc)
        if len(batch) >= batch_size:
            events_col.insert_many(batch)
            batch = []
if batch:
    events_col.insert_many(batch)

print("Friends edge collection")
# Insert undirected friendships as edges
batch = []
for _, row in friends.iterrows():
    u1, u2 = none_converter(row['friend1']), none_converter(row['friend2'])
    if u1 is not None and u2 is not None:
        u1_str, u2_str = str(u1), str(u2)
        # Store edge with _from and _to pointing to user documents
        # Ensure consistent ordering to avoid duplicates
        if u1_str < u2_str:
            from_id = f'users/{u1_str}'
            to_id = f'users/{u2_str}'
        else:
            from_id = f'users/{u2_str}'
            to_id = f'users/{u1_str}'
        batch.append({'_from': from_id, '_to': to_id})
        if len(batch) >= batch_size:
            friends_col.insert_many(batch)
            batch = []
if batch:
    friends_col.insert_many(batch)

print("Messages collection")
batch = []
for _, row in messages.iterrows():
    if none_converter(row['id']):
        doc = {
            '_key': str(int(row['id'])),
            'message_id': str(none_converter(row['message_id'])) if none_converter(row['message_id']) else None,
            'campaign_id': none_converter(row['campaign_id']),
            'message_type': str(none_converter(row['message_type'])) if none_converter(row['message_type']) else None,
            'client_id': str(none_converter(row['client_id'])) if none_converter(row['client_id']) else None,
            'channel': str(none_converter(row['channel'])) if none_converter(row['channel']) else None,
            'category': str(none_converter(row['category'])) if none_converter(row['category']) else None,
            'platform': str(none_converter(row['platform'])) if none_converter(row['platform']) else None,
            'email_provider': str(none_converter(row['email_provider'])) if none_converter(row['email_provider']) else None,
            'stream': str(none_converter(row['stream'])) if none_converter(row['stream']) else None,
            'date': none_converter(row['date']),
            'sent_at': none_converter(row['sent_at']),
            'is_opened': none_converter(row['is_opened']),
            'opened_first_time_at': none_converter(row['opened_first_time_at']),
            'opened_last_time_at': none_converter(row['opened_last_time_at']),
            'is_clicked': none_converter(row['is_clicked']),
            'clicked_first_time_at': none_converter(row['clicked_first_time_at']),
            'clicked_last_time_at': none_converter(row['clicked_last_time_at']),
            'is_unsubscribed': none_converter(row['is_unsubscribed']),
            'unsubscribed_at': none_converter(row['unsubscribed_at']),
            'is_hard_bounced': none_converter(row['is_hard_bounced']),
            'hard_bounced_at': none_converter(row['hard_bounced_at']),
            'is_soft_bounced': none_converter(row['is_soft_bounced']),
            'soft_bounced_at': none_converter(row['soft_bounced_at']),
            'is_complained': none_converter(row['is_complained']),
            'complained_at': none_converter(row['complained_at']),
            'is_blocked': none_converter(row['is_blocked']),
            'blocked_at': none_converter(row['blocked_at']),
            'is_purchased': none_converter(row['is_purchased']),
            'purchased_at': none_converter(row['purchased_at']),
            'created_at': none_converter(row['created_at']),
            'updated_at': none_converter(row['updated_at'])
        }
        batch.append(doc)
        if len(batch) >= batch_size:
            messages_col.insert_many(batch)
            batch = []
if batch:
    messages_col.insert_many(batch)

print("All data loaded successfully!")
client.close()