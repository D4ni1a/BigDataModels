from pymongo import MongoClient, ASCENDING, DESCENDING, TEXT
import pandas as pd
import os
import re

CLEANED_DATA_DIR = "../data/f11_cleaned"

# 1. Load cleaned data
campaigns = pd.read_csv(os.path.join(CLEANED_DATA_DIR, 'campaigns.csv'))
first_purchase = pd.read_csv(os.path.join(CLEANED_DATA_DIR, 'client_first_purchase_date.csv'))
events = pd.read_csv(os.path.join(CLEANED_DATA_DIR, 'events.csv'))
friends = pd.read_csv(os.path.join(CLEANED_DATA_DIR, 'friends.csv'))
messages = pd.read_csv(os.path.join(CLEANED_DATA_DIR, 'messages.csv'))

print("Building DB")
# 2. Connect to MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client['bd_a2']

# Drop existing collections
db.users.drop()
db.campaigns.drop()
db.messages.drop()
db.events.drop()

print("Campaigns table")
# 3. Campaigns
campaigns_collection = db.campaigns
campaigns_collection.create_index('campaign_id', unique=True)

campaigns_data = []
for _, row in campaigns.iterrows():
    if pd.notna(row['id']):
        campaign_doc = {
            'campaign_id': {
                'id':int(row['id']),
                'campaign_type': row['campaign_type'] if pd.notna(row['campaign_type']) else None
            },
            'channel': row['channel'] if pd.notna(row['channel']) else None,
            'topic': row['topic'] if pd.notna(row['topic']) else None,
            'started_at': row['started_at'] if pd.notna(row['started_at']) else None,
            'finished_at': row['finished_at'] if pd.notna(row['finished_at']) else None,
            'total_count': int(row['total_count']) if pd.notna(row['total_count']) else None,
            'ab_test': bool(row['ab_test']) if pd.notna(row['ab_test']) else None,
            'warmup_mode': bool(row['warmup_mode']) if pd.notna(row['warmup_mode']) else None,
            'hour_limit': float(row['hour_limit']) if pd.notna(row['hour_limit']) else None,
            'subject': {
                'length': float(row['subject_length']) if pd.notna(row['subject_length']) else None,
                'with_personalization': bool(row['subject_with_personalization']) if pd.notna(row['subject_with_personalization']) else None,
                'with_deadline': bool(row['subject_with_deadline']) if pd.notna(row['subject_with_deadline']) else None,
                'with_emoji': bool(row['subject_with_emoji']) if pd.notna(row['subject_with_emoji']) else None,
                'with_bonuses': bool(row['subject_with_bonuses']) if pd.notna(row['subject_with_bonuses']) else None,
                'with_discount': bool(row['subject_with_discount']) if pd.notna(row['subject_with_discount']) else None,
                'with_saleout': bool(row['subject_with_saleout']) if pd.notna(row['subject_with_saleout']) else None
            },
            'is_test': bool(row['is_test']) if pd.notna(row['is_test']) else None,
            'position': int(row['position']) if pd.notna(row['position']) else None
        }
        campaigns_data.append(campaign_doc)

if campaigns_data:
    campaigns_collection.insert_many(campaigns_data)

# Create campaign lookup for later denormalization in Messages
campaign_lookup = {}
for camp in campaigns_data:
    campaign_lookup[camp['campaign_id']['id']] = {
        'type': camp['campaign_id']['campaign_type'],
        'channel': camp['channel'],
        'topic': camp['topic']
    }

print("Users table")
# 4. Users (with embedded friends and first_purchases)
users_collection = db.users
users_collection.create_index('user_id', unique=True)
users_collection.create_index('first_purchases.client_id')
users_collection.create_index('friends')

# 4.1. Collect all unique user IDs from all sources
user_ids = set()
# 4.1.1. from first_purchase
user_ids.update(first_purchase['user_id'].dropna().unique())

# 4.1.2. from events
user_ids.update(events['user_id'].dropna().unique())

# 4.1.3. from messages
user_ids.update(messages['user_id'].dropna().unique())

# 4.1.4. from friends (both columns)
user_ids.update(friends['friend1'].dropna().unique())
user_ids.update(friends['friend2'].dropna().unique())

# 4.2. Build friends mapping
friends_map = {}
for _, row in friends.iterrows():
    if pd.notna(row['friend1']) and pd.notna(row['friend2']):
        u1, u2 = str(row['friend1']), str(row['friend2'])
        
        if u1 not in friends_map:
            friends_map[u1] = set()
        friends_map[u1].add(u2)
        
        if u2 not in friends_map:
            friends_map[u2] = set()
        friends_map[u2].add(u1)

# 4.3. Build first purchases mapping (by user_id)
first_purchases_map = {}
for _, row in first_purchase.iterrows():
    if pd.notna(row['client_id']):
        client_id = str(row['client_id'])
        user_id = str(row['user_id'])
        
        if user_id:
            if user_id not in first_purchases_map:
                first_purchases_map[user_id] = []
            
            first_purchases_map[user_id].append({
                'client_id': client_id,
                'user_device_id': row['user_device_id'] if pd.notna(row['user_device_id']) else None,
                'first_purchase_date': row['first_purchase_date'] if pd.notna(row['first_purchase_date']) else None
            })

# 4.4. Create user documents
users_data = []
for user_id in user_ids:
    user_id_str = str(user_id)
    user_doc = {
        'user_id': user_id_str,
        'friends': list(friends_map.get(user_id_str, set())),
        'first_purchases': first_purchases_map.get(user_id_str, [])
    }
    users_data.append(user_doc)

# 4.5. Insert users in batches
batch_size = 1000
for i in range(0, len(users_data), batch_size):
    batch = users_data[i:i+batch_size]
    users_collection.insert_many(batch)

print("Messages table")
# 5. Messages (with denormalized campaign info)
messages_collection = db.messages
messages_collection.create_index('id', unique=True)
messages_collection.create_index('message_id')
messages_collection.create_index('campaign_id')
messages_collection.create_index('user_id')
messages_collection.create_index('client_id')
messages_collection.create_index('sent_at')
messages_collection.create_index([('user_id', ASCENDING), ('sent_at', DESCENDING)])
messages_collection.create_index([('campaign_id', ASCENDING), ('flags.is_purchased', ASCENDING)])
messages_collection.create_index([('user_id', ASCENDING), ('timing.sent_at', DESCENDING)])

messages_data = []
for _, row in messages.iterrows():
    if pd.notna(row['id']):
        campaign_id = int(row['campaign_id']) if pd.notna(row['campaign_id']) else None
        campaign_info = campaign_lookup.get(campaign_id, {})
        
        client_id = str(row['client_id']) if pd.notna(row['client_id']) else None
        user_id = str(row['user_id']) if pd.notna(row['user_id']) else None
        user_device_id = str(row['user_device_id']) if pd.notna(row['user_device_id']) else None
        
        message_doc = {
            'id': int(row['id']),
            'message_id': row['message_id'] if pd.notna(row['message_id']) else None,
            'campaign_id': campaign_id,
            # Denormalized campaign info
            'campaign': {
                'type': campaign_info.get('type'),
                'channel': campaign_info.get('channel'),
                'topic': campaign_info.get('topic')
            },
            'message_type': row['message_type'] if pd.notna(row['message_type']) else None,
            'client_id': client_id,
            'user_id': user_id,  # For easy joining
            'user_device_id': user_device_id,  # For easy joining
            'channel': row['channel'] if pd.notna(row['channel']) else None,
            'platform': row['platform'] if pd.notna(row['platform']) else None,
            'email_provider': row['email_provider'] if pd.notna(row['email_provider']) else None,
            'stream': row['stream'] if pd.notna(row['stream']) else None,
            'timing': {
                'date': row['date'] if pd.notna(row['date']) else None,
                'sent_at': row['sent_at'] if pd.notna(row['sent_at']) else None,
                'opened_first_time': row['opened_first_time_at'] if pd.notna(row['opened_first_time_at']) else None,
                'opened_last_time': row['opened_last_time_at'] if pd.notna(row['opened_last_time_at']) else None,
                'clicked_first_time': row['clicked_first_time_at'] if pd.notna(row['clicked_first_time_at']) else None,
                'clicked_last_time': row['clicked_last_time_at'] if pd.notna(row['clicked_last_time_at']) else None,
                'unsubscribed_at': row['unsubscribed_at'] if pd.notna(row['unsubscribed_at']) else None,
                'hard_bounced_at': row['hard_bounced_at'] if pd.notna(row['hard_bounced_at']) else None,
                'soft_bounced_at': row['soft_bounced_at'] if pd.notna(row['soft_bounced_at']) else None,
                'complained_at': row['complained_at'] if pd.notna(row['complained_at']) else None,
                'blocked_at': row['blocked_at'] if pd.notna(row['blocked_at']) else None,
                'purchased_at': row['purchased_at'] if pd.notna(row['purchased_at']) else None
            },
            'flags': {
                'is_opened': bool(row['is_opened']) if pd.notna(row['is_opened']) else None,
                'is_clicked': bool(row['is_clicked']) if pd.notna(row['is_clicked']) else None,
                'is_unsubscribed': bool(row['is_unsubscribed']) if pd.notna(row['is_unsubscribed']) else None,
                'is_hard_bounced': bool(row['is_hard_bounced']) if pd.notna(row['is_hard_bounced']) else None,
                'is_soft_bounced': bool(row['is_soft_bounced']) if pd.notna(row['is_soft_bounced']) else None,
                'is_complained': bool(row['is_complained']) if pd.notna(row['is_complained']) else None,
                'is_blocked': bool(row['is_blocked']) if pd.notna(row['is_blocked']) else None,
                'is_purchased': bool(row['is_purchased']) if pd.notna(row['is_purchased']) else None
            },
            'metadata': {
                'created_at': row['created_at'] if pd.notna(row['created_at']) else None,
                'updated_at': row['updated_at'] if pd.notna(row['updated_at']) else None
            }
        }
        messages_data.append(message_doc)

# 5.1. Insert in batches
for i in range(0, len(messages_data), 1000):
    batch = messages_data[i:i+1000]
    messages_collection.insert_many(batch)

print("Events table")
# 6. Events (with session_id and denormalized product)
events_collection = db.events
events_collection.create_index([('user_id', ASCENDING), ('event_time', DESCENDING)])
events_collection.create_index([('session_id', ASCENDING), ('event_time', DESCENDING)])
events_collection.create_index([('product.product_id', ASCENDING)])
events_collection.create_index('event_type')
events_collection.create_index('event_time')
events_collection.create_index('session_id')
# Text index for search
events_collection.create_index([('product.category_code', TEXT), ('product.brand', TEXT)])

events_data = []
for _, row in events.iterrows():
    if pd.notna(row['user_id']) and pd.notna(row['user_session']):
        event_doc = {
            'event_time': row['event_time'] if pd.notna(row['event_time']) else None,
            'event_type': row['event_type'] if pd.notna(row['event_type']) else None,
            'user_id': str(row['user_id']),
            'session_id': row['user_session'],  # This is the session_id
            'product': {
                'product_id': int(row['product_id']) if pd.notna(row['product_id']) else None,
                'category_id': int(row['category_id']) if pd.notna(row['category_id']) else None,
                'category_code': row['category_code'] if pd.notna(row['category_code']) else None,
                'brand': row['brand'] if pd.notna(row['brand']) else None,
                'price': float(row['price']) if pd.notna(row['price']) else None
            }
        }
        events_data.append(event_doc)

# 6.1. Insert in batches
for i in range(0, len(events_data), 1000):
    batch = events_data[i:i+1000]
    events_collection.insert_many(batch)

client.close()