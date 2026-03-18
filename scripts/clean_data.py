#!/usr/bin/env python3
import pandas as pd
import os
from pathlib import Path
import numpy as np

# Configuration
RAW_DATA_DIR = "./data/f11_raw"
CLEANED_DATA_DIR = "./data/f11_cleaned"
Path(CLEANED_DATA_DIR).mkdir(exist_ok=True)

# 0. Read raw data
campaigns = pd.read_csv(os.path.join(RAW_DATA_DIR, 'campaigns.csv'))
first_purchase = pd.read_csv(os.path.join(RAW_DATA_DIR, 'client_first_purchase_date.csv'))
events = pd.read_csv(os.path.join(RAW_DATA_DIR, 'events.csv'))
friends = pd.read_csv(os.path.join(RAW_DATA_DIR, 'friends.csv'))
messages = pd.read_csv(os.path.join(RAW_DATA_DIR, 'messages.csv'))

# 1. Clean events.csv
events = events.drop_duplicates()
# Convert event_time to datetime
events['event_time'] = pd.to_datetime(events['event_time'], utc=True, errors='coerce')

# Fill missing category_code, where exists code for same category_id
events["category_code"] = events.groupby("category_id")["category_code"].transform('first')

# Ensure price is float
events['price'] = events['price'].astype("Float32")
events['user_id'] = events['user_id'].astype(str)
events.to_csv(os.path.join(CLEANED_DATA_DIR, 'events.csv'), index=False)

# 2. Clean campaigns.csv
campaigns = campaigns.drop_duplicates()
# Convert boolean-like columns (empty string = NaN)
bool_cols = ['ab_test', 'warmup_mode', 'subject_with_personalization', 'subject_with_deadline',
             'subject_with_emoji', 'subject_with_bonuses', 'subject_with_discount',
             'subject_with_saleout', 'is_test']
for col in bool_cols:
    campaigns[col] = campaigns[col].apply(lambda x: pd.NA if pd.isna(x) else str(x).lower() == 'true')

# Convert timestamps
campaigns['started_at'] = pd.to_datetime(campaigns['started_at'], utc=True, errors='coerce')
campaigns['finished_at'] = pd.to_datetime(campaigns['finished_at'], utc=True, errors='coerce')

# Convert numeric columns
campaigns['total_count'] = pd.to_numeric(campaigns['total_count'], errors='coerce').astype("Int64")
campaigns['hour_limit'] = pd.to_numeric(campaigns['hour_limit'], errors='coerce')
campaigns['subject_length'] = pd.to_numeric(campaigns['subject_length'], errors='coerce')
campaigns['position'] = pd.to_numeric(campaigns['position'], errors='coerce').astype("Int64")

campaigns.to_csv(os.path.join(CLEANED_DATA_DIR, 'campaigns.csv'), index=False)

# 3. Clean messages.csv
messages = messages.drop_duplicates()
# Convert date and timestamp columns
messages['date'] = pd.to_datetime(messages['date'])
messages['sent_at'] = pd.to_datetime(messages['sent_at'], utc=True, errors='coerce')

# Boolean flags
bool_msg_cols = ['is_opened', 'is_clicked', 'is_unsubscribed', 'is_hard_bounced',
                 'is_soft_bounced', 'is_complained', 'is_blocked', 'is_purchased']
for col in bool_msg_cols:
    messages[col] = messages[col].astype("bool")

# Timestamp columns for each event
ts_cols = ['opened_first_time_at', 'opened_last_time_at', 'clicked_first_time_at',
           'clicked_last_time_at', 'unsubscribed_at', 'hard_bounced_at',
           'soft_bounced_at', 'complained_at', 'blocked_at', 'purchased_at']
for col in ts_cols:
    messages[col] = pd.to_datetime(messages[col], utc=True, errors='coerce')

messages['client_id'] = messages['client_id'].astype(str)
messages['user_id'] = messages['user_id'].astype(str)
messages['message_id'] = messages['message_id'].astype(str)
messages['message_type'] = messages['message_type'].astype(str)
messages['channel'] = messages['channel'].astype(str)
messages['category'] = messages['category'].astype(str)
messages['platform'] = messages['platform'].astype(str)
messages['email_provider'] = messages['email_provider'].astype(str)
messages['stream'] = messages['stream'].astype(str)
messages.to_csv(os.path.join(CLEANED_DATA_DIR, 'messages.csv'), index=False)

# 4. Clean client_first_purchase_date.csv
first_purchase = first_purchase.drop_duplicates()

# Fullfill client_ids from messages
m_unique = set(messages['client_id'].unique())
fp_unique = set(first_purchase['client_id'].unique())
m_fp_difference = m_unique.difference(fp_unique)
messages_diff = messages[messages['client_id'].isin(m_fp_difference)][['client_id', 'user_device_id', 'user_id']]
first_purchase = pd.concat([first_purchase, messages_diff], ignore_index=True)

first_purchase['client_id'] = first_purchase['client_id'].astype(str)
first_purchase['user_id'] = first_purchase['user_id'].astype(str)
first_purchase['user_device_id'] = first_purchase['user_device_id'].astype(str)
first_purchase['first_purchase_date'] = pd.to_datetime(first_purchase['first_purchase_date'], utc=True, errors='coerce')
first_purchase.to_csv(os.path.join(CLEANED_DATA_DIR, 'client_first_purchase_date.csv'), index=False)

# 5. Clean friends.csv
# Swap friend1 and friend2 by sorting them to remove duplicated rows
friends[['friend1', 'friend2']] = np.sort(friends[['friend1', 'friend2']], axis=1)
friends = friends.drop_duplicates()

friends['friend1'] = friends['friend1'].astype(str)
friends['friend2'] = friends['friend2'].astype(str)

friends.to_csv(os.path.join(CLEANED_DATA_DIR, 'friends.csv'), index=False)