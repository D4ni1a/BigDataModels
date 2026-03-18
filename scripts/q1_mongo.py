import json
import os
import pandas as pd
from pymongo import MongoClient
from tabulate import tabulate
from collections import defaultdict

def run_q1():
    client = MongoClient('mongodb://localhost:27017/')
    db = client['bd_a2']

    # Load simplified aggregation pipeline
    script_path = './q1.js'
    with open(script_path, 'r') as f:
        pipeline = json.load(f)

    # Execute aggregation to get campaign counts
    campaign_stats = list(db.messages.aggregate(pipeline))

    results = []
    for camp in campaign_stats:
        campaign_id = camp['campaign_id']
        total_sent = camp['total_sent']
        total_purchased = camp['total_purchased']

        if total_purchased == 0:
            results.append({
                'campaign_id': campaign_id,
                'total_sent': total_sent,
                'total_purchased': 0,
                'conversion_rate_pct': 0.0,
                'purchasers_with_friend_purchased': 0,
                'pct_of_purchasers_with_friend': 0.0
            })
            continue

        # Get the list of purchasers for this campaign
        purchasers = db.messages.distinct('user_id', {
            'campaign_id': campaign_id,
            'flags.is_purchased': True
        })

        purchaser_set = set(purchasers)
        count_with_friend = 0

        for uid in purchasers:
            user = db.users.find_one({'user_id': uid}, {'friends': 1})
            if user and user.get('friends'):
                if any(friend in purchaser_set for friend in user['friends']):
                    count_with_friend += 1

        conv_rate = (total_purchased / total_sent * 100) if total_sent else 0
        pct_with_friend = (count_with_friend / total_purchased * 100) if total_purchased else 0

        results.append({
            'campaign_id': campaign_id,
            'total_sent': total_sent,
            'total_purchased': total_purchased,
            'conversion_rate_pct': round(conv_rate, 2),
            'purchasers_with_friend_purchased': count_with_friend,
            'pct_of_purchasers_with_friend': round(pct_with_friend, 2)
        })

    # Sort by conversion rate descending
    results.sort(key=lambda x: x['conversion_rate_pct'], reverse=True)

    # Print table
    if results:
        df = pd.DataFrame(results)
        print(tabulate(df, headers='keys', tablefmt='mongo', showindex=False))
        df.to_csv("../output/q1_mongo.csv", index=False)
    else:
        print("No data")
        pass

    client.close()

if __name__ == '__main__':
    run_q1()