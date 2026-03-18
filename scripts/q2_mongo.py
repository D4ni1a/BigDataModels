import json
import os
import pandas as pd
from pymongo import MongoClient
from tabulate import tabulate

def run_q2():
    client = MongoClient('mongodb://localhost:27017/')
    db = client['bd_a2']

    script_path = './q2.js'
    with open(script_path, 'r') as f:
        pipeline = json.load(f)

    # Execute aggregation
    cursor = db.events.aggregate(pipeline)

    # Collect recommendations and add rank
    recommendations = []
    for doc in cursor:
        # The pipeline returns one document per recommendation, but rank is missing.
        # We'll simulate rank by grouping in Python (simpler).
        # Alternatively, we could use a more complex pipeline, but this is clearer.
        # For simplicity, we'll collect and then rank.
        recommendations.append(doc)

    # Re‑rank: group by category and assign rank
    from collections import defaultdict
    by_cat = defaultdict(list)
    for rec in recommendations:
        by_cat[rec['category_id']].append(rec)

    final = []
    for cat, items in by_cat.items():
        # Sort by total_interactions descending
        items.sort(key=lambda x: x['total_interactions'], reverse=True)
        for idx, item in enumerate(items, start=1):
            final.append({
                'user_id': item['user_id'],
                'category_id': item['category_id'],
                'product_id': item['product_id'],
                'total_interactions': item['total_interactions'],
                'rank_in_category': idx
            })

    # Print table
    if final:
        df = pd.DataFrame(final)
        print(tabulate(df, headers='keys', tablefmt='mongo', showindex=False))
        df.to_csv("../output/q2_mongo.csv", index=False)
    else:
        print("No recommendations found")

    client.close()

if __name__ == '__main__':
    run_q2()