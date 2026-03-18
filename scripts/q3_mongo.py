import json
import os
import re
import pandas as pd
from pymongo import MongoClient
from tabulate import tabulate

def run_q3():
    client = MongoClient('mongodb://localhost:27017/')
    db = client['bd_a2']

    script_path = './q3.js'
    with open(script_path, 'r') as f:
        pipeline = json.load(f)

    # Get base products from the pipeline
    base_products = list(db.events.aggregate(pipeline))
    if not base_products:
        print("No base products to expand from")
        client.close()
        return

    results = []
    for bp in base_products:
        original_id = bp['original_product_id']
        raw_keywords = bp['original_category_code']
        if not raw_keywords:
            continue

        # Clean keywords for $text search
        clean_keywords = re.sub(r'[^a-zA-Z0-9]+', ' ', raw_keywords)

        # Find similar products using $text
        similar = list(db.events.aggregate([
            { '$match': {
                '$text': { '$search': clean_keywords },
                'product.product_id': { '$ne': original_id }
            } },
            { '$group': {
                '_id': '$product.product_id',
                'category_code': { '$first': '$product.category_code' },
                'brand': { '$first': '$product.brand' },
                'score': { '$sum': { '$meta': 'textScore' } }
            } },
            { '$sort': { 'score': -1 } },
            { '$limit': 5 }
        ]))

        for s in similar:
            results.append({
                'original_product_id': original_id,
                'original_category_code': raw_keywords,
                'similar_product_id': s['_id'],
                'similar_category_code': s['category_code'],
                'similar_brand': s['brand'],
                'rank': round(s['score'], 4)
            })

    # Print table
    if results:
        df = pd.DataFrame(results)
        print(tabulate(df, headers='keys', tablefmt='mongo', showindex=False))
        df.to_csv("../output/q3_mongo.csv", index=False)
    else:
        print("No similar products found")

    client.close()

if __name__ == '__main__':
    run_q3()