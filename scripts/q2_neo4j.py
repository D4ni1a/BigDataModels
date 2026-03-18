from neo4j import GraphDatabase
import os

class Neo4jRunner:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def run_query(self, query, parameters=None):
        with self.driver.session() as session:
            result = session.run(query, parameters)
            return [record.data() for record in result]

def main():
    script_path = './q2.cypherl'
    with open(script_path, 'r') as f:
        query = f.read()

    uri = "bolt://localhost:7687"
    user = ""
    password = ""
    runner = Neo4jRunner(uri, user, password)

    target_user = '548885063'
    params = {'user_id': target_user}

    try:
        results = runner.run_query(query, params)
        # Post‑process: assign rank within each category
        from collections import defaultdict
        by_cat = defaultdict(list)
        for rec in results:
            by_cat[rec['category_id']].append(rec)

        final = []
        for cat, items in by_cat.items():
            items.sort(key=lambda x: x['total_interactions'], reverse=True)
            for idx, item in enumerate(items, start=1):
                item['rank_in_category'] = idx
                final.append(item)

        if final:
            headers = ['user_id', 'category_id', 'product_id', 'total_interactions', 'rank_in_category']
            print('\t'.join(headers))
            for row in final:
                print('\t'.join(str(row[h]) for h in headers))
        else:
            print("No recommendations found")
    finally:
        runner.close()

if __name__ == '__main__':
    main()