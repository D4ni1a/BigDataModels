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
    script_path = './q3.cypherl'
    with open(script_path, 'r') as f:
        query = f.read()

    uri = "bolt://localhost:7687"
    user = ""
    password = ""
    runner = Neo4jRunner(uri, user, password)

    target_user = '548885063'  # same user
    params = {'user_id': target_user}

    try:
        results = runner.run_query(query, params)
        if results:
            headers = list(results[0].keys())
            print('\t'.join(headers))
            for row in results:
                print('\t'.join(str(row[h]) for h in headers))
        else:
            print("No similar products found")
    finally:
        runner.close()

if __name__ == '__main__':
    main()