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
    # Read query from file
    script_path = './q1.cypherl'
    with open(script_path, 'r') as f:
        query = f.read()

    # Connect to Neo4j
    uri = "bolt://localhost:7687"
    user = ""
    password = ""
    runner = Neo4jRunner(uri, user, password)

    try:
        results = runner.run_query(query)
        if results:
            headers = list(results[0].keys())
            print('\t'.join(headers))
            for row in results:
                print('\t'.join(str(row[h]) for h in headers))
        else:
            print("No results")
    finally:
        runner.close()

if __name__ == '__main__':
    main()