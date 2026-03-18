#!/bin/bash
echo "Cleaning data..."
python ./clean_data.py

echo "Loading PostgreSQL..."
python ./load_data_psql.py

echo "Loading MongoDB..."
python ./load_data_mongodb.py

echo "Loading Neo4j..."
python ./load_data_graph.py

echo "Running benchmarks..."
python ./analysis_queries.py

echo "Done!"