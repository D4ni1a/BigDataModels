import subprocess
import sys
import time
import statistics
from pathlib import Path

# Configuration for each query: (name, db, script_filename, number_of_runs)
QUERIES = [
    ('q1_psql', 'PostgreSQL', 'q1_psql.py', 5),
    ('q1_mongo', 'MongoDB', 'q1_mongo.py', 5),
    ('q1_neo4j', 'Neo4j', 'q1_neo4j.py', 5),
    ('q2_psql', 'PostgreSQL', 'q2_psql.py', 5),
    ('q2_mongo', 'MongoDB', 'q2_mongo.py', 5),
    ('q2_neo4j', 'Neo4j', 'q2_neo4j.py', 5),
    ('q3_psql', 'PostgreSQL', 'q3_psql.py', 5),
    ('q3_mongo', 'MongoDB', 'q3_mongo.py', 5),
    ('q3_neo4j', 'Neo4j', 'q3_neo4j.py', 5),
]

def run_script(script_path):
    """Run a Python script and return its elapsed time in seconds."""
    start = time.perf_counter()
    # Run script, hide output (optional: remove capture to see output)
    result = subprocess.run(
        [sys.executable, str(script_path)],
        capture_output=True,
        text=True
    )
    end = time.perf_counter()
    if result.returncode != 0:
        print(f"Error in {script_path}: {result.stderr}")
    return end - start

def main():
    results = []

    # Process each query in order
    for qname, dbname, script_file, runs in QUERIES:
        script_path = Path(__file__).parent / script_file
        if not script_path.exists():
            print(f"Script {script_path} not found, skipping.")
            continue

        times = []
        print(f"\n--- {qname} ({dbname}) ---")
        for i in range(1, runs + 1):
            print(f"  Run {i}/{runs}...", end=' ', flush=True)
            elapsed = run_script(script_path)
            times.append(elapsed)
            print(f"done in {elapsed:.3f}s")

        # Compute statistics
        avg = statistics.mean(times) if times else 0
        std = statistics.stdev(times) if len(times) > 1 else 0.0

        results.append({
            'Query': qname,
            'Database': dbname,
            'Run1': times[0] if len(times) > 0 else None,
            'Run2': times[1] if len(times) > 1 else None,
            'Run3': times[2] if len(times) > 2 else None,
            'Run4': times[3] if len(times) > 3 else None,
            'Run5': times[4] if len(times) > 4 else None,
            'Average': avg,
            'StdDev': std
        })

    # Print final summary table
    print("\n" + "="*80)
    print("FINAL STATISTICS")
    print("="*80)
    # Determine column widths
    headers = ['Query', 'Database', 'Run1', 'Run2', 'Run3', 'Run4', 'Run5', 'Average', 'StdDev']
    col_widths = {h: len(h) for h in headers}
    for row in results:
        for h in headers:
            val = row.get(h)
            if val is None:
                s = 'N/A'
            elif isinstance(val, float):
                s = f"{val:.3f}"
            else:
                s = str(val)
            col_widths[h] = max(col_widths[h], len(s))

    # Print header
    header_line = ' | '.join(h.ljust(col_widths[h]) for h in headers)
    print(header_line)
    print('-' * len(header_line))

    # Print rows
    for row in results:
        row_line = []
        for h in headers:
            val = row.get(h)
            if val is None:
                s = 'N/A'
            elif isinstance(val, float):
                s = f"{val:.3f}"
            else:
                s = str(val)
            row_line.append(s.ljust(col_widths[h]))
        print(' | '.join(row_line))

if __name__ == '__main__':
    main()