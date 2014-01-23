"""
Run an arbitrary SQL query and print the results as CSV.
"""

import psycopg2
import os, sys
import csv

queryfile = sys.argv[1]
args = sys.argv[2:]

query = open(queryfile).read()

connectionstring = open(os.path.expanduser('~/socorro.connection'), 'r').read().strip()

conn = psycopg2.connect(connectionstring)
cur = conn.cursor()

cur.execute(query, args)

csvw = csv.writer(sys.stdout)
for r in cur:
    csvw.writerow(r)
