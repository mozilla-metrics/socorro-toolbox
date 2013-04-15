from optparse import OptionParser
import dateoption
from datetime import date, timedelta
import psycopg2
import os, sys
import csv
# import HTML

p = OptionParser(option_class = dateoption.OptionWithDate)
p.add_option('-s', '--start-date', dest='startdate', type='date',
             help='Start date', default=date.today() - timedelta(days=14))
p.add_option('-e', '--end-date', dest='enddate', type='date',
             help='End date', default=date.today())

opts, args = p.parse_args()
if len(args) != 0:
    p.error("No arguments expected")
    sys.exit(1)

connectionstring = open(os.path.expanduser('~/socorro.connection'), 'r').read().strip()

conn = psycopg2.connect(connectionstring)
cur = conn.cursor()

cur.execute('''
  SELECT build, date, SUM(adu_count)
  FROM raw_adu
  WHERE
    product_name = 'Firefox' AND
    product_os_platform = 'Windows' AND
    build_channel = 'nightly' AND
    date BETWEEN %s AND %s AND
    date < to_date(substring(build from 1 for 8), 'YYYYMMDD') + interval '14 days'
  GROUP BY build, date
  ORDER BY build, date
  ''', (opts.startdate.strftime('%Y-%m-%d'), opts.enddate.strftime('%Y-%m-%d')))

csvw = csv.writer(sys.stdout)

for r in cur:
    csvw.writerow(r)
