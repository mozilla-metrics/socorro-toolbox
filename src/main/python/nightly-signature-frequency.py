from optparse import OptionParser
import dateoption
from datetime import date, timedelta
import psycopg2
import os, sys
import csv

p = OptionParser(option_class=dateoption.OptionWithDate)
p.add_option('-s', '--start-date', dest='startdate', type='date',
             help='Start date', default=date.today() - timedelta(days=14))
p.add_option('-e', '--end-date', dest='enddate', type='date',
             help='End date', default=date.today())
p.add_option('-c', '--channel', dest='channel', type='string',
             help='Channel name', default='nightly')

opts, (signature,) = p.parse_args()

channels = {
    'nightly': 'mozilla-central',
    'aurora': 'mozilla-aurora',
}

if opts.channel not in channels:
    print >>sys.stderr, "Channel '%s' unknown: add it to the channels map" % opts.channel
    sys.exit(2)

connectionstring = open(os.path.expanduser('~/socorro.connection'), 'r').read().strip()

conn = psycopg2.connect(connectionstring)
cur = conn.cursor()

cur.execute('''
WITH build_adus AS (
  SELECT build, SUM(adu_count) AS aducount
  FROM raw_adu
  WHERE
    product_name = 'Firefox' AND
    product_os_platform = 'Windows' AND
    build_channel = %(channel)s AND
    date >= %(startdate)s AND date <= %(enddate)s AND
    date < to_date(substring(build from 1 for 8), 'YYYYMMDD') + interval '10 days'
  GROUP BY build
),
sigreports AS (
  SELECT build, COUNT(*) AS crashcount
  FROM
    reports_clean
    JOIN signatures ON reports_clean.signature_id = signatures.signature_id
    JOIN product_versions ON reports_clean.product_version_id = product_versions.product_version_id
  WHERE
    date_processed >= %(startdate)s AND date_processed <= %(enddate)s + interval '10 days' AND
    signature = %(signature)s AND
    product_name = 'Firefox'
  GROUP BY build
)
SELECT
  build_id,
  (
    SELECT aducount FROM build_adus
    WHERE build_adus.build = releases_raw.build_id::text
  ) AS aducount,
  (
    SELECT crashcount FROM sigreports
    WHERE sigreports.build = releases_raw.build_id
  ) AS crashcount
FROM releases_raw
WHERE
  product_name = 'firefox' AND
  platform = 'win32' AND
  build_type = %(studlyChannel)s AND
  repository = %(repository)s AND
  to_date(substring(build_id::text from 1 for 8), 'YYYYMMDD') BETWEEN %(startdate)s AND %(enddate)s
ORDER BY build_id
  ''', {'startdate': opts.startdate,
        'enddate': opts.enddate,
        'signature': signature,
        'channel': opts.channel,
        'studlyChannel': opts.channel.capitalize(),
        'repository': channels[opts.channel]})

csvw = csv.writer(sys.stdout)

for r in cur:
    csvw.writerow(r)
