from optparse import OptionParser
import dateoption
from datetime import date, timedelta
import psycopg2
import os, sys

p = OptionParser(option_class=dateoption.OptionWithDate)
p.add_option('-s', '--start-date', dest='startdate', type='date',
             help='Start date', default=date.today() - timedelta(days=14))
p.add_option('-e', '--end-date', dest='enddate', type='date',
             help='End date', default=date.today())
p.add_option('-c', '--channel', dest='channel', type='string',
             help='Channel name', default='nightly')
p.add_option('-g', '--graph', action="store_true", dest='graph',
             help='Produce graph instead of CSV', default=False)
p.add_option('-b', '--bug', dest="bugs", action="append", type="int",
             default=[], help="Signatures from bug")
p.add_option('--search', dest="search", type="string", default=None,
             help="Signature regex")

opts, signatures = p.parse_args()

for bugid in opts.bugs:
    import signaturesbybug
    signatures.extend(signaturesbybug.signatures_by_bug(bugid))

if opts.search:
    if len(signatures):
        print >>sys.stderr, "Search and signatures cannot both be used"
        sys.exit(2)
    searchterm = "signature ~ (%(search)s)"
else:
    if len(signatures) == 0:
        print >>sys.stderr, "At least one signature must be specified"
        sys.exit(2)
    searchterm = "signature = ANY(%(signatures))"

channels = {
    'nightly': 'mozilla-central',
    'aurora': 'mozilla-aurora',
    'beta': 'mozilla-beta',
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
    date_processed >= %(startdate)s AND date_processed <= %(enddate)s + interval '10 days' AND ''' + searchterm + ''' AND
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
        'signatures': signatures,
        'search': opts.search,
        'channel': opts.channel,
        'studlyChannel': opts.channel.capitalize(),
        'repository': channels[opts.channel]})

if opts.graph:
    import nightly_signature_graph
    builds = map(nightly_signature_graph.RowType, cur)
    label = "Crashes/100ADI for signature(s) %s on the %s channel" % (
        ','.join(signatures), opts.channel)
    nightly_signature_graph.produce_graph(builds, label, sys.stdout)
else:
    import csv
    csvw = csv.writer(sys.stdout)
    for r in cur:
        csvw.writerow(r)
