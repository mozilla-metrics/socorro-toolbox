from optparse import OptionParser
import psycopg2
import os, sys
import csv
import re
from collections import namedtuple

p = OptionParser(usage='usage: %prog [options] outfile')
p.add_option('-V', '--product-version', dest='productversion',
             help='Firefox version', default=None)
p.add_option('-s', '--start-date', dest='startdate',
             help='Start date (YYYY-MM-DD)', default=None)
p.add_option('-e', '--end-date', dest='enddate',
             help='End date (YYYY-MM-DD)', default=None)
p.add_option('-c', '--cutoff', dest='cutoff',
             help="Minimum cutoff", default=50)

opts, args = p.parse_args()

if len(args) != 1 or opts.productversion is None or opts.startdate is None or opts.enddate is None:
    p.error("Required arguments missing")
    sys.exit(1)

datere = re.compile(r'\d{4}-\d{2}-\d{2}$')
if datere.match(opts.startdate) is None or datere.match(opts.enddate) is None:
    p.print_usage()
    sys.exit(1)

outpattern, = args

connectionstring = open(os.path.expanduser('~/socorro.connection'), 'r').read().strip()

conn = psycopg2.connect(connectionstring)
cur = conn.cursor()

cur.execute('''
  SELECT product_version_id
  FROM product_versions
  WHERE product_name = 'Firefox' AND version_string = %s''', (opts.productversion,))
if cur.rowcount != 1:
    print >>sys.stderr, "More than one build has version '%s'" % opts.productversion
    sys.exit(2)

productversion, = cur.fetchone()

# Note: the startdate/enddate are substituted by python into the query string.
# This is required so that the partition optimizer can exclude irrelevant
# partitions. The product version is passed as a bound parameter.

byvendorq = """
WITH BySignatureGraphics AS (
  SELECT
    signature_id,
    substring(r.app_notes from E'AdapterVendorID: (?:0x)?(\\\\w+)') AS AdapterVendorID,
    COUNT(*) AS c
  FROM
    reports r JOIN reports_clean rc ON rc.uuid = r.uuid
  WHERE
    r.date_processed > '%(startdate)s'
    AND r.date_processed < '%(enddate)s'
    AND rc.date_processed > '%(startdate)s'
    AND rc.date_processed < '%(enddate)s'
    AND r.app_notes ~ E'AdapterVendorID: (?:0x)?(\\\\w+)'
    AND r.os_name ~ 'Windows NT'
    AND r.hangid IS NULL
    AND rc.product_version_id = %%(product_version_id)s
  GROUP BY signature_id, AdapterVendorID
),
BySignature AS (
  SELECT
    signature_id,
    SUM(c) AS total_by_signature
  FROM BySignatureGraphics
  GROUP BY signature_id
),
ByGraphics AS (
  SELECT
    AdapterVendorID,
    SUM(c) AS total_by_graphics
  FROM BySignatureGraphics
  GROUP BY AdapterVendorID
),
AllCrashes AS (
  SELECT SUM(c) AS grand_total
  FROM BySignatureGraphics
),
Correlations AS (
  SELECT
    s.signature,
    bsg.AdapterVendorID,
    bsg.c,
    total_by_signature,
    total_by_graphics,
    (bsg.c::float * grand_total::float) /
    (total_by_signature::float * total_by_graphics::float) AS correlation,
    grand_total
  FROM
    BySignatureGraphics AS bsg
    JOIN BySignature ON BySignature.signature_id = bsg.signature_id
    JOIN ByGraphics ON
      ByGraphics.AdapterVendorID = bsg.AdapterVendorID
    CROSS JOIN AllCrashes
    JOIN signatures AS s
      ON bsg.signature_id = s.signature_id
  WHERE
    bsg.c > 25
    AND total_by_signature > %%(cutoff)s
    AND bsg.AdapterVendorID != '0000'
),
HighCorrelation AS (
  SELECT
    signature,
    MAX(correlation) AS highcorrelation
  FROM Correlations
  GROUP BY signature
)
SELECT
  Correlations.*
FROM
  Correlations
  JOIN HighCorrelation ON
    Correlations.signature = HighCorrelation.signature
WHERE highcorrelation > 2
""" % {'startdate': opts.startdate, 'enddate': opts.enddate}

bycpufamilyq = """
WITH BySignatureCPU AS (
  SELECT
    signature_id,
    substring(r.cpu_info FROM E'\\\\A(\\\\w+ family \\\\d+) model \\\\d+ stepping \\\\d+( \\\\| \\\\d+)?\\\\Z') AS cpufamily,
    COUNT(*) AS c
  FROM
    reports r JOIN reports_clean rc on rc.uuid = r.uuid
  WHERE
    r.date_processed > '%(startdate)s'
    AND r.date_processed < '%(enddate)s'
    AND rc.date_processed > '%(startdate)s'
    AND rc.date_processed < '%(enddate)s'
    AND r.os_name ~ 'Windows NT'
    AND r.hangid IS NULL
    AND rc.product_version_id = %%(product_version_id)s
    AND r.cpu_info ~ E'\\\\A(\\\\w+ family \\\\d+) model \\\\d+ stepping \\\\d+( \\\\| \\\\d+)?\\\\Z'
  GROUP BY signature_id, cpufamily
),
BySignature AS (
  SELECT
    signature_id,
    SUM(c) AS total_by_signature
  FROM BySignatureCPU
  GROUP BY signature_id
),
ByCPU AS (
  SELECT
    cpufamily,
    SUM(c) AS total_by_cpu
  FROM BySignatureCPU
  GROUP BY cpufamily
),
AllCrashes AS (
  SELECT SUM(c) AS grand_total
  FROM BySignatureCPU
),
Correlations AS (
  SELECT
    s.signature,
    bsg.cpufamily,
    bsg.c,
    total_by_signature,
    total_by_cpu,
    (bsg.c::float * grand_total::float) /
    (total_by_signature::float * total_by_cpu::float) AS correlation,
    grand_total
  FROM
    BySignatureCPU AS bsg
    JOIN BySignature ON BySignature.signature_id = bsg.signature_id
    JOIN ByCPU ON
      ByCPU.cpufamily = bsg.cpufamily
    CROSS JOIN AllCrashes
    JOIN signatures AS s
      ON bsg.signature_id = s.signature_id
  WHERE
    total_by_signature > %%(cutoff)s
),
HighCorrelation AS (
  SELECT
    signature,
    MAX(correlation) AS highcorrelation
  FROM Correlations
  GROUP BY signature
)
SELECT
  Correlations.*
FROM
  Correlations
  JOIN HighCorrelation ON
    Correlations.signature = HighCorrelation.signature
WHERE highcorrelation > 2
""" % {'startdate': opts.startdate, 'enddate': opts.enddate}

Result = namedtuple('Result', ('signature', 'vendorid', 'c', 'bysig', 'bygraphics', 'grandtotal', 'correlation'))

def savedata(cur, filename):
    results = []
    for signature, vendorid, c, bysig, bygraphics, correlation, grandtotal in cur:
        # if bygraph < 50:
        #     continue

        # Also, if less than 20 (SWAG!) unique users have experienced this crash,
        # the correlation is going to be really noisy and probably meaningless.

        # if correlation < 4:
        #     continue
        results.append(Result(signature, vendorid, c, bysig, bygraphics, grandtotal, correlation))

    results.sort(key=lambda r: (r.bysig, r.correlation), reverse=True)

    fd = open(filename, 'w')
    w = csv.writer(fd)
    w.writerow(('signature', 'vendorid', 'c', 'bysig', 'bygraphics', 'grandtotal', 'correlation'))

    for r in results:
        correlation = "{0:.2f}".format(r.correlation)
        w.writerow((r.signature, r.vendorid, r.c, r.bysig, r.bygraphics, r.grandtotal, correlation))

    fd.close()

params = {
    'product_version_id': productversion,
    'cutoff': opts.cutoff
    }

cur.execute(byvendorq, params)
savedata(cur, outpattern + 'byadaptervendor.csv')

cur.execute(bycpufamilyq, params)
savedata(cur, outpattern + 'bycpufamily.csv')
