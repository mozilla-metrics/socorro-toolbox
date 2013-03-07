import psycopg2
import os, sys
import csv
from collections import namedtuple

connectionstring = open(os.path.expanduser('~/socorro.connection'), 'r').read().strip()

conn = psycopg2.connect(connectionstring)
cur = conn.cursor()

startdate = '2013-02-27'
enddate = '2013-03-07'
productversion = 1439

# Note: the startdate/enddate are substituted by python into the query string.
# This is required so that the partition optimizer can exclude irrelevant
# partitions. The product version is passed as a bound parameter.

q = """
WITH BySignatureGraphics AS (
  SELECT
    signature_id,
    substring(r.app_notes from E'AdapterVendorID: (?:0x)?(\\\\w+)') AS AdapterVendorID,
    substring(r.app_notes from E'AdapterDeviceID: (?:0x)?(\\\\w+)') AS AdapterDeviceID,
    COUNT(*) AS c
  FROM
    reports r JOIN reports_clean rc ON rc.uuid = r.uuid
  WHERE
    r.date_processed > '%(startdate)s'
    AND r.date_processed < '%(enddate)s'
    AND rc.date_processed > '%(startdate)s'
    AND rc.date_processed < '%(enddate)s'
    AND r.app_notes ~ E'AdapterVendorID: (?:0x)?(\\\\w+)'
    AND r.app_notes ~ E'AdapterDeviceID: (?:0x)?(\\\\w+)'
    AND r.os_name ~ 'Windows NT'
    AND r.hangid IS NULL
    AND rc.product_version_id = %%(product_version_id)s
  GROUP BY signature_id, AdapterVendorID, AdapterDeviceID
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
    AdapterDeviceID,
    SUM(c) AS total_by_graphics
  FROM BySignatureGraphics
  GROUP BY AdapterVendorID, AdapterDeviceID
),
AllCrashes AS (
  SELECT SUM(c) AS grand_total
  FROM BySignatureGraphics
)
SELECT
  s.signature,
  bsg.AdapterVendorID,
  bsg.AdapterDeviceID,
  bsg.c,
  total_by_signature,
  total_by_graphics,
  (bsg.c::float * grand_total::float) /
  (total_by_signature::float * total_by_graphics::float) AS correlation
FROM
  BySignatureGraphics AS bsg
  JOIN BySignature ON BySignature.signature_id = bsg.signature_id
  JOIN ByGraphics ON
    ByGraphics.AdapterVendorID = bsg.AdapterVendorID
    AND ByGraphics.AdapterDeviceID = bsg.AdapterDeviceID
  CROSS JOIN AllCrashes
  JOIN signatures AS s
    ON bsg.signature_id = s.signature_id
""" % {'startdate': startdate, 'enddate': enddate}

# In this alternate query, we're counting installs (unique install dates)
# instead of total crashes. This reduces the effect of a small number of
# users with many crashes skewing the correlation results. This is good for
# for figuring out whether a correlation is significant, but may be less
# useful if a user is actually more likely to crash *because* of the graphics
# hardware, instead of just being the unlucky user doing the magic something
# needed to trigger the crash. Which result to use really depends on the
# situation.

q2 = """
WITH BySignatureGraphics AS (
  SELECT
    signature_id,
    substring(r.app_notes from E'AdapterVendorID: (?:0x)?(\\\\w+)') AS AdapterVendorID,
    substring(r.app_notes from E'AdapterDeviceID: (?:0x)?(\\\\w+)') AS AdapterDeviceID,
    COUNT(DISTINCT r.install_age) AS count_distinct,
    COUNT(*) AS count_total
  FROM
    reports r JOIN reports_clean rc ON rc.uuid = r.uuid
  WHERE
    r.date_processed > '%(startdate)s'
    AND r.date_processed < '%(enddate)s'
    AND rc.date_processed > '%(startdate)s'
    AND rc.date_processed < '%(enddate)s'
    AND r.app_notes ~ E'AdapterVendorID: (?:0x)?(\\\\w+)'
    AND r.app_notes ~ E'AdapterDeviceID: (?:0x)?(\\\\w+)'
    AND r.os_name ~ 'Windows NT'
    AND r.hangid IS NULL
    AND rc.product_version_id = %%(product_version_id)s
  GROUP BY signature_id, AdapterVendorID, AdapterDeviceID
),
BySignature AS (
  SELECT
    signature_id,
    SUM(count_distinct) AS distinct_by_signature,
    SUM(count_total) AS total_by_signature
  FROM BySignatureGraphics
  GROUP BY signature_id
),
ByGraphics AS (
  SELECT
    AdapterVendorID,
    AdapterDeviceID,
    SUM(count_distinct) AS distinct_by_graphics,
    SUM(count_total) AS total_by_graphics
  FROM BySignatureGraphics
  GROUP BY AdapterVendorID, AdapterDeviceID
),
AllCrashes AS (
  SELECT SUM(count_distinct) AS distinct_total
  FROM BySignatureGraphics
)
SELECT
  s.signature,
  bsg.AdapterVendorID,
  bsg.AdapterDeviceID,
  bsg.count_distinct,
  total_by_signature,
  total_by_graphics,
  (bsg.count_distinct::float * distinct_total::float) /
  (distinct_by_signature::float * distinct_by_graphics::float) AS correlation
FROM
  BySignatureGraphics AS bsg
  JOIN BySignature ON BySignature.signature_id = bsg.signature_id
  JOIN ByGraphics ON
    ByGraphics.AdapterVendorID = bsg.AdapterVendorID
    AND ByGraphics.AdapterDeviceID = bsg.AdapterDeviceID
  CROSS JOIN AllCrashes
  JOIN signatures AS s
    ON bsg.signature_id = s.signature_id
""" % {'startdate': startdate, 'enddate': enddate}

Result = namedtuple('Result', ('signature', 'vendorid', 'deviceid', 'correlation', 'devicetotal', 'signaturetotal'))

def savedata(cur, filename):
    results = []
    for signature, vendorid, deviceid, c, bysig, bygraph, correlation in cur:
        # Let's only care about topcrashes
        if bysig < 150:
            continue

        if bygraph < 50:
            continue

        # Also, if less than 20 (SWAG!) unique users have experienced this crash,
        # the correlation is going to be really noisy and probably meaningless.

        if correlation < 4:
            continue
        results.append(Result(signature, vendorid, deviceid, correlation, c, bysig))

    # results.sort(key=lambda r: r.correlation, reverse=True)
    results.sort(key=lambda r: (r.signaturetotal, r.correlation), reverse=True)

    fd = open(filename, 'w')
    w = csv.writer(fd)
    w.writerow(('signature', 'vendorid', 'deviceid', 'correlation', 'devicetotal', 'signaturetotal'))

    for r in results:
        correlation = "{0:.2f}".format(r.correlation)
        w.writerow((r.signature, r.vendorid, r.deviceid, correlation, r.devicetotal, r.signaturetotal))

    fd.close()

outpattern, = sys.argv[1:]

cur.execute(q, {'product_version_id': productversion})
savedata(cur, outpattern + '.csv')

cur.execute(q2, {'product_version_id': productversion})
savedata(cur, outpattern + '-byinstall.csv')

