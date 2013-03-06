import psycopg2
import os, sys
import csv
from collections import namedtuple

connectionstring = open(os.path.expanduser('~/socorro.connection'), 'r').read().strip()

conn = psycopg2.connect(connectionstring)
cur = conn.cursor()

productversion = 1402


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
    r.date_processed > '2013-03-04'
    AND r.date_processed < '2013-03-06'
    AND rc.date_processed > '2013-03-04'
    AND rc.date_processed < '2013-03-06'
    AND r.app_notes ~ E'AdapterVendorID: (?:0x)?(\\\\w+)'
    AND r.app_notes ~ E'AdapterDeviceID: (?:0x)?(\\\\w+)'
    AND r.os_name ~ 'Windows NT'
    AND r.hangid IS NULL
    AND rc.product_version_id = 1402
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
"""

cur.execute(q)

results = []

Result = namedtuple('Result', ('signature', 'vendorid', 'deviceid', 'correlation', 'c'))

for signature, vendorid, deviceid, c, correlation in cur:
    # small samples make results much lss inteeresting
    if c < 10:
        continue
    if correlation < 2:
        continue
    results.append(Result(signature, vendorid, deviceid, correlation, c))

results.sort(key=lambda r: r.correlation, reverse=True)

w = csv.writer(sys.stdout)
w.writerow(('signature', 'vendorid', 'deviceid', 'correlation', 'c'))

for r in results:
    correlation = "{0:.2f}".format(r.correlation)
    w.writerow((r.signature, r.vendorid, r.deviceid, correlation, r.c))
