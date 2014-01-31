from datetime import date, datetime, time, timedelta
import psycopg2
import os, sys
from collections import namedtuple, defaultdict
from dateoption import date_to_timestamp
import csv

Range = namedtuple('Range', ('version', 'enddate'))

class Signature(object):
    def __init__(self):
        self.data = [0, 0]

    def set(self, pos, ratio):
        self.data[pos] = ratio

    def get(self, pos):
        return self.data[pos]

    def difference(self):
        return self.data[1] - self.data[0]

class CrashData(object):
    def __init__(self, range, os):
        self.range = range
        self.os = os

    def fetch(self, cur):
        cur.execute('''
        SELECT SUM(adu_count)
        FROM product_adu badu
        JOIN product_versions pv ON
          pv.product_version_id = badu.product_version_id
        WHERE
          adu_date >= %(enddate)s - interval '1 week' AND adu_date < %(enddate)s
          AND pv.build_type = 'beta' AND pv.major_version = %(version)s
          AND pv.product_name = 'Firefox'
        ''', {'os': self.os,
              'version': self.range.version,
              'enddate': self.range.enddate})

        adu, = cur.fetchone()
        self.adu = float(adu)

        startdate = date_to_timestamp(self.range.enddate - timedelta(days=7))
        enddate = date_to_timestamp(self.range.enddate - timedelta(days=1))
        cur.execute('''
            SELECT COUNT(*) count
            FROM reports_clean rc
            JOIN product_versions pv ON
              pv.product_version_id = rc.product_version_id
            WHERE
              date_processed BETWEEN %(startdate)s AND %(enddate)s
              AND pv.build_type = 'beta' AND pv.major_version = %(version)s
              AND pv.product_name = 'Firefox'
              AND rc.process_type = 'Browser'
            ''', {'os': self.os,
                  'version': self.range.version,
                  'startdate': startdate,
                  'enddate': enddate})
        self.total, = cur.fetchone()

        cur.execute('''
            SELECT s.signature, COUNT(*) count
            FROM reports_clean rc
            JOIN signatures s ON
              s.signature_id = rc.signature_id
            JOIN product_versions pv ON
              pv.product_version_id = rc.product_version_id
            WHERE
              date_processed BETWEEN %(startdate)s AND %(enddate)s
              AND pv.build_type = 'beta' AND pv.major_version = %(version)s
              AND pv.product_name = 'Firefox'
              AND rc.process_type = 'Browser'
            GROUP BY signature
            ORDER BY count DESC
            LIMIT 200
            ''', {'os': self.os,
                  'version': self.range.version,
                  'startdate': startdate,
                  'enddate': enddate})

        self.signatures = list(cur)

    def populate(self, all_signatures, pos):
        for signature, count in self.signatures:
            s = all_signatures[signature]
            ratio = count / self.adu
            s.set(pos, ratio)

osname = 'Windows'
first = Range('26.0', date(2013, 12, 9))
second = Range('27.0', date(2014, 1, 26))

connectionstring = open(os.path.expanduser('~/socorro.connection'), 'r').read().strip()

conn = psycopg2.connect(connectionstring)
cur = conn.cursor()

firstdata = CrashData(first, osname)
firstdata.fetch(cur)
seconddata = CrashData(second, osname)
seconddata.fetch(cur)

all_signatures = defaultdict(Signature)
firstdata.populate(all_signatures, 0)
seconddata.populate(all_signatures, 1)

all_signatures = all_signatures.items()
all_signatures.sort(key=lambda i: i[1].difference(), reverse=True)

print "%s crash ratio: %.2f (%i/%i)" % (first.version, firstdata.total / firstdata.adu * 1000, firstdata.total, firstdata.adu)
print "%s crash ratio: %.2f (%i/%i)" % (second.version, seconddata.total / seconddata.adu * 1000, seconddata.total, seconddata.adu)
csvw = csv.writer(sys.stdout)
csvw.writerow(("signature","oldrate","newrate","difference"))
for s, signature in all_signatures:
    csvw.writerow((s,
                   "%.2f" % (signature.get(0) * 1000),
                   "%.2f" % (signature.get(1) * 1000),
                   "%.2f" % (signature.difference() * 1000)))

sys.exit(0)

def print_result(adu, totals, signatures):
    for os in oslist:
        adu = adu[os]
        total = totals[os]
        print "OS: %s - %8.3f (%i/%i)" % (os, float(total) / adu * 1000000, total, adu)
        for signature, count in signatures[os]:
            print "  %8.3f %s" % (float(count) / adu * 1000000, signature)

print "Firefox %s" % (first.version,)
print_result(firstadu, firsttotals, firstsignatures)
print
print "Firefox %s" % (second.version,)
print_result(secondadu, secondtotals, secondsignatures)
