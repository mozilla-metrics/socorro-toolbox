# postprocess the results of GFXGroupings.pig

import sys
import csv
from collections import namedtuple

Row = namedtuple('Row', ('vendorid', 'version', 'count'))

rows = [Row(items[0], items[1], int(items[2]))
        for items in csv.reader(sys.stdin, dialect='excel-tab')]

total = sum( (row.count for row in rows) )

rows.sort(key=lambda row: row.count, reverse=True)

for row in rows:
    print "%s:%s\t%i (%.1f%%)" % (row.vendorid, row.version,
                                  row.count, float(row.count) / total * 100)
