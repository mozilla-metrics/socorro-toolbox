import csv
import sys
import os

items = []


symboldirs = [
  'symbols_ffx',
  'symbols_adobe',
  'symbols_os'
]
pattern = '/mnt/netapp/breakpad/%(symboldir)s/%(pdbname)s/%(id)s/%(symname)s'

for t in csv.reader(sys.stdin, dialect='excel-tab'):
    pdb, id, c = t

    if pdb.endswith('.pdb'):
        symname = pdb[:-4] + '.sym'
    else:
        symname = pdb + '.sym'

    found = False
    for symboldir in symboldirs:
        path = pattern % {'symboldir': symboldir,
                          'pdbname': pdb,
                          'id': id,
                          'symname': symname}
        if os.path.exists(path):
            found = True
            break
    if found:
        continue

    items.append(t)

items.sort(key=lambda i: int(i[2]), reverse=True)

w = csv.writer(sys.stdout, dialect='excel-tab')
for t in items:
    w.writerow(t)
