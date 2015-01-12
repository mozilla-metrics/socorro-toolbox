import os, sys, csv

startdir, = sys.argv[1:]

w = csv.writer(sys.stdout)

for dirpath, dirnames, filenames in os.walk(startdir):
    reldir = os.path.relpath(dirpath, startdir)
    for leaf in filenames:
        size = os.path.getsize(os.path.join(dirpath, leaf))
        w.writerow((os.path.join(reldir, leaf), size))
