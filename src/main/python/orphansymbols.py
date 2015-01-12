import sys, os, csv

mountpoint = "/mnt/netapp/breakpad/symbols_ffx"

symbols = {}
found = set()

r = csv.reader(open("all-symbols.csv"))
for fname, size in r:
    if fname.startswith("./"):
        fname = fname[2:]
    if fname.endswith(".txt"):
        for foundname in open(os.path.join(mountpoint, fname)):
            foundname = foundname.strip()
            found.add(foundname)
    else:
        symbols[fname] = int(size)

for foundname in found:
    symbols.pop(foundname, None)

unfound = symbols.items()
unfound.sort(key=lambda i: i[1], reverse=True)

totalsize = sum((count for name, count in unfound))

print "Found %i symbol files not listed in .txt files (%i bytes)" % (len(unfound), totalsize)
for name, count in unfound:
    print "%s\t%i" % (name, count)
