import sys, os

file, = sys.argv[1:]

oldsignaturemap = {}
newsignaturemap = {}

for line in open(file):
    line = line.rstrip('\n')
    try:
        oldsignature, newsignature, count, example = line.split('\t')
    except ValueError:
        print >>sys.stderr, "Questionable line: %r" % (line,)
        continue
    count = int(count)

    t = count, example
    oldsignaturemap.setdefault(oldsignature, {})[newsignature] = t
    newsignaturemap.setdefault(newsignature, {})[oldsignature] = t

print "Signature generation report: %s" % (file,)
print
print "******"
print
print "Mappings of current signatures to new signatures"
print

items = filter(lambda i: i[0] > 5,
  ((sum(count for newsignature, (count, example) in newsignatures.iteritems()),
    oldsignature,
    newsignatures)
   for oldsignature, newsignatures in oldsignaturemap.iteritems()))

items.sort(key=lambda i: i[0])

for totalcount, oldsignature, newsignatures in items:
    if len(newsignatures) == 1:
        newsignature, (count, example) = newsignatures.items()[0]
        print "'%s' always maps to '%s' (%i : %s)" % (oldsignature, newsignature, count, example)
    else:
        print "'%s' maps to multiple new signatures:" % (oldsignature,)
        for newsignature, (count, example) in newsignatures.items():
            print "  '%s' (%i : %s)" % (newsignature, count, example)

print
print "******"
print
print "New signatures which combine several old signatures"
print

for newsignature, oldsignatures in newsignaturemap.iteritems():
    if len(oldsignatures) == 1: continue

    print "'%s' combines multiple old signatures:" % (newsignature,)

    for oldsignature, (count, example) in oldsignatures.items():
        print "  '%s' (%i : %s)" % (oldsignature, count, example)
