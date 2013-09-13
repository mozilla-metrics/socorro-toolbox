import urllib, urllib2
import json
import sys, os, re

"""
Find the rank of a bug in the topcrash report by combining all the signatures
associated with the bug.
"""

crashapi = 'https://crash-stats.mozilla.com/api/'
bzapi = 'https://api-dev.bugzilla.mozilla.org/latest/'

application, version, bug = sys.argv[1:]

finder = re.compile(r'\[\@([^\]]*)\]')

bzurl = "%s/bug/%s?include_fields=whiteboard,cf_crash_signature,summary" % (bzapi, bug)
print "Fetching Buzilla Data"
bugdata = json.load(urllib2.urlopen(bzurl))

sigs = set(s.strip() for s in
           finder.findall(bugdata['cf_crash_signature']) +
           finder.findall(bugdata['summary']) +
           finder.findall(bugdata['whiteboard']))

print "Found signatures:"
for s in sigs:
    print "* ", s

print "Fetching crash-stats data"
apiurl = '%s/TCBS/?%s' % (crashapi,
                          urllib.urlencode({'product': application,
                                            'version': version,
                                            'limit': 300}))

topdata = json.load(urllib2.urlopen(apiurl))

count = sum(crash['count']
            for crash in topdata['crashes']
            if crash['signature'] in sigs)

for rank in xrange(0, 300):
    if count > topdata['crashes'][rank]['count']:
        print "Rank #%d" % (rank + 1,)
        break

