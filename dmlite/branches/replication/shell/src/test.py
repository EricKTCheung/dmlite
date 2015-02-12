from dbutils import DPMDB

db = DPMDB()

print
print "Getting replicas for a specific disk-server"
print

list = db.getReplicasInServer("dpm-puppet01.cern.ch")

for  i in list:
        print i

print
print "Getting replicas for a specific FS"
print

list = db.getReplicasInFS("/srv/dpm","dpm-puppet01.cern.ch")

for  i in list:
        print i

print
print "Getting replicas for a specific pool"
print

list = db.getReplicasInPool("pool_1")


for  i in list:
        print i

print
print "Getting filename for a specific SFN"
print

filename = db.getLFNFromSFN("dpm-puppet01.cern.ch:/srv/dpm/dteam/2015-01-16/testFile.19.0")

print filename


print
print "Getting FS for a specific pool"
print

list = db.getFilesystems("pool_1")

for  i in list:
        print i

print
print "Getting groupnme for a specific guid"
print

list = db.getGroupByGID("101")

for  i in list:
        print i

print
print "Getting guid for a specific groupname"
print

list = db.getGroupIdByName("dteam")

for  i in list:
        print i

