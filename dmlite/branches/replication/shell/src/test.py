from dbutils import DPMDB

db = DPMDB()

print
print "Getting replicas for a specific disk-server"
print

list = db.getReplicasInServer("dpm-puppet01.cern.ch")

for i in range(0, len(list)):
	print list[i]

print
print "Getting replicas for a specific FS"
print

list = db.getReplicasInFS("/srv/dpm")

for i in range(0, len(list)):
        print list[i]

print
print "Getting replicas for a specific pool"
print

list = db.getReplicasInPool("pool_1")

for i in range(0, len(list)):
        print list[i]
print
print "Getting FS for a specific pool"
print

list = db.getFilesystems("pool_1")

for i in range(0, len(list)):
        print list[i]

print
print "Getting groupnme for a specific guid"
print

list = db.getGroupByGID("101")

for i in range(0, len(list)):
        print list[i]

print
print "Getting guid for a specific groupname"
print

list = db.getGroupIdByName("dteam")

for i in range(0, len(list)):
        print list[i]
