import sys
import os
try:
        import MySQLdb
except:
        sys.exit("Could not import MySQLdb, please install the MySQL Python module.")

class DPMDB(object):

	def __init__(self):
		#getting the mysql conf from configuration
		#TO DO: read the conf from the conf file
        	try :
                	conf = open("/etc/dmlite.conf.d/mysql.conf", 'r')
        	except Exception, e:
                	return self.error(e.__str__() + '\nParameter(s): ' + ', '.join(given))

		username = None
        	password = None
		host = None
		port = None
		nsDBName = None
		dpmDBName = None

        	for line in conf:
                	if line.startswith("MySqlHost"):
                        	host  = line[len("MySqlHost")+1:len(line)].strip()
                	if line.startswith("MySqlUsername"):
                        	username=  line[len("MySqlUsername")+1:len(line)].strip()
			if line.startswith("MySqlPassword"):
        	                password=  line[len("MySqlPassword")+1:len(line)].strip()
			if line.startswith("MySqlPort"):
                	        port=  line[len("MySqlPort")+1:len(line)].strip()
			if line.startswith("NsDatabase"):
                	        nsDBName=  line[len("NsDatabase")+1:len(line)].strip()
			if line.startswith("DpmDatabase"):
				dpmDBName= line[len("DpmDatabase")+1:len(line)].strip()
		conf.close()

		if int(port) == 0:
			port =3316 

		try:
		  	dpmdb = MySQLdb.connect(host=host, user=username, passwd=password, db=dpmDBName,port= port )
        		self.dpmdb_c = dpmdb.cursor()
        	except MySQLdb.Error, e:
			sys.exit("Error %d: %s" % (e.args[0], e.args[1]))

		try:
                	nsdb = MySQLdb.connect(host=host, user=username, passwd=password, db=nsDBName,port= port )
                	self.nsdb_c = nsdb.cursor()
        	except MySQLdb.Error, e:
                	sys.exit("Error %d: %s" % (e.args[0], e.args[1]))

	def getReplicasInServer(self,server):
  		"""Method to get all the file replica for a single server."""
		try:
			self.nsdb_c.execute('''
	        	SELECT m.name, r.poolname,r.fs, r.host, r.sfn, m.filesize, m.gid, m.status, r.setname
        		FROM Cns_file_replica r
        	 	JOIN Cns_file_metadata m USING (fileid)
        		WHERE r.host = '%(host)s'
        		''' % {"host" : server})
      			ret = list()
      			for row in self.nsdb_c.fetchall():
     		   		ret.append(FileReplica(row[0], row[1], row[2], row[3], row[4], row[5],row[6],row[7],row[8]))
      			return ret
	   	except MySQLdb.Error, e:
      			print "Error %d: %s" % (e.args[0], e.args[1])
      			sys.exit(1)  

	def getReplicasInFS(self,fsname,server):
                """Method to get all the file replica for a FS."""
                try:
                        self.nsdb_c.execute('''
                        SELECT m.name, r.poolname,r.fs, r.host, r.sfn, m.filesize, m.gid, m.status, r.setname
                        FROM Cns_file_replica r
                        JOIN Cns_file_metadata m USING (fileid)
                        WHERE r.fs = '%(fs)s' AND r.host= '%(host)s'
                        ''' % {"fs" : fsname,"host" : server})
                        ret = list()
                        for row in self.nsdb_c.fetchall():
                                ret.append(FileReplica(row[0], row[1], row[2], row[3], row[4], row[5],row[6],row[7],row[8]))
                        return ret
                except MySQLdb.Error, e:
                        print "Error %d: %s" % (e.args[0], e.args[1])
                        sys.exit(1)


	def getReplicasInPool(self,poolname):
                """Method to get all the file replica for a pool"""
                try:
                        self.nsdb_c.execute('''
                        SELECT m.name, r.poolname,r.fs, r.host, r.sfn, m.filesize, m.gid, m.status, r.setname
                        FROM Cns_file_replica r
                        JOIN Cns_file_metadata m USING (fileid)
                        WHERE r.poolname = '%(poolname)s'
                        ''' % {"poolname" : poolname})
                        ret = list()
                        for row in self.nsdb_c.fetchall():
                                ret.append(FileReplica(row[0], row[1], row[2], row[3], row[4], row[5], row[6],row[7],row[8]))
                        return ret
                except MySQLdb.Error, e:
                        print "Error %d: %s" % (e.args[0], e.args[1])
                        sys.exit(1)

        def getFilesystems(self, poolname):
                """get all filesystems in a pool"""
                try:
                        self.dpmdb_c.execute('''
                        SELECT poolname, server, fs, status, weight
                        FROM dpm_fs
                        WHERE poolname = '%(pool)s'
                        ''' % {"pool" : poolname })
                        ret = list()
                        for row in self.dpmdb_c.fetchall():
                                ret.append(FileSystem(row[0], row[1], row[2], row[3], row[4]))
                        return ret
                except MySQLdb.Error, e:
                        print "Error %d: %s" % (e.args[0], e.args[1])
                        sys.exit(1)

	def getGroupByGID(self, gid):
		"""get groupname by gid """
		try:
			self.nsdb_c.execute('''
                        SELECT groupname
                        FROM Cns_groupinfo
                        WHERE gid = '%(gid)s' 
                        ''' % {"gid" : gid })
                        ret = list()
                        for row in self.nsdb_c.fetchall():
                                ret.append(row[0])
                        return ret
                except MySQLdb.Error, e:
                        print "Error %d: %s" % (e.args[0], e.args[1])
                        sys.exit(1)

	def getGroupIdByName(self, groupname):
                """get groupid by name """
                try:
                        self.nsdb_c.execute('''
                        SELECT gid
                        FROM Cns_groupinfo
                        WHERE groupname = '%(groupname)s'                         
                        ''' % {"groupname" : groupname })
                        ret = list()
                        for row in self.nsdb_c.fetchall():
                                ret.append(row[0])
                        return ret
                except MySQLdb.Error, e:
                        print "Error %d: %s" % (e.args[0], e.args[1])
                        sys.exit(1)

# Get a filesytem information from db
class FileReplica(object):
  	def __init__(self, name,poolname, fsname, host,sfn, size,gid,status,setname):
		self.name = name
		self.poolname = poolname
		self.fsname = fsname
		self.host = host
		self.sfn = sfn
		self.size =size
		self.gid= gid
		self.status=status
		self.setname=setname
		
	def __repr__(self):
                return "FileReplica(name=" + self.name + ", poolname=" + self.poolname + ", server=" + self.host + ", fsname=" + self.fsname + ", sfn=" + self.sfn + ", size=" + str(self.size) + ", gid=" + str(self.gid) + ", status=" + self.status + ", setname=" + self.setname + ")"


# Get a filesytem information from db
class FileSystem(object):

	def __init__(self, poolname, server, name, status, weight):
    		self.poolname = poolname
	    	self.server = server
		self.name = name
    		self.status = status
		self.files = list()
    		# Filled in by annotateFreeSpace()
		self.size = None
    		self.avail = None
    		self.status = status
    		self.weight = weight


	def desc(self):
    		"""Short description of this location - i.e. a minimal human readable description of what this is"""
    		return str(self.server) + str(self.name)

  	def fileCount(self):
    		"""Support for using these to track dirs at a time"""
    		return len(self.files)

	def __repr__(self):
		return "FileSystem(poolname=" + self.poolname + ", server=" + self.server + ", name=" + self.name + ", status=" + str(self.status) + ", weight=" + str(self.weight) + ", with " + str(len(self.files)) + " files and " + str(self.avail) + " 1k blocks avail)"

