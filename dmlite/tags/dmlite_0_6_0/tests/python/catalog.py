#!/usr/bin/env python

import unittest
import pydmlite
import sys
import time
import stat

class TestDmliteCatalog(unittest.TestCase):

	def setUp(self):
		self.conf_file = "/etc/dmlite.conf"
                self.path = "/dpm/cern.ch/home/dteam"
                self.mode = "0777"
                self.newdir = "sli"
                self.rmdir = "sli"
                self.newfile = "sli.txt"
                self.rmfile = "sli.txt"
		self.newlink = "lnsli.txt"  
		self.manager = pydmlite.PluginManager()
		try :
			self.manager.loadConfiguration(self.conf_file)
		except Exception, err:
			self.fail("%s" % err)
  		self.stack = pydmlite.StackInstance(self.manager)
		self.creds = pydmlite.SecurityCredentials()
		self.creds.clientName   = "/C=CH/O=CERN/OU=GD/CN=Test user 1"
		self.creds.remoteAdress = "127.0.0.1"
		self.creds.fqans.append("dteam")

		try:
			self.stack.setSecurityCredentials(self.creds)
		except Exception, err:
			self.fail("%s" % err)

	def tearDown(self):
		pass

	def test_stat_dir(self):
		# TODO: actually create the dir we test against, test times and uid/gid, test parent
  		catalog = self.stack.getCatalog()

		xstat = catalog.extendedStat("/dpm/cern.ch", True)

		self.assertEquals(xstat.name, "cern.ch")
		self.assertTrue(xstat.stat.isDir())
		self.assertTrue(stat.S_ISDIR(xstat.stat.st_mode))
		self.assertFalse(xstat.stat.isLnk())
		self.assertFalse(stat.S_ISLNK(xstat.stat.st_mode))
		self.assertFalse(xstat.stat.isReg())
		self.assertFalse(stat.S_ISREG(xstat.stat.st_mode))
		self.assertTrue(xstat.stat.st_ino > 0)
		self.assertEquals(xstat.stat.st_nlink, 1)
		self.assertEquals(xstat.stat.st_size, 0)

	def test_stat_file(self):
		# TODO: actually create the file we test against, test times and uid/gid, test parent
  		catalog = self.stack.getCatalog()

		xstat = catalog.extendedStat("/dpm/cern.ch/home/dteam/index2.html", True)

		self.assertEquals(xstat.name, "index2.html")
		self.assertFalse(xstat.stat.isDir())
		self.assertFalse(stat.S_ISDIR(xstat.stat.st_mode))
		self.assertFalse(xstat.stat.isLnk())
		self.assertFalse(stat.S_ISLNK(xstat.stat.st_mode))
		self.assertTrue(xstat.stat.isReg())
		self.assertTrue(stat.S_ISREG(xstat.stat.st_mode))
		self.assertTrue(xstat.stat.st_ino > 0)
		self.assertEquals(xstat.stat.st_nlink, 1)
	def test_mkdir(self):
		catalog = self.stack.getCatalog()
		try:
		   catalog.changeDir(self.path)
		except Exception, err:
		   sys.stderr.write('ERROR: %s\n' % str(err))
		   return 1
		try:
		   catalog.makeDir(self.newdir,0775) # create a test dir
		except Exception, err:
		   sys.stderr.write('ERROR: %s\n' % str(err))
		   return 1
		try:
		   catalog.setMode(self.newdir,0777) # change mode from 0775 to 0777
		except Exception, err:
		   sys.stderr.write('ERROR: %s\n' % str(err))
		   return 1
		return 0
	def test_mydir_stat(self):
		catalog = self.stack.getCatalog()
		mydirpath = self.path + "/" + self.newdir
		try:
		   xstat = catalog.extendedStat(mydirpath, True)  
		except Exception, err:
		   sys.stderr.write('ERROR: %s\n' % str(err))
		   return 1  
		self.assertEquals(xstat.name,self.newdir)    # checking the directory name 
		self.assertTrue(xstat.stat.isDir())
		self.assertTrue(stat.S_ISDIR(xstat.stat.st_mode))
		self.assertFalse(xstat.stat.isLnk())
		self.assertFalse(xstat.stat.isReg())
		self.assertTrue(xstat.stat.st_ino > 0)
		self.assertNotEqual(xstat.stat.st_nlink, 1)
		self.assertEquals(xstat.stat.st_size, 0)
		deltatimed = time.time() - xstat.stat.getMTime()
		self.assertTrue(deltatimed < 4)             # checking the time to create the directory
		self.assertEquals(xstat.stat.st_blksize,0)
		self.assertEquals(xstat.stat.st_size, 0)
		self.assertEquals(str(oct(xstat.stat.st_mode)),"040777")  # checking the mode
	def test_rmdir(self):
		catalog = self.stack.getCatalog()
		try:
		   catalog.changeDir(self.path)
		except Exception, err:
		   sys.stderr.write('ERROR: %s\n' % str(err))
		   return 1
		arg = self.path + "/" + self.rmdir
		try:
		   catalog.removeDir(arg)
		except Exception, err:
		   sys.stderr.write('ERROR: %s\n' % str(err))
		   return 1
		return 0 
	def test_mkfile(self):
		catalog = self.stack.getCatalog()
		try:
		   catalog.changeDir(self.path)
		except Exception, err:
		   sys.stderr.write('ERROR: %s\n' % str(err))
		   return 1
		try:
		   catalog.create(self.newfile, 0775) # create a test file
		except Exception, err:
		   sys.stderr.write('ERROR: %s\n' % str(err))
		   return 1
		try:
		   xstat = catalog.extendedStat(self.newfile, True)
		except Exception, err:
		   sys.stderr.write('ERROR: %s\n' % str(erra))
		   return 1
		try:
		   catalog.setMode(self.newfile,0777)   # change mode from 0775 to 0777
		except Exception, err:
		   sys.stderr.write('ERROR: %s\n' % str(err))
		   return 1
		return 0
	def test_myfile_stat(self):
		catalog = self.stack.getCatalog()
		filevername = self.path + "/" + self.newfile
		try:
		   xstat = catalog.extendedStat(filevername,True)
		except Exception, err:
		   sys.stderr.write('ERROR: %s\n' % str(erra))
		   return 1
		self.assertFalse(xstat.stat.isDir())
		self.assertFalse(stat.S_ISDIR(xstat.stat.st_mode))
		self.assertFalse(xstat.stat.isLnk())
		self.assertFalse(stat.S_ISLNK(xstat.stat.st_mode))
		self.assertTrue(xstat.stat.isReg())
		self.assertTrue(stat.S_ISREG(xstat.stat.st_mode))
		self.assertEquals(xstat.name, self.newfile)               # checking the file name
		self.assertEquals(str(oct(xstat.stat.st_mode)),"0100777") # checking the file mode
		self.assertTrue(xstat.stat.st_ino > 0)
		self.assertEquals(xstat.stat.st_nlink, 1)
                deltatime = time.time() - xstat.stat.getMTime()
                self.assertTrue(deltatime < 4)                            # checking the time to create the file
                self.assertEquals(xstat.stat.st_blksize,0)
		self.assertEquals(xstat.stat.st_size, 0)
		self.assertTrue(xstat.stat.st_uid,True)
		self.assertTrue(xstat.stat.st_gid,True)
		return 0
        def test_mklink(self):
                catalog = self.stack.getCatalog()
                try:
                   catalog.changeDir(self.path)
                except Exception, err:
                   sys.stderr.write('ERROR: %s\n' % str(err))
                   return 1
                try:
                   catalog.symlink(self.newfile, self.newlink)
                except Exception, err:
                   sys.stderr.write('ERROR: %s\n' % str(err))
                   return 1
                name = self.path + "/" +  self.newlink
                try:
                   filename = catalog.readLink(name)
                except Exception, err:
                   sys.stderr.write('ERROR: %s\n' % str(err))
                   return 1
                return 0
	def test_mylink_stat(self):
		catalog = self.stack.getCatalog()
		try:
		   xstat = catalog.extendedStat(self.newlink,False)
		except Exception, err:
		   sys.stderr.write('ERROR: %s\n' % str(err))
		   return 1
		self.assertFalse(xstat.stat.isDir())
		self.assertFalse(xstat.stat.isReg())
		self.assertTrue(xstat.stat.isLnk())
		self.assertEquals(xstat.name,self.newlink)                     # checking the link name
		self.assertEquals(catalog.readLink(self.newlink),self.newfile) # checking of the link (newlink->newfile)
		self.assertEquals(str(oct(xstat.stat.st_mode)),"0120777")      # checking the link  mode
		self.assertTrue(xstat.stat.st_ino > 0)
		self.assertEquals(xstat.stat.st_nlink, 1)
		deltatimel = time.time() - xstat.stat.getMTime()
		self.assertTrue(deltatimel < 4)                                # checking the time to create the link
		self.assertEquals(xstat.stat.st_blksize,0)
		self.assertEquals(xstat.stat.st_size, 0)
		
	def test_rmlink(self):
		catalog = self.stack.getCatalog()
		try:
		   catalog.changeDir(self.path)
		except Exception, err:
		   sys.stderr.write('ERROR: %s\n' % str(err))
		   return 1
		try:
		   catalog.unlink (self.newlink)
		except Exception, err:
		   sys.stderr.write('ERROR: %s\n' % str(err))
		   return 1
		return 0

	def test_rmfile(self):
		catalog = self.stack.getCatalog()
		try:
		   catalog.changeDir(self.path)
		except Exception, err:
		   sys.stderr.write('ERROR: %s\n' % str(err))
		   return 1
		try:
		   xstat = catalog.extendedStat(self.newfile,True)
		except Exception, err:
		   sys.stderr.write('ERROR: %s\n' % str(err))
		   return 1	
		try:
		   catalog.unlink(self.newfile)
		except Exception, err:
		   sys.stderr.write('file is removed: %s\n' % str(err))
		   return 1
		return 0	
if __name__ == '__main__':
    unittest.main()
