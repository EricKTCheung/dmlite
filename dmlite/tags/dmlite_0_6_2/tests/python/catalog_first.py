#!/usr/bin/env python

import unittest
import pydmlite
import sys
import time
import stat

class TestDmliteCatalog(unittest.TestCase):

	def setUp(self):
		self.conf_file = "/etc/dmlite.conf"
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

if __name__ == '__main__':
    unittest.main()
