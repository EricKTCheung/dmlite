#!/usr/bin/python
import pydmlite
import sys
import time
from datetime import datetime
 
if __name__ == "__main__":
  manager = pydmlite.PluginManager()
  len = len(sys.argv)
  l=0
  if len < 4 or sys.argv[1] == "-h":
    print "Usage:", sys.argv[0], " <configuration file> <path> <file> "
    sys.exit(1)
  try:
     manager.loadConfiguration(sys.argv[1])
  except Exception, err:
     sys.stderr.write('ERROR: %s\n' % str(err))
     sys.exit(1)
  stack = pydmlite.StackInstance(manager)
  
  creds = pydmlite.SecurityCredentials()
  creds.clientName   = "/C=CH/O=CERN/OU=GD/CN=Test user 1"
  creds.remoteAdress = "127.0.0.1"
  creds.fqans.append("dteam")

  try:
     stack.setSecurityCredentials(creds)
  except Exception, err:
     sys.stderr.write('ERROR: %s\n' % str(err))
     sys.exit(1)

  catalog = stack.getCatalog()
  try:
     catalog.changeDir(sys.argv[2])
  except Exception, err:
     sys.stderr.write('ERROR: %s\n' % str(err))
     sys.exit(1)
  try:
     xstat = catalog.extendedStat(sys.argv[3], True)
  except Exception, err:
     sys.stderr.write('ERROR: %s\n' % str(err))
     sys.exit(1)
  ltime = time.localtime(xstat.stat.getMTime())
  print "file exists: "

  date=datetime(ltime[0],ltime[1],ltime[2],ltime[3],ltime[4],ltime[5])
  print "%o" % xstat.stat.st_mode, "\t", "\t", xstat.stat.st_uid, "\t", xstat.stat.st_gid, '\t', xstat.stat.st_size, "\t", date, "\t%s" % xstat.name   
  catalog.unlink(sys.argv[3])
  
  try:
      xstat = catalog.extendedStat(sys.argv[3], True)
  except Exception, err:
     sys.stderr.write('file is removed: %s\n' % str(err))
     sys.exit(0)     
#  sys.exit(0)
