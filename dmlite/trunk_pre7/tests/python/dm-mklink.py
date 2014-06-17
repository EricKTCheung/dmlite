#!/usr/bin/python
import pydmlite
import sys
 
if __name__ == "__main__":
  manager = pydmlite.PluginManager()
  len = len(sys.argv)
  l=0
#  print len
  if len < 5 or sys.argv[1] == "-h":
    print "Usage:", sys.argv[0], " <configuration file> <path>  <dir/file>  <ln> "
    sys.exit(1)
#  print sys.argv[1]
  try :
     manager.loadConfiguration(sys.argv[1])
  except Exception, err:
     sys.stderr.write('ERROR: %s\n' % str(err))
     sys.exit(1) 
     
  stack = pydmlite.StackInstance(manager)
  
  creds = pydmlite.SecurityCredentials()
  creds.clientName   = "/C=CH/O=CERN/OU=GD/CN=Test user 1"
  creds.remoteAdress = "127.0.0.1"
  creds.fqans.append("dteam")

  stack.setSecurityCredentials(creds)

  catalog = stack.getCatalog()
  try:
     catalog.changeDir(sys.argv[2])
  except Exception, err:
    sys.stderr.write('ERROR: %s\n' % str(err))
    sys.exit(1)
  try:
     catalog.symlink(sys.argv[3],sys.argv[4])
  except Exception, err:
     sys.stderr.write('ERROR: %s\n' % str(err))
     sys.exit(1)
  name = sys.argv[2] + sys.argv[4]
  filename = catalog.readLink(name)
  print   sys.argv[4], "->",filename
#  catalog.setMode(sys.argv[4],120777)
#  arg = sys.argv[2] + "/" + sys.argv[3] 
#  catalog.removeDir(arg)
#  print arg," was removed"      
  sys.exit(0)
