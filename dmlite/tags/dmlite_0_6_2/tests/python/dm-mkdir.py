#!/usr/bin/python
import pydmlite
import sys
 
if __name__ == "__main__":
  manager = pydmlite.PluginManager()
  len = len(sys.argv)
  l=0
#  print len
  if len < 3 or sys.argv[1] == "-h":
    print "Usage:", sys.argv[0], " <configuration file> <path> <dir> [<mode>]"
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
     catalog.makeDir(sys.argv[3],0777)
  except Exception, err:
     sys.stderr.write('ERROR: %s\n' % str(err))
     sys.exit(1)
  print sys.argv[3]," was created"
  if  len == 5 :
   try:
      mode = int(sys.argv[4],8)
   except Exception, err:
     sys.stderr.write('ERROR: %s\n' % str(err))
     sys.exit(1)
   print sys.argv[3],"%o" % mode
   catalog.setMode(sys.argv[3],mode)
#  arg = sys.argv[2] + "/" + sys.argv[3] 
#  catalog.removeDir(arg)
#  print arg," was removed"      
  sys.exit(0)
