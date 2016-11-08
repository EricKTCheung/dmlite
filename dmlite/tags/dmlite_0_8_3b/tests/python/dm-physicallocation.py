#!/usr/bin/python
import pydmlite
import sys

if __name__ == "__main__":
  manager = pydmlite.PluginManager()

  if len(sys.argv) < 3:
    print "Usage:", sys.argv[0], "<configuration file> <path>"
    sys.exit(1)
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
  try :
      stack.setSecurityCredentials(creds)
  except Exception, err:
     sys.stderr.write('ERROR: %s\n' % str(err))
     sys.exit(1)
  poolManager = stack.getPoolManager()
  try :
      location = poolManager.whereToRead(sys.argv[2])
  except Exception, err:
     sys.stderr.write('ERROR: %s\n' % str(err))
     sys.exit(1)
  for l in location:
    print "Chunk: %s:%s (%d-%d)" % (l.host, l.path, l.offset, l.offset + l.size)
    for k in l.getKeys():
      print "\t%s: %s" % (k, l.getString(k))
  sys.exit(0)

