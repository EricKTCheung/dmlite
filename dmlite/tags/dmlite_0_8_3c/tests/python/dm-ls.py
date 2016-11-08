#!/usr/bin/python
import pydmlite
import sys
import time
from datetime import datetime
 
if __name__ == "__main__":
  manager = pydmlite.PluginManager()
  len = len(sys.argv)
  l=0
#  print len
  if len < 3 or sys.argv[1] == "-h":
    print "Usage:", sys.argv[0], "[-l] <configuration file> <path>"
    sys.exit(1)
#  print sys.argv[1]
  if sys.argv[1] == "-l" :
     l=1
  try :
      manager.loadConfiguration(sys.argv[l+1])
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
  inputname = sys.argv[l+2] + "/"
  try:
    xstat = catalog.extendedStat(inputname, True)
  except :
    try :
      xstat = catalog.extendedStat(sys.argv[l+2],False)
      flag = xstat.stat.isLnk()
      if flag == True :
#                     print "\t%s" % xstat.name, "  is link "
                     filename = catalog.readLink(sys.argv[l+2])
#                     print "\t%s" % xstat.name, "  is link to ", filename 
                     ltime = time.localtime(xstat.stat.getMTime())
                     date=datetime(ltime[0],ltime[1],ltime[2],ltime[3],ltime[4],ltime[5])
                     print "%o" % xstat.stat.st_mode, "\t", "\t", xstat.stat.st_uid, "\t", xstat.stat.st_gid, '\t', xstat.stat.st_size, "\t", date, "\t%s" %xstat.name, "->",filename
 
                     sys.exit(0)  
    except Exception, err:
       sys.stderr.write('ERROR: %s\n' % str(err))
       sys.exit(1)
  flag = xstat.stat.isDir()
#  print flag
  if flag == True :
#    print "it is directory"
    catalog = stack.getCatalog()
    try :
        mydir = catalog.openDir(inputname)
    except Exception, err:
     sys.stderr.write('ERROR: %s\n' % str(err))
     sys.exit(1)
    while True:
     try:
        f = catalog.readDirx(mydir)
        if f.name == "" :
           catalog.closeDir(mydir)
           sys.exit(0)
#  dm_ls -l
        if l>0 :
#           name=sys.argv[3] + f.name
           name = inputname + f.name
#           print name
           try :
               xstat = catalog.extendedStat(name, True)
           except :
                  xstat = catalog.extendedStat(name, False)
                  flag = xstat.stat.isLnk()
                  if flag == True :
#                     print "\t%s" % f.name, "  is link "
                     filename = catalog.readLink(name)    
                     ltime = time.localtime(xstat.stat.getMTime())
                     date=datetime(ltime[0],ltime[1],ltime[2],ltime[3],ltime[4],ltime[5])
                     print "%o" % xstat.stat.st_mode, "\t", "\t", xstat.stat.st_uid, "\t", xstat.stat.st_gid, '\t', xstat.stat.st_size, "\t", date, "\t%s" % f.name, "->",filename
                  continue
           ltime = time.localtime(xstat.stat.getMTime()) 
           date=datetime(ltime[0],ltime[1],ltime[2],ltime[3],ltime[4],ltime[5])           
           print "%o" % xstat.stat.st_mode, "\t", "\t", xstat.stat.st_uid, "\t", xstat.stat.st_gid, '\t', xstat.stat.st_size, "\t", date, "\t%s" % f.name
        else :  
           print "\t%s" % f.name
     except:
        catalog.closeDir(mydir)
        break
  else :
        if l>0 :
           ltime = time.localtime(xstat.stat.getMTime())
           date=datetime(ltime[0],ltime[1],ltime[2],ltime[3],ltime[4],ltime[5])
           print "%o" % xstat.stat.st_mode, "\t", "\t", xstat.stat.st_uid, "\t", xstat.stat.st_gid, '\t', xstat.stat.st_size, "\t", date, "\t%s" % xstat.name
        else :
           print "\t%s" % xstat.name
  sys.exit(0)
  
