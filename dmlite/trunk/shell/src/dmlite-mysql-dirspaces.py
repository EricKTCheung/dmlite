#!/usr/bin/env python


###########################################################################
#
# dmlite-mysql-dirspaces
# March 2015 - Fabrizio Furano - CERN IT/SDC - furano@cern.ch
#
# A script that calculates the space occupied by files in every directory
# and can set the metadata filesize field with it, for the first N levels
# 
#
# Syntax:
#
# dmlite-mysql-dirspaces [-h] [--help]
# .. to get the help screen
#
#

__version__ = "1.0.1"
__author__  = "Fabrizio Furano"


import collections
import sys
import re
from optparse import OptionParser
import MySQLdb

import datetime
from time import mktime



#
# Given a replica entry, crawl all the parent directories
# and adjust the counters in the dirhash
#
#
#  repname: replica name (for logging, should not be necessary for processing)
#  fileid: ID of the corresponding logical file entry
#  parentid: ID of the parent of the corresponding logical file entry
#
#

def gulpreplica(dbconn, dirhash, repname, repsize, fileid, parentid):

  # Get the entry
  dbcurs = dbconn.cursor()
  cnsname = repname
  cnsparentid = parentid
  dirtokens = []
  
  
  while(1):
   
    
    if (cnsparentid == 0):
      #print "Finished :-)"
      break
    
    item = None
    
    dirtokens.append(cnsparentid)
    
    try:
      
      item = dirhash[cnsparentid]
      
      
      
      # Try to update the size of the object in memory
      # an item is: name, size, parentid
      # indexed by fileid
      
      # name is already in place
        
      item2 = (item[0], item[1]+repsize, item[2], item[3])
      if options.verbose > 2:
        print item, "becomes ", item2
      dirhash[cnsparentid] = item2
      cnsparentid = item[2]
      
      
    except KeyError:
      # Get the currently pointed entry, then go up one level
      dbcurs.execute("select parent_fileid, name, filesize from cns_db.Cns_file_metadata where fileid = %s", (cnsparentid,))
      res = dbcurs.fetchone()
      if (res != None):
        (cnsfileid, cnsname, cnssize) = res
        
        # create new entry for the in memory hash
        item = ( cnsname, repsize, cnsfileid, 0 )
        
        if options.verbose:
          print "New item ", item
          
        dirhash[cnsparentid] = item
        cnsparentid = cnsfileid
      else:        
        # This is an error, we have found an entry whose parent is missing!
        print "Error. Could not find one of the parents of fileid:", cnsfileid
        print " The fileid looked for was: ", cnsparentid
        print "Most likely the entry is orphan:", cnsname
        return
  
  
  
  # Now take the list of the path tokens for this replica
  # and adjust their depth in the global hash
  dirtokens.reverse()
  if options.verbose > 3:
    print "dirtokens: ", dirtokens
    
  cnt = 1
  for tk in dirtokens:
    item = dirhash[tk]
    if (item[3] == 0):
      item2 = (item[0], item[1], item[2], cnt)
      dirhash[tk] = item2
      
    cnt = cnt+1
    
  # Done
  
  
    
#############
# Main code #
#############


starttime =  datetime.datetime.now()

glbdirhash = {}

parser = OptionParser()
parser.add_option('--updatedb', dest='updatedb', action='store_true', default=False, help="Update original DB with the results of the directory sizes")

parser.add_option('-v', '--debug', dest='verbose', action='count', help='Each v increases the verbosity level for debugging (on stderr)')

parser.add_option('--updatelevels', dest='updatelevels', default="6", help="Allows setting the directory size only for the first N levels")

parser.add_option('--nsconfig', dest='nsconfig', default=None, help="Path to the NSCONFIG file where to take the db login info")
parser.add_option('--dbhost', dest='dbhost', default=None, help="Database host, if no NSCONFIG given")
parser.add_option('--dbuser', dest='dbuser', default=None, help="Database user, if no NSCONFIG given")
parser.add_option('--dbpwd', dest='dbpwd', default=None, help="Database password, if no NSCONFIG given")

options, args = parser.parse_args()

dbhost = options.dbhost
dbuser = options.dbuser
dbpwd = options.dbpwd

try:
  updatelevelsi = int(options.updatelevels)
except Exception, e:
  print "Invalid parameter ", options.updatelevels
  print "For a list of parameters run:"
  print "  dmlite-mysql-dirspaces.py -h"
  sys.exit(1) 


print
print "Going to update the first ", updatelevelsi

if options.nsconfig:
  # Parse the NSCONFIG line, extract the db login info from it
  try:
    f = open(options.nsconfig, 'r')
    s = f.read()
    s = s.strip()
    spl = re.split ("[/@]", s, 3)
    
    
    dbuser = spl[0].strip()
    dbpwd = spl[1].strip()
    dbhost = spl[2].strip()
  except Exception, e:
    print "Error while accessing NSCONFIG. " + str(e)
    sys.exit(1)



#
# Connect to the db
#

if options.verbose:
    print "dbuser: " + dbuser
    if options.verbose > 2:
      print "dbpwd: " + dbpwd
    print "dbhost: " + dbhost

try:
  conn = MySQLdb.connect (str(dbhost), str(dbuser), str(dbpwd), "")
except MySQLdb.Error, e:
  print "Error Connecting to mysql. %d: %s" % (e.args[0], e.args[1])
  print "For a list of parameters run:"
  print "  dmlite-mysql-dirspaces.py -h"
  print
  sys.exit (1)

print "DB... Connected!"

if options.verbose:
  print "Selecting replicas"

repcount = 0;
cursor = conn.cursor ()
cursor.execute ("select r.fileid, r.sfn, m.filesize, m.parent_fileid from cns_db.Cns_file_replica r, cns_db.Cns_file_metadata m where m.fileid=r.fileid;")

while (1):
  row = cursor.fetchone ()
  if row == None:
    break
  if options.verbose > 3:
    print "Replica info:", row

  repcount = repcount+1;
  if (repcount % 10000 == 0):
    print "Processing replica ", repcount
    
  gulpreplica(conn, glbdirhash, row[1], row[2], long(row[0]), long(row[3]))


print
print 
print "Finished calculating..."
print "-----------------------------------------------------------"
if options.verbose > 2:
  for k in glbdirhash:
    print k, glbdirhash[k]
print "-----------------------------------------------------------"
print 

#
# TODO:
# here the individual values in the hash should be used to
#  update the values in the DB
#
print "Total replicas processed: ", repcount
print "Total directory count: ", len(glbdirhash)
elapsed = datetime.datetime.now() - starttime;
el = elapsed.seconds + elapsed.microseconds/1000000.0
print "Time elapsed: ", elapsed, "(", el, " seconds )"
print "Total speed:", repcount / el, "processed replicas per second."


starttime =  datetime.datetime.now()

print
print 
print "Fixing counters..."
print "-----------------------------------------------------------"

fixcnt = 0;
for k in glbdirhash:
  if glbdirhash[k][3] <= updatelevelsi:
    
    if options.updatedb:
      print "Setting fileid ", k, glbdirhash[k][0], " with size ", glbdirhash[k][1]
      cursor.execute ("update cns_db.Cns_file_metadata set filesize=%s where fileid=%s", (glbdirhash[k][1], k,) )
    else:
      print "Dry run... Would set fileid ", k, glbdirhash[k][0], " with size ", glbdirhash[k][1]
    fixcnt = fixcnt + 1
    
print "Commit..."
conn.commit()
print "-----------------------------------------------------------"
print

print "Success. ", fixcnt, " filesizes have been set." 
elapsed = datetime.datetime.now() - starttime;
el = elapsed.seconds + elapsed.microseconds/1000000.0
print "Time elapsed: ", elapsed, "(", el, " seconds )"
print "Total speed:", repcount / el, "fixed directories per second."
print

