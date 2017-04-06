"""
This script is also a GIP provider (and not a plug-in), when used with option
--gip, generating information related to DPM storage areas (GlueSA) and 
associated VO specific information (GlueVOInfo). Information on current 
configuration is collected through DPM Python API.

GlueSA entries describes non-overlapping storage areas. There is one entry per
statically reserved spaces and one entry for the non-reserved space. In each 
GlueSA, ReservedSpace described the SRM reserved space if any and TotalSpace
describes the actually available space (servers drained or down are not taken 
into account). For GlueSA associated with DPM pools, pool capacity as returned
by DPM is reduced by the the amount of reserved space (reported in GlueSAs 
associated with space tokens) in the pool.

If the space area is usable (shared) by several VOs, there is only one 
corresponding GlueSA and one entry for each VO/FQAN in ACBR. Unreserved space 
areas sitting on different DPM pools have distinct GlueSA entries. 
GlueSAPath is not defined in GlueSA.

A GlueVOInfo entry is created for each VO/GlueSA combination defining the 
following attributes :
    - GlueVOInfoPath : home directory for the VO
    - GlueVOInfoTag : space token (description) associated with the GlueSA,
                      if any
    - GlueChunkKey  : GlueSALocalID/GlueSEUniqueID of the associated SA
    
For unreserved space, if there is several GlueSA corresponding to different 
DPM pools, there is one distinct GlueVOInfo for each GlueSA/VO combination.

DISCLAIMER : the output of this script is not intended to be parseable. 
             Formatting is subject to change. If you interested in getting 
             values displayed by this script, you are advised to use DPM 
             Python API. This script may be used as an example on how to 
             use it...

Code created by Michel Jouvin and adapted by Ivan Calvet and Andrea Manzi 
"""
try:
  import pydmlite
except:
	pass
import sys
import os
import os.path
import socket
import re
import subprocess
import signal
from optparse import OptionParser
from time import strftime, gmtime

__version__ = "1.4.3" # should be the version of dmlite
    
class SA(object)  
  """
  Class handling a generic SA description that can be used both with pools and space tokens.
  
  'type' attribute is 0 for pool-related SAs, 1 for reserved space.
  """
  
  def __init__(self,name,acbr,total,free,reserved=None,type=0,resid=None,linkedSA=None,lifetime=None,retention=None,latency=None,
               defsize=None,max_lifetime=None,def_pintime=None,max_pintime=None,fss_policy=None,rs_policy=None,gc_start_thresh=None,
               gc_stop_thresh=None,gc_policy=None,mig_policy=None,space_type=None,server=None,allVos=False):
    self.name = name
    self.id = resid
    self.acbr = acbr
    self.acbr.sort()
    # Initial total is what reported by DPM. For a pool this is the total capacity minus unavailable file systems.
    # self.totalAvailable will always reflect this value but self.total will be updated when reading information about
    # file systems to reflect the configured size.
    self.total = Size(total)
    self.totalAvailable = self.total
    self.free = Size(free)
    if type == 1 and int(self.free) > int(self.total):
      self.free = self.total
    self.unavailableFree = Size(0)
    self.unavailableUsed = Size(0)
    # self.FSFree tracks the total amount of free space in all the file systems online in the pool.
    # Unused (contains 0) for reservations.
    self.FSFree = Size(0)
    # self.reallyUsed keeps track of the used space, independently of the reserved space and of the unavailable space.
    # For a reservation, this the reserved space minus the free space.
    # For a pool, this is the sum of used part of the unreserved space and used part of every reservation in the pool: it is
    # initialized to the pool size minus the free space and will be updated after all the reservations has been read.
    if reserved:
      self.reserved = Size(reserved)
      self.reallyUsed = self.reserved - self.free
    else:
      self.reserved = Size(0)
      self.reallyUsed = self.total - self.free
    self.type = type
    self.fs_list = None
    # server contains configuration information about the current server
    self.server = server
    # vo_list is used to get a list of each VO with the associated FQANs (VO must be one of them
    # if access is allowed to the whole VO)
    self.vo_list = {}
    for fqan in acbr:
      fqan_toks = fqan.split('/',1)
      if not fqan_toks[0] in self.vo_list:
        self.vo_list[fqan_toks[0]] = []
      self.vo_list[fqan_toks[0]].append(fqan)
    self.allVos = allVos
    # linkedSA: for a reservation, the pool it belongs to. For a pool, a list of the reservations
    # created in this pool.
    self.linkedSA = {}
    self.addLinkedSA(linkedSA)
    self.lifetime = LifeTime(lifetime)
    self.retention = Retention(retention)
    # Latency is not explicitly defined for pools, assume online
    if latency:
      self.latency = Latency(latency)
    else:
      self.latency = Latency('O')
    self.space_type = SpaceType(space_type)
    # Some attributes applies only to pools, others are defaulted from parent
    # pool for reservations
    if type == 0:
      self.defsize = Size(defsize)
      self.max_lifetime = LifeTime(max_lifetime)
      self.def_pintime = LifeTime(def_pintime)
      self.max_pintime = LifeTime(max_pintime)
      self.fss_policy = fss_policy
      self.rs_policy = rs_policy
      self.mig_policy = mig_policy
      self.gc_start_thresh = gc_start_thresh             # Percentage of free space
      self.gc_stop_thresh = gc_stop_thresh               # Percentage of free space > gc_start_thresh
      self.gc_policy = gc_policy
    else:
      # Max pin time and life time inherited from pool
      if linkedSA:
        parent_pool = linkedSA
        debug(1,'Updating reserved space and reallyUsed space in pool %s' % parent_pool.name)
        self.max_pintime = parent_pool.max_pintime
        self.max_lifetime = parent_pool.max_lifetime
        # Add reference to the reservation in the pool.
        parent_pool.addLinkedSA(self)
        # Update pool reserved and really used space.
        # Really used space for the pool is initially based on the reserved space: remove the free space in the reservation
        parent_pool.reserved += self.reserved
        parent_pool.reallyUsed -= self.free
        debug(1,'New reserved space = %s, reallyUsed = %s' % (parent_pool.reserved,parent_pool.reallyUsed))
      else:
        debug(0,'ERROR: parent pool not defined for reservation %s' % (self.name))
        sys.exit(20)

    # Must be done last as it relies on other attributes
    self.sa_id = self.buildSaId()

  def vo_str(self):
    vo_list = self.vo_list.keys()
    vo_list.sort()
    return ','.join(vo_list)

  # Build GlueSALocalID based on pool/reservation name, retention and latency.
  # If this is a reservation, add ':SR' to avoid potential conflicts between pool
  # and reservation names.
  # Note: GlueSALocalID value is considered opaque and may be arbitrary.
  def buildSaId(self):
    sa_id = self.name
    if self.type == 1:
      sa_id += ":SR"
    if self.retention:
      sa_id += ":%s" % str(self.retention).lower()
    if self.latency:
      sa_id += ':%s' % str(self.latency).lower()
    return sa_id

  # Function to display selected pool/reservation in the chosen format
  def displaySA(self,gip_mode=False,header=False,details=False,legacy=False,glue_versions={}):
    if gip_mode:
      debug(1,"\nGenerating GlueSA and GlueVOInfo for pool/reservation %s..." % self.name)
      if 'v1' in glue_versions and glue_versions['v1']:
        self.glueSA(legacy)
        self.voInfo()
      if 'v2' in glue_versions and glue_versions['v2']:
        self.glue2Share()
    else:
      debug(1,"\nDisplaying information about pool/reservation %s..." % self.name)
      self.printSA(header,details)

  # Display information in human-readable format.
  def printSA(self,header=False,details=False,ident=4):
    prefix = ident * ' '
    # Handle properly total = 0 for free_percent calculation
    # Ignore reserved space in used space calculation for reservations
    # self.total reflects configured capacity independently of unavailable parts
    # Consider free=0 when internally negative
    if self.type == 0:
      total = self.total
      used = total - self.reserved - self.free - self.unavailableFree
      if int(self.free) < 0:
        debug(1,"Pool %s: free count negative(%s). Resetting to 0." % (self.name,self.free))
        free = Size(0)
      else:
        free = self.free
    else:
      total,free,used,unavailable = self.reservationSpace()
    if int(total) == 0:
      free_percent = 100.0
    else:
      free_percent = free / total * 100
    if self.type == 1:
      if header:
        print "\nSPACE RESERVATIONS:\n"
      print "%s\tID=%s" % (self.name, self.id)
      print prefix+"CAPACITY: %-10s  RESERVED: %-10s  UNAVAIL (free): %s" % (total,self.reserved,unavailable)
      print prefix+"                      USED: %-10s      FREE: %s (%.1f%%)" % (used,free,free_percent)
      print prefix+"Space Type: %s       Retention: %s    Latency: %s" % (self.space_type, self.retention, self.latency)
      print prefix+"Lifetime: %s" % (self.lifetime)
      legend = 'Authorized FQANs:'
      line_fmt = "%%%ds %%s" % len(legend)
      for line in self.acbrStr(details=details,allVos=self.allVos):
        print prefix+line_fmt % (legend,line)
        legend = ''
      print prefix+"Pool: %s" % self.parentPoolName()
      print ""
    else:
      if header:
        print "\nPOOLS:\n"
      print "%s" % (self.name)
      print prefix+"CAPACITY: %-10s  RESERVED: %-10s  UNAVAIL (free/used): %s/%s" % (total,self.reserved,self.unavailableFree,self.unavailableUsed)
      print prefix+"                      USED: %-10s      FREE: %s (%.1f%%)" % (used,free,free_percent)
      if len(self.linkedSA):
        legend = 'Space Tokens:'
        line_fmt = "%%%ds %%s" % len(legend)
        for line in self.tokenList():
          print prefix+line_fmt % (legend,line)
          legend = ''
      legend = 'Authorized FQANs:'
      line_fmt = "%%%ds %%s" % len(legend)
      for line in self.acbrStr(details=details,allVos=self.allVos):
        print prefix+line_fmt % (legend,line)
        legend = ''
      print prefix+"Space Type: %-10s                Retention Policy: %s" % (self.space_type,self.retention)
      if details:
        print prefix+"Default Life Time: %-10s         Max Life Time: %s" % (self.lifetime,self.max_lifetime)
        print prefix+"Default Pin Time: %-10s          Max Pin Time: %s" % (self.def_pintime,self.max_pintime)
        print prefix+"Migration Policy: %-10s          Request Selection Policy: %s" % (self.mig_policy,self.rs_policy)
        if self.gc_start_thresh and self.gc_stop_thresh:
          print prefix+"Garbage Collector: Start < %4.1f%%\tStop > %4.1f%%\t\tPolicy: %s" % (self.gc_start_thresh,self.gc_stop_thresh,self.gc_policy)
        else:
          print prefix+"Garbage Collector: not configured"
      if self.fs_list:
        print prefix+"Number of file systems : %-3d          FS selection policy: %s" % (len(self.fs_list),self.fss_policy)
        if details:
          header = True
          for fs in self.fs_list:
            fs.display(header,details=details,ident=ident)
            header = False
      print ""

  # Display SA information in Glue v1 format (LDIF)
  def glueSA(self,legacy):
    total,reserved,free,used,installed,sa_descr = self.glueSACharacteristics()
    if self.type == 1:
      installedStr = "GlueSACapability: InstalledOnlineCapacity=%s\n" % reserved.GB()
    else:
      installedStr = "GlueSACapability: InstalledOnlineCapacity=%s\n" % installed.GB()

    acbr = ''
    for fqan in self.glueACBR():
      acbr += "GlueSAAccessControlBaseRule: %s\n" % fqan
    
    # Valid values for GlueSAType are permanent, volatile, durable.
    # Assume an undefined type is in fact permanent.
    if str(self.space_type) == 'Any':
      space_type = 'permanent'
    else:
      space_type = str(self.space_type).lower()

    print "dn: GlueSALocalID="+self.sa_id+",GlueSEUniqueID="+self.server.name+",mds-vo-name=resource,o=grid\n" + \
          "objectClass: GlueSATop\n" + \
          "objectClass: GlueSA\n" + \
          "objectClass: GlueSAPolicy\n" + \
          "objectClass: GlueSAState\n" + \
          "objectClass: GlueSAAccessControlBase\n" + \
          "objectClass: GlueKey\n" + \
          "objectClass: GlueSchemaVersion\n" + \
          "GlueSALocalID: "+self.sa_id+"\n" + \
          "GlueSAName: "+sa_descr+"\n" + \
          "GlueSATotalOnlineSize: "+total.GB()+"\n" + \
          "GlueSAUsedOnlineSize: "+used.GB()+"\n" + \
          "GlueSAFreeOnlineSize: "+free.GB()+"\n" + \
          "GlueSAReservedOnlineSize: "+reserved.GB()+"\n" + \
          installedStr + \
          "GlueSACapability: InstalledNearlineCapacity=0\n" + \
          "GlueSATotalNearlineSize: 0\n" + \
          "GlueSAUsedNearlineSize: 0\n" + \
          "GlueSAFreeNearlineSize: 0\n" + \
          "GlueSAReservedNearlineSize: 0\n" + \
          "GlueSARetentionPolicy: "+str(self.retention)+"\n" + \
          "GlueSAAccessLatency: "+str(self.latency)+"\n" + \
          "GlueSAExpirationMode: "+self.space_type.expirationMode()+"\n" + \
          "GlueSAPolicyFileLifeTime: "+space_type.capitalize()+"\n" + \
          "GlueSAType: "+space_type+"\n" + \
          "GlueSAStateAvailableSpace: "+free.KB()+"\n" + \
          "GlueSAStateUsedSpace: "+used.KB()+"\n" + \
          acbr + \
          "GlueChunkKey: GlueSEUniqueID="+self.server.name+"\n" + \
          "GlueSchemaVersionMajor: 1\n" + \
          "GlueSchemaVersionMinor: 3"

    # GlueSAPolicy is considered obsolete
    if legacy:
      print "GlueSAPolicyMaxFileSize: 10000\n" + \
            "GlueSAPolicyMinFileSize: 1\n" + \
            "GlueSAPolicyMaxData: 100\n" + \
            "GlueSAPolicyMaxNumFiles: 10\n" + \
            "GlueSAPolicyMaxPinDuration: "+self.max_pintime.glue()+"\n" + \
            "GlueSAPolicyQuota: 0\n"
    else:
      print ""

  # Create VOInfo object associated with current SA
  def voInfo(self):
    for vo in self.vo_list.keys():
      if self.server.fqanInVoList(vo):
        voinfo_id = vo + ':' + self.name

        # Do not define a tag if there is no space token defined. This allows selecting unreserved SA with a LDAP
        # filter like : 
        # '(&(&(ObjectClass=GlueVOInfo)(GlueChunkKey=*grid05.lal.in2p3.fr)(GlueVOInfoAccessControlBaseRule=*dteam))(!(GLueVOInfoTag=*)))'
        if self.type == 1:
          voinfo_tag = "GlueVOInfoTag: "+self.name+"\n"
        else:
          voinfo_tag = ''
        acbr = ''
        for fqan in self.glueACBR(self.vo_list[vo]):
          acbr += "GlueVOInfoAccessControlBaseRule: %s\n" % fqan
      
        print "dn: GlueVOInfoLocalID="+voinfo_id+",GlueSALocalID="+self.sa_id+",GlueSEUniqueID="+self.server.name+",mds-vo-name=resource,o=grid\n" + \
              "objectClass: GlueSATop\n" + \
              "objectClass: GlueVOInfo\n" + \
              "objectClass: GlueKey\n" + \
              "objectClass: GlueSchemaVersion\n" + \
              "GlueVOInfoLocalID: "+voinfo_id+"\n" + \
              "GlueVOInfoName: "+voinfo_id+"\n" + \
              "GlueVOInfoPath: "+self.server.getVOHome(vo)+"\n" + \
              voinfo_tag + \
              acbr + \
              "GlueChunkKey: GlueSALocalID="+self.sa_id+"\n" + \
              "GlueChunkKey: GlueSEUniqueID="+self.server.name+"\n" + \
              "GlueSchemaVersionMajor: 1\n" + \
              "GlueSchemaVersionMinor: 3\n" + \
              ""
      else:
        debug(1,'VO %s not configured: VOInfo not created' % vo)

  # Display SA information in Glue V2 format.
  # In Glue2 there is one Glue2Share object per VO for each SA. The Glue2StorageShareSharingId allows to keep
  # track of the fact that each share uses the same physical space.
  def glue2Share(self):
    total,reserved,free,used,installed,sa_descr = self.glueSACharacteristics()
    if self.type == 1:
        reserved = Size(0)
    vo_list = self.vo_list.keys()
    vo_list.sort()
    for vo in vo_list:
      # Build StorageShare
      share_id = self.server.name + "/" + vo + "/" + self.name
      print "dn: Glue2ShareId="+share_id+",Glue2ServiceId="+self.server.name+",GLUE2GroupID=resource,o=glue\n" + \
            "objectClass: GLUE2Share\n" + \
            "objectClass: GLUE2StorageShare\n" + \
            "GLUE2ShareID: "+share_id+"\n" + \
            "GLUE2StorageShareSharingID: "+self.sa_id+"\n" + \
            "GLUE2EntityCreationTime: "+__creationtime__+"\n" + \
            "GLUE2ShareDescription: "+sa_descr+"\n" + \
            "GLUE2StorageShareTag: "+self.name+"\n" + \
            "GLUE2StorageShareAccessLatency: online"+"\n" + \
            "GLUE2StorageShareRetentionPolicy: output"+"\n" + \
            "GLUE2StorageShareExpirationMode: releasewhenexpired"+"\n" + \
            "GLUE2StorageShareServingState: "+"production"+"\n" + \
            "GLUE2ShareServiceForeignKey: "+self.server.name+"\n" + \
            "GLUE2StorageShareStorageServiceForeignKey: "+self.server.name+"\n" + \
            ""

      # Build StorageShareCapacity
      capacity_id = share_id + "/capacity"
      print "dn: Glue2StorageShareCapacityId="+capacity_id+",Glue2ShareId="+share_id+",Glue2ServiceId="+self.server.name+",GLUE2GroupID=resource,o=glue\n" + \
            "objectClass: GLUE2StorageShareCapacity\n" + \
            "GLUE2StorageShareCapacityID: "+capacity_id+"\n" + \
            "GLUE2EntityCreationTime: "+__creationtime__+"\n" + \
            "GLUE2StorageShareCapacityType: online"+"\n" + \
            "GLUE2StorageShareCapacityTotalSize: "+total.GB()+"\n" + \
            "GLUE2StorageShareCapacityUsedSize: "+used.GB()+"\n" + \
            "GLUE2StorageShareCapacityFreeSize: "+free.GB()+"\n" + \
            "GLUE2StorageShareCapacityReservedSize: "+reserved.GB()+"\n" + \
            "GLUE2StorageShareCapacityStorageShareForeignKey: "+share_id+"\n" + \
            ""

      mapping_policy_id = share_id + "/MP"
      print "dn: GLUE2PolicyID="+mapping_policy_id+", GLUE2ShareId="+share_id+",Glue2ServiceId="+self.server.name+",GLUE2GroupID=resource,o=glue\n" + \
            "objectClass: GLUE2Policy\n" + \
            "objectClass: GLUE2MappingPolicy\n" + \
            "GLUE2PolicyID: "+mapping_policy_id+"\n" + \
            "GLUE2EntityCreationTime: "+__creationtime__+"\n" + \
            "GLUE2PolicyScheme: org.glite.standard\n" + \
            "GLUE2MappingPolicyShareForeignKey: "+share_id+"\n" + \
            "GLUE2PolicyRule: VO:"+vo+"\n" + \
            "GLUE2PolicyUserDomainForeignKey: "+vo+"\n" + \
            ""
          
  # Compute SA total,installed,used,free,reserved space for a given SA.
  # Used by methods producing Glue v1 and GLUE v2 information.
  def glueSACharacteristics(self):
    if len(self.vo_list) == 1:
      suffix = ''
    else:
      suffix = 's'
      
    if self.type == 1:
      sa_descr = "Reserved space for VO%s %s (space token %s)" % (suffix,self.vo_str(),self.name)
      total,free,used,unavailable = self.reservationSpace()
      installed = self.total
      reserved = self.reserved
    else:
      sa_descr = "Unreserved space for VO%s %s" % (suffix,self.vo_str())
      # Total space published is the total unreserved space.
      # Unreserved space published is pool size minus reserved space in it and minus unavailable space
      # due to disabled/read-only FS. Total space is never less than used space.
      # Compute used space without taking into account unavailable space to avoid side effect of negative total/free.
      # Unavailable used space (due to disable file systems) is removed from used space.
      # self.total reflects configured capacity independently of unavailable parts.
      # Free space ('free') already takes into account unavailable free space (ignored by DPM). It is allowed to be negative
      # to reflect situations with too much unavailable space compared to reserved space. Assume free=0 when internally negative.
      reserved = self.reserved
      installed = self.total - reserved
      total = installed  - self.unavailableUsed - self.unavailableFree
      used = total - self.free
      if int(total) < int(used):
        debug(1,"Pool %s: total reset to used (%s)" % (self.name,used))
        total = used
      free = self.free
      if int(total) < int(free):
        debug(1,"Pool %s: free reset to total (%s)" % (self.name,total))
        free = total
      if int(total) < int(reserved):
        debug(1,"Pool %s: reserved reset to total (%s)" % (self.name,total))
        reserved = total
      if int(free) < 0:
        debug(1,"Pool %s: free count negative (%s). Resetting to 0." % (self.name,free))
        free = Size(0)
      if int(used) < 0:
        debug(1,"Pool %s: used count negative (%s). Resetting to 0." % (self.name,used))
        used = Size(0)
      if int(reserved) < 0:
        debug(1,"Pool %s: reserved count negative (%s). Resetting to 0." % (self.name,reserved))
      if int(installed) < 0:
        debug(1,"Pool %s: installed count negative (%s). Resetting to 0." % (self.name,installed))
        installed = Size(0)
      if int(total) < 0:
        debug(1,"Pool %s: total count negative (%s). Resetting to 0." % (self.name,total))
        total = Size(0)
      reserved = Size(0)
    
    return total,reserved,free,used,installed,sa_descr
    
  # Use a dictionary for linkedSA for easier duplicate elimination in case of tokens
  # and for faster access to linked SA name.
  # If sa is None, return an empty dictionnary
  def addLinkedSA(self,sa):
    if not sa:
      return {}
    if not self.linkedSA:
      self.linkedSA = {}
    self.linkedSA[sa.name] = sa

  # Return parent pool name.
  # This method assumes there is only one linkedSA in this context and returns the name of
  # the first one.
  def parentPoolName(self):
    if len(self.linkedSA) == 1:
      pool_name = self.linkedSA.keys()[0]
      return pool_name
    elif not linkedSA:
      debug(0,"FATAL: No parent pool defined for token %s" % self.name)
    else:
      debug(0,"FATAL: Several parent pool defined for token %s" % self.name)

  # Compute an updated view for total, free and unavailable space for reservations.
  # Take into account space that cannot be allocated because
  # there is too much unavailable space compared to reserved space.
  # Total is never allowed to be lower than used space (and thus is always >=0).
  # Free is set to 0 if it happens to be negative (because of unavailable FS)
  def reservationSpace(self):
    pool_name = self.parentPoolName()
    # Unavailable space has already been removed from pool free space in addFS()
    pool_free = self.linkedSA[pool_name].free
    free = self.free
    total = self.total
    # Don't take into account pool unavailableUsed as we don't know which reservation is affected.
    used = self.reserved - self.free
    # Check if there is enough online free space to fulfill reservation free space.
    # There is a potential problem only if the free space in the pool is negative as 
    # a result of unavailable file systems.
    if int(pool_free) < 0:
      unavailable = free - self.linkedSA[pool_name].FSFree
      if int(unavailable) > 0:
        debug(1,"Unused reserved space (%s) larger than available free space (%s). Reducing free/total space in the reservation." % (free,self.linkedSA[pool_name].FSFree))
        total -= unavailable
        free = self.linkedSA[pool_name].FSFree
        if int(total) < int(used):
          debug(1,"Reservation %s: total reset to used (%s)" % (self.name,used))
          total = used
      else:
        unavailable = 0
    else:
      unavailable = 0
    # Just in case... should not happen.
    if int(free) < 0:
      debug(1,"Reservation %s: free count negative. Reset to 0." % (self.name))
      free = Size(0)
    return total, free, used, unavailable

  # tokenList is a generator returning list of tokens in line of str_max_len characters
  def tokenList(self,str_max_len=50):
    # First build a sorted list of tokens
    st_list = self.linkedSA.keys()
    st_list.sort()

    # Create a list of string of str_max_len characters
    st_list_str = []
    i = 0
    st_list_str.append('')
    for st in st_list:
      if len(st_list_str[i])+len(st) >= str_max_len:
        st_list_str[i] += ','
        i += 1
        st_list_str.append('')
      elif len(st_list_str[i]) > 0:
        st_list_str[i] += ', '
      st_list_str[i] += st

    # Return line one by one
    for line in st_list_str:
      yield(line)
  
  # acbrStr is a generator returning list of tokens in line of str_max_len characters
  def acbrStr(self,details=False,allVos=False,str_max_len=50):
    # Create a list of string of str_max_len characters
    acbr_list_str = []
    parenthesis = False
    i = 0
    if allVos:
      acbr_list_str.append('all VOs')
      if details:
        acbr_list_str[i] += ' ('
        parenthesis = True
    else:
      acbr_list_str.append('')
    prefix_len = len(acbr_list_str[i])

    if (not allVos) or details:
      for fqan in self.acbr:
        if len(acbr_list_str[i])+len(fqan) >= str_max_len:
          acbr_list_str[i] += ','
          i += 1
          acbr_list_str.append('')
        elif len(acbr_list_str[i]) > prefix_len:
          acbr_list_str[i] += ', '
        prefix_len = 0
  
        if self.server.fqanInVoList(fqan):
          acbr_list_str[i] += fqan
        else:
          # Mark the FQAN as unusable as the VO is not configured
          debug(1,'VO not configured: skipping FQAN %s' % (fqan))
          acbr_list_str[i] += '(%s)' % fqan

    if parenthesis:
      acbr_list_str[i] += ')'
      
    # Return line one by one
    for line in acbr_list_str:
      yield(line)
  
  # Generator returning each FQAN in Glue ACBR format.
  # An FQAN is skipped if the corresponding VO is not configured
  # or if there is an entry defined for the whole VO.
  def glueACBR(self,acbr=None):
    if acbr == None:
      acbr = self.acbr
    for fqan in acbr:
      fqan_toks = fqan.split('/',1)
      if self.server.fqanInVoList(fqan_toks[0]):
        prefix = None
        if len(fqan_toks) == 1:
          prefix = 'VO:'
        else:
          # Add only if there is no entry for the whole VO
          if not fqan_toks[0] in self.vo_list[fqan_toks[0]]:
            prefix = 'VOMS:/'
          else:
            debug(1,'VO %s: an entry for the whole VO is present, FQAN %s skipped' % (fqan_toks[0],fqan))
        if prefix:
          yield ("%s%s" % (prefix,fqan))
      else:
        debug(1,'VO %s not configured: skipping FQAN %s' % (fqan_toks[0],fqan))

  def addFS(self,fs):
    if self.type == 1:
      print "ERROR: file systems are not supported on space reservations (%s)" % sa.name
      sys.exit(21)
    debug(1, "Adding FS %s:%s" % (fs.server,fs.fs))
    if not self.fs_list:
      self.fs_list = []
    self.fs_list.append(FS(fs))
    # Add FS space to unavailable if FS is disabled or read-only
    unavailableFree,unavailableUsed = self.fs_list[-1].unavailSpace()
    self.unavailableFree += unavailableFree
    self.unavailableUsed += unavailableUsed
    # When a FS is disabled/read-only, DPM doesn't count it anymore in the pool capacity and in the free space.
    # Fix it so that 'total' is the real capacity, including unavailable parts.
    if unavailableFree:
      debug(1,"Updating FS %s:%s total space to include unavailable space (%s)" % (fs.server,fs.fs,self.fs_list[-1].total))
      self.total +=  self.fs_list[-1].total
    # If file system is online, add its free space to the total actual free space in the pool (based on FS usage)
    else:
      self.FSFree += self.fs_list[-1].free

  def addAllFS(self):
    st,fs_list = dpm.dpm_getpoolfs(self.name)# get fs in a pool
    for fs in fs_list:
      self.addFS(fs)

  # Compute aggregated space for the SA if type is pool.
  # Aggregated space is pool capacity, used space in pool and in any child reservation, free space in pool
  # and any child reservation.
  def aggregatedSpace(self):
    if self.type == 0:
      total = self.total
      free = self.free - self.unavailableFree
      used = self.total - self.reserved - self.free
      reserved = self.reserved
      if len(self.linkedSA) > 0:
        for reservation in self.linkedSA.values():
          free += reservation.free
          reserv_used = reservation.reserved - reservation.free
          used += reserv_used
      return total,used,free,reserved
    else:
      debug(1,'WARNING: aggregatedSpace() called for a reservation. Ignoring...')
      return Size(0),Size(0),Size(0),Size(0)
    
# Class FS describes a DPM file system.
# Class attribues are derived from DPM dpm_fs structure returned by dpm_getpoolfs.
#
# Available attributes as of DPM 1.6.10 (from man dpm_getpoolfs) are:
#       struct dpm_fs {
#            char      poolname[CA_MAXPOOLNAMELEN+1];
#            char      server[CA_MAXHOSTNAMELEN+1];
#            char      fs[80];
#            u_signed64     capacity;
#            u_signed64     free;
#            int       status;
#       };

class FS(object):
  def __init__(self,fs):
    self.server = fs.server
    self.pool = fs.poolname
    self.fs = fs.fs
    self.total = Size(fs.capacity)
    self.free = Size(fs.free)
    self.status = Status(fs.status)
    
  def display(self,header=False,ident=4,details=False):
    used = self.total - self.free
    if int(self.total) == 0:
      free_percent = 0.
    else:
      free_percent = self.free / self.total * 100
    prefix = ''
    for i in range(ident):
      prefix = ident * ' '
    if header:
      print prefix+"%-40s%-8s%10s%10s%10s" % ('File System','Status','Capacity','Used','Free')
      print prefix+"-----------------------------------------------------------------------------------"
    print prefix+"%-40s%-8s%10s%10s%10s (%.1f%%)" % (self.server+':'+self.fs,self.status, self.total,used,self.free,free_percent)
   
  # Return unavailable space if file system is disabled or read-only.
  # There is a separate count of unavailable free and used space
  def unavailSpace(self):
    if int(self.status) == 0:            # Online
      free = 0
      used = 0
    elif int(self.status) == 1:          # Disabled
      free = self.free
      used = self.total - self.free
    elif int(self.status) == 2:          # Read-only
      free = self.free
      used = 0
    else:
      debug(0,"FATAL: Unknown file system status %d for %s:%s" % (self.status,self.server,self.fs))
      sys.exit(10)
    return free,used

def handler(signum, frame):
    raise Exception("Xrootd not working.")


# Class to represent a DPM server.
# VO configured on the server are stored internally as a dictionnary for easier
# checking of FQAN later.
class DPMServer(object):
  def __init__(self,name,domain,
               basedir,
               volist=None,
               site=None,
               subsite=None,
               status='production',
               vo_home_root='/undefined',
               gsiftp=2811,
               srmv1=8443,
               srmv2=8446,
               webdav=443,
               xroot=1094,
               legacy=False):
    self.name = name
    self.domain = domain
    self.basedir = basedir
    self.site = site
    self.subsite = subsite
    self.vo_home_root = vo_home_root
    self.vo_list = volist

    self.capacity = Size(0)
    self.used = Size(0)
    self.free = Size(0)
    self.reserved = Size(0)
    self.vo_list = {}
    if volist:
      for vo in volist:
        self.addVO(vo)

    self.core_services = []
    self.access = {}
    self.control = {}
    self.interface = {}
    self.version = 'unset'
    self.emi_version = self.getEMIVersion()
    if self.name ==  socket.getfqdn():    
      self.core_services.append(StorageService('dpm'))
      self.core_services.append(StorageService('dpns'))
  
      version_with_age = dpm.dpm_ping(socket.getfqdn())[1] #Alternatives to DPM ping?
      self.version = version_with_age.split('-')[0]
  
      # self.access is a dictionary with 1 entry per protocol name supported.
      # The value for each protocol is a dictionnary with key 'protocol', 'port' 
      # and 'version' required and key 'security' optional (or None) for 
      # unsecure protocols.
      if gsiftp:
        params = Interface(name='gsiftp', protocol="gsiftp", port=gsiftp, version='2.0.0')
        if params.enabled():
            self.access['gsiftp'] = {'protocol': 'gsiftp', 'port': gsiftp, 'version': '2.0.0', 'security': 'GSI' }
            self.interface['gsiftp'] = params
      if srmv1:
        params = ControlProtocol(name='srmv1',port=srmv1)
        if params.enabled():
          self.control['srmv1'] = params:
      if srmv2:
        params = ControlProtocol(name='srmv2',port=srmv2)
        if params.enabled():
          self.control['srmv2'] = params
      # self.interface is for webdav/xroot
      if webdav:
        params = Interface(name='webdav',protocol="https",port=webdav,version=self.version, semantics='http://tools.ietf.org/html/rfc4918')
        if params.enabled():
          self.access['webdav'] = {'protocol': 'https', 'port': webdav, 'version': self.version, 'security': 'GSI' }
          self.interface['webdav'] = params
        params = Interface(name='https',protocol="https",port=webdav,version=self.version, semantics='http://tools.ietf.org/html/rfc4918')
        if params.enabled():
          self.access['https'] = {'protocol': 'https', 'port': webdav, 'version': self.version, 'security': 'GSI' }
          self.interface['https'] = params
      if xroot:
        xrootd_version = 'None'
        signal.signal(signal.SIGALRM, handler)
        signal.alarm(5)
        try:
            pro = subprocess.Popen("xrdfs "+socket.getfqdn()+" query stats p",shell=True,stdout=subprocess.PIPE, stderr=subprocess.PIPE, preexec_fn=os.setsid)
            xrootd_query = pro.stdout.readline()
            try:
              os.killpg(pro.pid, signal.SIGKILL)
            except:
              pass
            error = pro.communicate()[1]
            if not error:
                # Parsing of the answer to get the xrootd version number
                start = xrootd_query.find('dpmoss')+11
                end = xrootd_query.find('/',start)
                xrootd_version = xrootd_query[start:end]
        except Exception, exc:
            pass
        signal.alarm(0)
        params = Interface(name='xroot',protocol="xroot",port=xroot,version=xrootd_version)
        if params.enabled():
          self.access['xroot'] = {'protocol': 'xroot', 'port': xroot, 'version': xrootd_version, 'security': 'GSI' }
          self.interface['xroot'] = params
      
      # Define server status.
      # Server is considered up and running if DPM, DPNS and at least one of the control protocols
      # are up and running.
      self.running = False
      for protocol in self.control:
          if self.control[protocol].running():
            self.running = True
      for service in self.core_services:
        if not service.running():
          self.running = False
      server_reachable = False
      if self.running:
        self.status = 'production'
      else:
        self.status = 'closed'
  
  # Add a VO in configured VO list.
  # VO are entered as keys in a dictionnary for easier lookup later.
  def addVO(self,vo):
    self.vo_list[vo] = ''         # Value is meaningless
  
  # Returns list of configured VO as an ordered list
  def voList(self):
    vo_list = self.vo_list.keys()
    vo_list.sort()
    return vo_list
  
  # Return VO home path in the server namespace
  def getVOHome(self,vo):
    return self.vo_home_root + "/" + vo
  
  # Check if FQAN belongs to a configured VO
  def fqanInVoList(self,fqan):
    fqan_toks = fqan.split('/',1)
    if fqan_toks[0] in self.vo_list:
      return True
    else:
      return False
  
  # Compute total/used space based on configured pools and reservations.
  # Pool list must be a list of SA, not a dictionnary.
  def setSpace(self,pool_list):
    self.capacity = Size(0)
    self.used = Size(0)
    self.reserved = Size(0)
    for pool in pool_list:
      total,used,free,reserved = pool.aggregatedSpace()
      self.capacity += total
      self.used += used
      self.reserved += reserved
    self.free = self.capacity - self.used - self.reserved
    if int(self.free) < 0:
    	debug(1,"Free count negative(%s). Resetting to 0." % (self.free))
        self.free = Size(0)

  # Returns the version of the EMI distribution, as read from /etc/emi-version.
  # Requires the package emi-version to be installed to work.
  def getEMIVersion(self):
    try:
      return open("/etc/emi-version", "r").read().strip()
    except:
      return 'unset'

  # Generate a GlueSE object representing the server.
  # GlueSEName is normally based on grid site name, except if there is a subsite defined.
  def glueSE(self,legacy=False,glue_versions={}):
    se_name = self.site
    if self.subsite:
      se_name += '-%s' % self.subsite
    if legacy:
      se_name += ':srm'
    else:
      se_name += ' DPM server'
    for glue_version in glue_versions:
      if glue_version == 'v1':
        print "dn: GlueSEUniqueID="+self.name+",mds-vo-name=resource,o=grid\n" + \
              "objectClass: GlueTop\n" + \
              "objectClass: GlueSE\n" + \
              "objectClass: GlueKey\n" + \
              "objectClass: GlueSchemaVersion\n" + \
              "GlueSEUniqueID: "+self.name+"\n" + \
              "GlueSEName: "+se_name+"\n" + \
              "GlueSEArchitecture: multidisk\n" + \
              "GlueSEImplementationName: DPM\n" + \
              "GlueSEImplementationVersion: "+self.version+"\n" + \
              "GlueSEStatus: "+self.status.title()+"\n" + \
              "GlueSETotalOnlineSize: "+self.capacity.GB()+"\n" + \
              "GlueSEUsedOnlineSize: "+self.used.GB()+"\n" + \
              "GlueSESizeTotal: "+self.capacity.GB()+"\n" + \
              "GlueSESizeFree: "+self.free.GB()+"\n" + \
              "GlueSETotalNearlineSize: 0\n" + \
              "GlueSEUsedNearlineSize: 0\n" + \
              "GlueForeignKey: GlueSiteUniqueID="+self.site+"\n" + \
              "GlueSchemaVersionMajor: 1\n" + \
              "GlueSchemaVersionMinor: 3\n" + \
              ""
      else:
        print "dn: Glue2ServiceId="+self.name+",GLUE2GroupID=resource,o=glue\n" + \
              "objectClass: GLUE2Service\n" + \
              "objectClass: GLUE2StorageService\n" + \
              "GLUE2ServiceID: "+self.name+"\n" + \
              "GLUE2EntityCreationTime: "+__creationtime__+"\n" + \
              "GLUE2EntityOtherInfo: ProfileName=EGI\n" + \
              "GLUE2EntityOtherInfo: ProfileVersion=1.0\n" + \
              "GLUE2ServiceType: DPM\n" + \
              "GLUE2ServiceQualityLevel: production"+"\n" + \
              "GLUE2ServiceCapability: data.access.flatfiles"+"\n" + \
              "GLUE2ServiceCapability: data.management.replica"+"\n" + \
              "GLUE2ServiceCapability: data.management.storage"+"\n" + \
              "GLUE2ServiceCapability: data.management.transfer"+"\n" + \
              "GLUE2ServiceCapability: data.transfer"+"\n" + \
              "GLUE2ServiceCapability: executionmanagement.reservation"+"\n" + \
              "GLUE2ServiceCapability: security.authentication"+"\n" + \
              "GLUE2ServiceCapability: security.authorization"+"\n" + \
              "GLUE2ServiceAdminDomainForeignKey: "+self.site+"\n" + \
              "GLUE2EntityOtherInfo: InfoProviderName=dome-listspaces"+"\n" + \
              "GLUE2EntityOtherInfo: InfoProviderVersion="+__version__+"\n" + \
              "GLUE2EntityOtherInfo: InfoProviderHost="+socket.getfqdn()+"\n" + \
              ""

        capacity_id = self.name + "/capacity"
        print "dn: GLUE2StorageServiceCapacityId="+capacity_id+",Glue2ServiceId="+self.name+",GLUE2GroupID=resource,o=glue\n" + \
              "objectClass: GLUE2StorageServiceCapacity\n" + \
              "GLUE2StorageServiceCapacityID: "+capacity_id+"\n" + \
              "GLUE2EntityCreationTime: "+__creationtime__+"\n" + \
              "GLUE2StorageServiceCapacityType: online"+"\n" + \
              "GLUE2StorageServiceCapacityTotalSize: "+self.capacity.GB()+"\n" + \
              "GLUE2StorageServiceCapacityFreeSize: "+self.free.GB()+"\n" + \
              "GLUE2StorageServiceCapacityUsedSize: "+self.used.GB()+"\n" + \
              "GLUE2StorageServiceCapacityReservedSize: "+self.reserved.GB()+"\n" + \
              "GLUE2StorageServiceCapacityStorageServiceForeignKey: "+self.name+"\n" + \
              ""

        manager_id = self.name + "/manager"
        print "dn: GLUE2ManagerID="+manager_id+",Glue2ServiceId="+self.name+",GLUE2GroupID=resource,o=glue\n" + \
              "objectClass: GLUE2Manager\n" + \
              "objectClass: GLUE2StorageManager\n" + \
              "GLUE2ManagerID: "+manager_id+"\n" + \
              "GLUE2EntityCreationTime: "+__creationtime__+"\n" + \
              "GLUE2ManagerProductName: DPM\n" + \
              "GLUE2ManagerProductVersion: "+self.version+"\n" + \
              "GLUE2ManagerServiceForeignKey: "+self.name+"\n" + \
              "GLUE2StorageManagerStorageServiceForeignKey: "+self.name+"\n" + \
              ""
        """ # There is no way to get the physical total size and as nobody is publishing it, we forget it for the moment.

        resource_id = self.name + "/resource"
        print "dn: GLUE2ResourceID="+resource_id+",GLUE2ManagerID="+manager_id+",Glue2ServiceId="+self.name+",GLUE2GroupID=resource,o=glue\n" + \
              "objectClass: GLUE2Resource\n" + \
              "objectClass: GLUE2DataStore\n" + \
              "GLUE2ResourceID: "+resource_id+"\n" + \
              "GLUE2EntityCreationTime: "+__creationtime__+"\n" + \
              "GLUE2DataStoreType: disk\n" + \
              "GLUE2DataStoreLatency: online\n" + \
              "GLUE2DataStoreTotalSize: ?\n" + \
              "GLUE2DataStoreStorageManagerForeignKey: "+manager_id+"\n" + \
              ""
        """

  # Generate GlueSEAccessProtocol
  def glueAccess(self,glue_versions={}):
    for protocol in self.access:
      params = self.access[protocol]
      debug(1,"Generating GlueSEAccessProtocol for %s" % protocol)
      protocol_localid = protocol
      dir(params)
      if ('security' in params) and params['security']:
        security = params['security']
      else:
        security = 'unset'
        
      for glue_version in glue_versions:
        if glue_version == 'v1':
          print "dn: GlueSEAccessProtocolLocalID="+protocol_localid+",GlueSEUniqueID="+self.name+",mds-vo-name=resource,o=grid\n" + \
                "objectClass: GlueTop\n" + \
                "objectClass: GlueSEAccessProtocol\n" + \
                "objectClass: GlueKey\n" + \
                "objectClass: GlueSchemaVersion\n" + \
                "GlueSEAccessProtocolLocalID: "+protocol_localid+"\n" + \
                "GlueSEAccessProtocolType: "+protocol+"\n" + \
                "GlueSEAccessProtocolVersion: "+params['version']+"\n" + \
                "GlueSEAccessProtocolSupportedSecurity: "+security+"\n" + \
                "GlueSEAccessProtocolEndpoint: %s://%s:%s" % (params['protocol'],self.name, params['port'])+"\n" + \
                "GlueChunkKey: GlueSEUniqueID="+self.name+"\n" + \
                "GlueSchemaVersionMajor: 1\n" + \
                "GlueSchemaVersionMinor: 3\n" + \
                ""
        elif glue_version == 'v2':
          protocol_localid = self.name + "/" + protocol
          print "dn: Glue2StorageAccessProtocolId="+protocol_localid+",Glue2ServiceId="+self.name+",GLUE2GroupID=resource,o=glue\n" + \
                "objectClass: GLUE2StorageAccessProtocol\n" + \
                "GLUE2StorageAccessProtocolID: "+protocol_localid+"\n" + \
                "GLUE2EntityCreationTime: "+__creationtime__+"\n" + \
                "GLUE2StorageAccessProtocolType: "+protocol+"\n" + \
                "GLUE2StorageAccessProtocolVersion: "+params['version']+"\n" + \
                "GLUE2StorageAccessProtocolStorageServiceForeignKey: "+self.name+"\n" + \
                ""

  # Generate GlueSEControlProtocol
  def glueControl(self,glue_versions):
    for protocol in self.control:
      debug(1,"Generating GlueSEControlProtocol for %s" % protocol)
      params = self.control[protocol]

      for glue_version in glue_versions:
        endpoint_url = "httpg://"+self.name+":"+str(params.port)+params.url
        if glue_version == 'v1':
          protocol_localid = protocol
          print "dn: GlueSEControlProtocolLocalID="+protocol_localid+",GlueSEUniqueID="+self.name+",mds-vo-name=resource,o=grid\n" + \
                "objectClass: GlueTop\n" + \
                "objectClass: GlueSEControlProtocol\n" + \
                "objectClass: GlueKey\n" + \
                "objectClass: GlueSchemaVersion\n" + \
                "GlueSEControlProtocolLocalID: "+protocol_localid+"\n" + \
                "GlueSEControlProtocolType: SRM\n" + \
                "GlueSEControlProtocolEndpoint: " + endpoint_url +"\n" + \
                "GlueSEControlProtocolVersion: "+params.version+"\n" + \
                "GlueChunkKey: GlueSEUniqueID="+self.name+"\n" + \
                "GlueSchemaVersionMajor: 1\n" + \
                "GlueSchemaVersionMinor: 3\n" + \
                ""
        elif glue_version == 'v2':
          protocol_localid = self.name + "/srm/" + params.version
          access_policy_id = protocol_localid + "/AP"
          protocol_ok = params.running()
          if protocol_ok and self.status == 'production':
            health = 'ok'
            health_msg = 'DPM server, DPNS server and %s server are up and running' % params.service
          else:
            health = 'error'
            if not protocol_ok:
              health_msg = '%s server is down' % params.service
            else:
              health_msg = 'DPM and/or DPNS daemon is down'
          print "dn: GLUE2EndpointId="+protocol_localid+",Glue2ServiceId="+self.name+",GLUE2GroupID=resource,o=glue\n" + \
                "objectClass: GLUE2Endpoint\n" + \
                "objectClass: GLUE2StorageEndpoint\n" + \
                "GLUE2EndpointID: "+protocol_localid+"\n" + \
                "GLUE2EntityCreationTime: "+__creationtime__+"\n" + \
                "GLUE2EndpointURL: "+endpoint_url+"\n" + \
                "GLUE2EndpointCapability: data.access.flatfiles"+"\n" + \
                "GLUE2EndpointCapability: data.management.replica"+"\n" + \
                "GLUE2EndpointCapability: data.management.storage"+"\n" + \
                "GLUE2EndpointCapability: data.management.transfer"+"\n" + \
                "GLUE2EndpointCapability: data.transfer"+"\n" + \
                "GLUE2EndpointCapability: executionmanagement.reservation"+"\n" + \
                "GLUE2EndpointCapability: security.authentication"+"\n" + \
                "GLUE2EndpointCapability: security.authorization"+"\n" + \
                "GLUE2EndpointInterfaceName: SRM\n" + \
                "GLUE2EndpointInterfaceVersion: "+params.version+"\n" + \
                "GLUE2EndpointImplementationName: DPM"+"\n" + \
                "GLUE2EndpointImplementationVersion: "+self.version+"\n" + \
                "GLUE2EndpointTechnology: webservice\n" + \
                "GLUE2EndpointWSDL: "+params.wsdl+"\n" + \
                "GLUE2EndpointSemantics: "+params.semantics+"\n" + \
                "GLUE2EndpointQualityLevel: production\n" + \
                "GLUE2EndpointServingState: "+params.status()+"\n" + \
                "GLUE2EndpointHealthState: "+health+"\n" + \
                "GLUE2EndpointHealthStateInfo: "+health_msg+"\n" + \
                "GLUE2EntityOtherInfo: InfoProviderName=dome-listspaces"+"\n" + \
                "GLUE2EntityOtherInfo: InfoProviderVersion="+__version__+"\n" + \
                "GLUE2EntityOtherInfo: InfoProviderHost="+socket.getfqdn()+"\n" + \
                "GLUE2EndpointServiceForeignKey: "+self.name+"\n" + \
                ""
          print "dn: GLUE2PolicyID="+access_policy_id+", GLUE2EndpointId="+protocol_localid+",Glue2ServiceId="+self.name+",GLUE2GroupID=resource,o=glue\n" + \
                "objectClass: GLUE2Policy\n" + \
                "objectClass: GLUE2AccessPolicy\n" + \
                "GLUE2PolicyID: "+access_policy_id+"\n" + \
                "GLUE2EntityCreationTime: "+__creationtime__+"\n" + \
                "GLUE2PolicyScheme: org.glite.standard\n" + \
                "GLUE2AccessPolicyEndpointForeignKey: "+protocol_localid
          if self.vo_list:
            for vo in self.vo_list:
              print "GLUE2PolicyRule: VO:"+vo+"\n" + \
                    "GLUE2PolicyUserDomainForeignKey: "+vo
          else:
            debug(1,"VO list not defined in DPMServer object: cannot define GLUE2Policy object "+access_policy_id)
          print ""


  def glueInterfaces(self,glue_versions):
    for interface_name in self.interface:
      debug(1,"Generating Glue2Endpoint for %s" % interface_name)
      interface = self.interface[interface_name]

      # check service status
      interface_ok = interface.running()
      if interface_ok:
        health = 'ok'
        health_msg = '%s up and running' % interface_name
      else:
        health = 'error'
        health_msg = '%s seems to be down' % interface_name

      endpoint_id = "%s/%s" % (self.name, interface_name)
      access_policy_id = endpoint_id + "/AP"
      print "dn: GLUE2EndpointId="+endpoint_id+",Glue2ServiceId="+self.name+",GLUE2GroupID=resource,o=glue\n" + \
           "objectClass: GLUE2Endpoint\n" + \
           "objectClass: GLUE2StorageEndpoint\n" + \
           "GLUE2EndpointID: "+endpoint_id+"\n" + \
           "GLUE2EntityCreationTime: "+__creationtime__+"\n" + \
           "GLUE2EndpointURL: "+interface.endpoint(self.name)+"\n" + \
           "GLUE2EndpointCapability: data.access.flatfiles"+"\n" + \
           "GLUE2EndpointCapability: data.management.replica"+"\n" + \
           "GLUE2EndpointCapability: data.management.storage"+"\n" + \
           "GLUE2EndpointCapability: data.management.transfer"+"\n" + \
           "GLUE2EndpointCapability: data.transfer"+"\n" + \
           "GLUE2EndpointCapability: executionmanagement.reservation"+"\n" + \
           "GLUE2EndpointCapability: security.authentication"+"\n" + \
           "GLUE2EndpointCapability: security.authorization"+"\n" + \
           "GLUE2EndpointInterfaceName: "+interface_name+"\n"+ \
           "GLUE2EndpointInterfaceVersion: "+interface.version+"\n" + \
           "GLUE2EndpointImplementationName: DPM"+"\n" + \
           "GLUE2EndpointImplementationVersion: "+self.version+"\n" + \
           "GLUE2EndpointTechnology: webservice"
      if interface.semantics:
           print "GLUE2EndpointSemantics: "+interface.semantics
      print "GLUE2EndpointQualityLevel: production\n" + \
           "GLUE2EndpointServingState: "+interface.status()+"\n" + \
           "GLUE2EndpointHealthState: "+health+"\n" + \
           "GLUE2EndpointHealthStateInfo: "+health_msg+"\n" + \
           "GLUE2EntityOtherInfo: InfoProviderName=dome-listspaces"+"\n" + \
           "GLUE2EntityOtherInfo: InfoProviderVersion="+__version__+"\n" + \
           "GLUE2EntityOtherInfo: InfoProviderHost="+socket.getfqdn()+"\n" + \
           "GLUE2EndpointServiceForeignKey: "+self.name+"\n" + \
           ""
      print "dn: GLUE2PolicyID="+access_policy_id+", GLUE2EndpointId="+endpoint_id+",Glue2ServiceId="+self.name+",GLUE2GroupID=resource,o=glue\n" + \
           "objectClass: GLUE2Policy\n" + \
           "objectClass: GLUE2AccessPolicy\n" + \
           "GLUE2EntityCreationTime: "+__creationtime__+"\n" + \
           "GLUE2PolicyID: "+access_policy_id+"\n" + \
           "GLUE2PolicyScheme: org.glite.standard\n" + \
           "GLUE2AccessPolicyEndpointForeignKey: "+endpoint_id
      if self.vo_list:
        for vo in self.vo_list:
          print "GLUE2PolicyRule: VO:"+vo+"\n" + \
                "GLUE2PolicyUserDomainForeignKey: "+vo
      else:
        debug(1,"VO list not defined in DPMServer object: cannot define GLUE2Policy object "+access_policy_id)
      print ""

# Class to represent a service (eg. DOME, DPM-GSIFTP ...).
class StorageService(object):
  def __init__(self,name=None):
    # Actual service name or related sysconfig file may not match the internal service name
    elif name == 'gsiftp':
      self.service = 'dpm-gsiftp'
      self.config = 'dpm-gsiftp'
    elif name == 'srmv2':
      self.service = 'srmv2.2'
      self.config = 'srmv2.2'
    elif name == 'webdav' or name == 'https':
      self.service = 'httpd'
      self.config = 'httpd'
    elif name == 'xroot':
      self.service = 'xrootd'
      self.config = 'xrootd'
    else:
      self.service = name
      self.config = name
      
  # Check if a service is enabled
  def enabled(self):
    if (os.system('/sbin/chkconfig %s >/dev/null 2>&1' % self.service) == 0) and os.path.exists('/etc/sysconfig/'+self.config):
      enabled = True
    else:
      debug(1,"Service %s not configured" % (self.service))
      enabled = False
    return enabled
      
  # Return true if the status is running
  def running(self):
    debug(1,"Checking if service %s is running..." % (self.service))
    if (os.system('/sbin/service %s status >/dev/null 2>&1' % self.service) and os.system('pgrep %s >/dev/null 2>&1' % self.service)):
      up = False
    else:
      up = True
    return up
  
  # Return service status as a string
  def status(self):
    if self.running():
      status = 'production'
    else:
      status = 'closed'
    return status
    

# Class to represent a control protocol (eg. SRM).
# Inherits from StorageService.

class ControlProtocol(StorageService):
  def __init__(self,name=None,port=None):
    StorageService.__init__(self,name)
    if name == 'srmv1':
      if port == None:
        port = 8443
      self.port = port
      self.version = '1.1.0'
      self.url = '/srm/managerv1'
      self.wsdl = 'http://sdm.lbl.gov/srm-wg/srm.v1.1.wsdl'
      self.semantics = 'http://sdm.lbl.gov/srm-wg/doc/SRM.v1.1.html'
    elif name == 'srmv2':
      if port == None:
        port = 8446
      self.port = port
      self.version = '2.2.0'
      self.url = '/srm/managerv2'
      self.wsdl = 'http://sdm.lbl.gov/srm-wg/srm.v2.2.wsdl'
      self.semantics = 'http://sdm.lbl.gov/srm-wg/doc/SRM.v2.2.html'
    else:
      print "ERROR: invalid control protocol '%s' (internal error)" % name
      sys.exit(20)
      
# Class to represent an interface (eg. webdav or xrootd)
class Interface(StorageService):
  def __init__(self,name=None,protocol=None,port=None,version=None,semantics=None):
    StorageService.__init__(self,name)
    self.name = name
    self.protocol = protocol
    self.port = port
    self.version = version
    self.semantics = semantics
  def endpoint(self, domain):
    return "%s://%s:%s" % (self.protocol, domain, self.port)

# Class to represent sizes and display value in a human readable unit.

class Size:
  def __init__(self,i):
    self.value = i
    
  def __add__(self,size):
    # If argument is not a Size instance, assume it is a numeric type
    if type(size) == type(self):
      size_val = size.value
    else:
      size_val = size
    return(Size(self.value+size_val))
           
  def __iadd__(self,size):
    # If argument is not a Size instance, assume it is a numeric type
    if type(size) == type(self):
      size_val = size.value
    else:
      size_val = size
    self.value += size_val
    return self
           
  def __sub__(self,size):
    # If argument is not a Size instance, assume it is a numeric type
    if type(size) == type(self):
      size_val = size.value
    else:
      size_val = size
    return(Size(self.value-size_val))
  
  def __isub__(self,size):
    # If argument is not a Size instance, assume it is a numeric type
    if type(size) == type(self):
      size_val = size.value
    else:
      size_val = size
    self.value -= size_val
    return self

  def __neg__(self):
    return Size(-self.value)

  # Div returns a float instead of Size object as this is generally the
  # expected behaviour
  def __div__(self,num):
    if type(num) == type(self):
      quotient = float(num)
    else:
      quotient = num
    return float(self) / quotient

  def __float__(self):
    return float(self.value)

  def __int__(self):
    return int(self.value)

  # Convert to a human readable unit followed by a suffix. The value is either a power of 2 or of 10 (SI unit),
  # based on option passed at dpm-listspaces invocation.
  # Negative size are used internally to keep track of unavailable space but they are always displayed as 0
  # as a negative size is meaningless.
  def __str__(self):
    if options.si_unit:
      base = 1000
    else:
      base = 1024
    if self.value != None:
      suffixes = ['', 'K', 'M', 'G', 'T', 'P', 'E', 'Z', 'Y']
      if self.value < 0:
        sizef = 0.
      else:
        sizef = float(self.value)
      if sizef == 0:
        new_size = 0.
        i = 0
      else:
        i = 8
        while i > 0:
          new_size = sizef / base**i
          if new_size >= 1:
            break
          else:      
            i -= 1
      return "%.2f%s" % (new_size, suffixes[i])
    else:
      return 'Undefined'

  def KB(self):
    return str(self.value/10**3)

  def MB(self):
    return str(self.value/10**6)

  def GB(self):
    return str(self.value/10**9)

  def TB(self):
    return str(self.value/10**12)


# Class to represent life time and print it in a human readable format

class LifeTime(object):
  def __init__(self,lifetime):
    self.value = lifetime       # Lifetime is in seconds
    
  def __str__(self):
    if not self.value:
      timestr = 'Undefined'
    elif self.value == 2147483647:
      timestr = 'Infinite'
    else:
      days,tmp = divmod(self.value, 86400)
      hours,tmp = divmod(tmp, 3600)
      minutes,seconds = divmod(tmp, 60)
      timestr = "%d-%2.2d:%2.2d:%2.2d" % (days,hours,minutes,seconds)    
    return timestr    

  # GLUE 1.x requires time in seconds
  def glue(self):
    if not self.value:
      timestr = 'Undefined'
    elif self.value == 2147483647:
      timestr = 'Permanent'
    else:
      timestr = str(self.value)
    return timestr


# Class to represent tetention policy and print it in a human readable format

class Retention(object):
  def __init__(self,retention):
    self.value = retention
    
  def __str__(self):
    if self.value == 'C':
      valuestr = 'Custodial'
    elif self.value == 'O':
      valuestr = 'Output'
    elif self.value == 'R' or self.value == '_':
      valuestr = 'Replica'
    else:
      valuestr = '*****'
    return valuestr


# Class to represent latency and print it in a human readable format

class Latency(object):
  def __init__(self,latency):
    self.value = latency
    
  def __str__(self):
    if self.value == 'N':
      valuestr = 'Nearline'
    elif self.value == 'O':
      valuestr = 'Online'
    else:
      valuestr = '*****'
    return valuestr

# Class to represent space type and print it in a human readable format

class SpaceType(object):
  def __init__(self,space_type):
    self.value = space_type
   
  def __str__(self):
    if self.value == '-':
      valuestr = 'Any'
    elif self.value == 'P':
      valuestr = 'Permanent'
    elif self.value == 'D':
      valuestr = 'Durable'
    elif self.value == 'V':
      valuestr = 'Volatile'
    else:
      valuestr = '*****'
    return valuestr

  def expirationMode(self):
    if self.value == '-' or self.value == 'P':
      valuestr = 'neverExpire'
    elif self.value == 'D':
      valuestr = 'warnWhenExpired'
    elif self.value == 'V':
      valuestr = 'releaseWhenExpired'
    else:
      valuestr = '*****'
    return valuestr


# Class to represent status and print it in a human readable format

class Status(object):
  def __init__(self,status):
    self.value = status
    
  def __int__(self):
    return self.value

  def __str__(self):
    if self.value == 0:
      valuestr = 'Online'
    elif self.value == 1 :
      valuestr = 'Disabled'
    elif self.value == 2:
      valuestr = 'Read-only'
    else:
      valuestr = '*****'
    return valuestr

class Dmlite(object):
  def __init__(self, conf):
	self.conf = conf
	self.setup()

  def setup(self):
        self.pluginManager = pydmlite.PluginManager()
        self.pluginManager.loadConfiguration(self.conf)
        self.stackInstance = pydmlite.StackInstance(self.pluginManager)
        self.poolManager = self.stackInstance.getPoolManager()

  	
  def getPoolInfo(self,poolname):
	pool = self.poolManager.getPool(poolname)
	try:
        	self.poolDriver = self.stackInstance.getPoolDriver(pool.type)
        except Exception, e:
                self.poolDriver = None
        self.poolHandler = self.poolDriver.createPoolHandler(poolname)
        capacity = self.poolHandler.getTotalSpace()
        free = self.poolHandler.getFreeSpace()
	return (capacity,free)


# Function to create GlueSA in legacy (Glue 1.2) format for unreserved space.
# There is one such entry per VO summing up all the unreserved spaces usable by the VO.
# The GlusSALocalID must be the VO name.
# SA matching reserved spaces in sa_list are ignored.
# Unavailable space is not taken into calculations.
#
# FIXME: ACBR of pools is not taken into account because of difficulties with dpns_getgrpbygids
  
def buildLegacySA(sa_list,vo_list):
  debug(1,"\nGenerating Glue 1.2 compatible GlueSA...")
  for vo in vo_list:
    free = Size(0)
    total = Size(0)
    for sa in sa_list:
      debug(1,"SA "+sa.name+" type: "+str(sa.type))
      if sa.type == 0 and vo in sa.acbr:
        total += sa.total
        free += sa.free
    used = total - free
    print "dn: GlueSALocalID="+vo+",GlueSEUniqueID="+dpm_server.name+",GLUE2GroupID=resource,o=grid\n" + \
          "objectClass: GlueSATop\n" + \
          "objectClass: GlueSA\n" + \
          "objectClass: GlueSAPolicy\n" + \
          "objectClass: GlueSAState\n" + \
          "objectClass: GlueSAAccessControlBase\n" + \
          "objectClass: GlueKey\n" + \
          "objectClass: GlueSchemaVersion\n" + \
          "GlueSALocalID: "+vo+"\n" + \
          "GlueSAName: Glue 1.2 legacy SA definition for VO "+vo+"\n" + \
          "GlueSAPath: "+self.server.getVOHome(vo)+"\n" + \
          "GlueSATotalOnlineSize: "+str(total.GB())+"\n" + \
          "GlueSAUsedOnlineSize: "+str(used.GB())+"\n" + \
          "GlueSAFreeOnlineSize: "+str(free.GB())+"\n" + \
          "GlueSAReservedOnlineSize: 0\n" + \
          "GlueSATotalNearlineSize: 0\n" + \
          "GlueSAUsedNearlineSize: 0\n" + \
          "GlueSAFreeNearlineSize: 0\n" + \
          "GlueSAReservedNearlineSize: 0\n" + \
          "GlueSARetentionPolicy: replica\n" + \
          "GlueSAAccessLatency: online\n" + \
          "GlueSAExpirationMode: neverExpire\n" + \
          "GlueSACapability: legacy\n" + \
          "GlueSAPolicyFileLifeTime: Permanent\n" + \
          "GlueSAType: permanent\n" + \
          "GlueSAStateAvailableSpace: "+str(free.KB())+"\n" + \
          "GlueSAStateUsedSpace: "+str(used.KB())+"\n" + \
          "GlueSAAccessControlBaseRule: "+vo+"\n" + \
          "GlueChunkKey: GlueSEUniqueID="+dpm_server.name+"\n" + \
          "GlueSchemaVersionMajor: 1\n" + \
          "GlueSchemaVersionMinor: 3\n" + \
          ""

#############
# Main code #
#############

parser = OptionParser()
parser.add_option('--children', dest='children', action='store_true', default=False, help="Also display pools's reservation")
parser.add_option('--domain', dest='dpm_domain', help='Set DPM domain (default based on /dpm contents)')
parser.add_option('--basedir', dest='dpns_basedir', help='Set DPNS base directory (default is "home")')
parser.add_option('-g', '--gip', dest='gip_mode', action='store_true', default=False, help='Use as a GIP provider and produce Glue LDIF output')
parser.add_option('--glue1', dest='glue_v1', action='store_true', default=True, help='In GIP mode, produce GLUE v1 information. Otherwise ignored.')
parser.add_option('--glue2', dest='glue_v2', action='store_true', default=False, help='In GIP mode, produce GLUE v2 information. Otherwise ignored.')
parser.add_option('--legacy', dest='legacy_mode', action='store_true', default=False,
                  help='Build a Glue 1.2 compatible SA in addition to standard ones (requires --gip)')
parser.add_option('-l', '--long', dest='details', action='store_true', default=False, help='Detailed information on pools and reservations')
parser.add_option('--no-glue1', dest='glue_v1', action='store_false', help='In GIP mode, negates --glue1, otherwise ignored.')
parser.add_option('--no-glue2', dest='glue_v2', action='store_false', help='In GIP mode, negates --glue2, otherwise ignored.')
parser.add_option('--parents', dest='parents', action='store_true', default=False, help="Also display reservation's parent")
parser.add_option('--pool', dest='pools', action='append', help='List of pools to display or -all')
parser.add_option('--protocols', dest='protocols', action='store_true', default=False,
                  help='Generate GlueSE and GlueAcessProtocol objects (requires --gip)')
parser.add_option('--reservation', dest='reservations', action='append', help='List of reservations to display or --all')
parser.add_option('--server','--headnode', dest='dpns_server', action='store', help='Host name of DPM head node')
parser.add_option('--si', dest='si_unit', action='store_true', default=False, help='Use SI units intead of power of 1024')
parser.add_option('--site', dest='site', action='store', help='Grid site name (required with --protocols)')
parser.add_option('--use-dmlite-conf', dest='dmlite', help='use dmlite with the specified configuration")')
parser.add_option('--subsite', dest='subsite', action='store', help='Grid subsite name if any (ignored without --protocols)')
parser.add_option('-v', '--debug', dest='verbose', action='count', help='Increase verbosity level for debugging (on stderr)')
parser.add_option('--version', dest='version', action='store_true', default=False, help='Display various information about this script')
options, args = parser.parse_args()

if options.verbose:
  verbosity = options.verbose
  
if options.version:
  debug (0,"Version %s written by %s" % (__version__,__author__))
  debug (0,__doc__)
  sys.exit(0)

# Check option conflicts

if options.gip_mode and (options.pools or options.reservations) and not options.verbose:
  debug(0,"ERROR: --pools and --reservations are not compatible with --gip, except if -v is specified")
  sys.exit(1)
  
if options.children:
  if not options.pools:
    debug(0,"ERROR: -children requires --pools")
    sys.exit(1)
  elif options.reservations:
    debug(0,"ERROR: --children conflicts with --reservations")
    sys.exit(1)
    
if options.parents:
  if not options.reservations:
    debug(0,"ERROR: --parents requires --reservations")
    sys.exit(1)
  elif options.pools:
    debug(0,"ERROR: --parents conflicts with --pools")
    sys.exit(1)
    
if options.legacy_mode and not options.gip_mode:
  debug(0,"WARNING: --legacy is ignored without --gip")

if options.protocols and not options.gip_mode:
  debug(0,"WARNING: --protocols is ignored without --gip")

if options.protocols and not options.site:
  debug(0,"ERROR: --protocols requires --site")
  sys.exit(1)

if options.site and not options.protocols:
  debug(1,"WARNING: --site is ignored without --protocols")

if options.subsite and not options.protocols:
  debug(1,"WARNING: --subsite is ignored without --protocols")

if options.details and options.gip_mode:
  # By default, don't display a warning as probably nobody will read it in gip mode...
  debug(1,"WARNING: --long is ignored with --gip")

if options.dmlite:
  dmlite_conf = options.dmlite

glue_versions = {}
if options.glue_v1:
  glue_versions['v1'] = True
if options.glue_v2:
  glue_versions['v2'] = True
  
# Initialize DPM environment
if options.dpns_server:
  dpns_server = options.dpns_server
else:
  dpns_server = os.getenv('DPNS_HOST')
if dpns_server:
  dpns_server = socket.getfqdn(dpns_server)
  if options.gip_mode and (dpns_server != socket.getfqdn()):
    debug(0,"ERROR: gip mode can only be used on DPM head node")
    sys.exit(1)
  elif not os.getenv('DPNS_HOST'):
    debug (1, "Defining DPNS server to %s" % dpns_server)
    os.environ['DPNS_HOST'] = dpns_server
else:
  dpns_server = socket.getfqdn()
  debug (2, "Defining DPNS server to %s" % dpns_server)
  os.environ['DPNS_HOST'] = dpns_server

if not os.getenv('DPM_HOST'):
  debug (2, "Defining DPM server to %s" % dpns_server)
  os.environ['DPM_HOST'] = dpns_server

if options.dpns_basedir:
  dpns_basedir = options.dpns_basedir
else:
  dpns_basedir = "home"

if options.dpm_domain:
  dpm_domain = options.dpm_domain
else:
  # Try go figure out domain name from namespace configuration
  dpm_domain = None
  dpm_root_name = "/dpm"
  dpm_root_dir = dpm.dpns_opendir(dpm_root_name)
  if not dpm_root_dir:
    debug (0, "ERROR: unable to open DPM root (%s)" % dpm_root_name)
    sys.exit(1)
  root_dir_entry = dpm.dpns_readdir(dpm_root_dir)
  while root_dir_entry:
    info = dpm.dpns_filestat()
    if dpm.dpns_stat(dpm_root_name+'/'+root_dir_entry.d_name+'/'+dpns_basedir,info) == 0:
      dpm_domain = root_dir_entry.d_name
      break
    root_dir_entry = dpm.dpns_readdir(dpm_root_dir)
  dpm.dpns_closedir(dpm_root_dir)
if dpm_domain:
  debug (1, "DPM domain = %s" % dpm_domain)
else:
  debug(0,"ERROR: unable to get DPM domain name. Check configuration or use --domain")
  sys.exit(1)
  
dpm_home_name = "/dpm/%s/%s" % (dpm_domain,dpns_basedir)
debug (1, "DPM root = %s" % dpm_home_name)

dpm_server = DPMServer(dpns_server,dpm_domain,
                       dpns_basedir,
                       site=options.site,
                       subsite=options.subsite,
                       legacy=options.legacy_mode,
                       vo_home_root=dpm_home_name)


# Use a dictionary instead of a list to allow fast access to pools and reservations.
pool_list = {}
reservation_list = {}


# Collect list of VOs configured on the server.
# This is done by listing all directories under DPM /homes (all directories are
# assumed to be a VO name).

dpm_home_dir = dpm.dpns_opendir(dpm_home_name)

# FIXME : in VO list obtained by listing DPM home, VO not enabled on the SE
# should be filtered out. Not clear how...
home_dir_entry = dpm.dpns_readdir(dpm_home_dir)
while home_dir_entry:
  dpm_server.addVO(home_dir_entry.d_name)
  home_dir_entry = dpm.dpns_readdir(dpm_home_dir)
dpm.dpns_closedir(dpm_home_dir)
if len(dpm_server.voList()) == 0:
  debug(1,"WARNING: no VO configured in DPM namespace")
else:
  debug (1, "\nList of supported VOs:")
  for vo in dpm_server.voList():
    debug (1, vo)

# Collect information on all DPM pools.
#
# Structure describing a pool is (from DPM 1.6.10 man page):
#       struct dpm_pool {
#            char      poolname[CA_MAXPOOLNAMELEN+1];
#            u_signed64     defsize;
#            int       gc_start_thresh;
#            int       gc_stop_thresh;
#            int       def_lifetime;
#            int       defpintime;
#            int       max_lifetime;
#            int       maxpintime;
#            char      fss_policy[CA_MAXPOLICYLEN+1];
#            char      gc_policy[CA_MAXPOLICYLEN+1];
#            char      mig_policy[CA_MAXPOLICYLEN+1];
#            char      rs_policy[CA_MAXPOLICYLEN+1];
#            int       nbgids
#            gid_t          *gids;         /* restrict the pool to given group(s) */
#            char      ret_policy;    /*  retention  policy:  R, O or C */
#            char       s_type;         /* space type: V, D or P */
#            u_signed64     capacity;
#            u_signed64     free;
#            struct dpm_fs  *elemp;
#            int       nbelem;
#            int       next_elem;     /* next pool element  to  be used */
#       };

debug(1,"\nCollecting information about DPM pools...")
nbpools, dpm_pools = dpm.dpm_getpools() #get pools
#use dmlite to get pool space info
if options.dmlite:
  dmlite = Dmlite(dmlite_conf)
if nbpools < 0:
  debug(0,"ERROR: failed to get information about configured pools");
  sys.exit(5)
else:
  if len(dpm_pools) == 0:
    debug(1,"No pool configured")
  for pool in dpm_pools:
    debug (1, "Pool = %s" % pool.poolname)
    all_vos = False
    if (pool.nbgids == 1) and (pool.gids[0] == 0):
      debug (1,'Pool %s authorized to all VOs' % pool.poolname)
      acbr = dpm_server.voList()
      all_vos = True
    else:
      acbr = []
      for gid in pool.gids:
        pool_fqan = ''
        i = 256
        while i > 0:
          pool_fqan += "\0"
          i -= 1
        i = 0
        status = dpm.dpns_getgrpbygid(gid,pool_fqan)
        fqan_len = pool_fqan.index("\0")
        acbr.append(pool_fqan[:fqan_len])
      for fqan in acbr:
        debug (1,'Pool %s authorized GID : %s' % (pool.poolname,fqan))
    capacity = None
    free = None
    space_type = None
    retention = None
    if options.dmlite:
	    capacity, free = dmlite.getPoolInfo(pool.poolname)
	    space_type = '-'
	    retention = "_"
    else:
	capacity = pool.capacity
	free = pool.free
	space_type = pool.s_type
	retention = pool.ret_policy

    pool_list[pool.poolname] = SA(pool.poolname,acbr,capacity,free,type=0,retention=retention,lifetime=pool.def_lifetime,
                                  max_lifetime=pool.max_lifetime,def_pintime=pool.defpintime,max_pintime=pool.maxpintime,
                                  gc_start_thresh=pool.gc_start_thresh,gc_stop_thresh=pool.gc_stop_thresh,gc_policy=pool.gc_policy,
                                  fss_policy=pool.fss_policy,rs_policy=pool.rs_policy,mig_policy=pool.mig_policy,
                                  space_type=space_type,server=dpm_server,allVos=all_vos)
    # Add all file systems belonging to the pool and define total/used/free accordingly
    pool_list[pool.poolname].addAllFS()


# Collect information on all reserved spaces.
# A statically reserved space is a reserved space with client DN = local server
# and an associated space token description (reserved spaces without a token description
# are ignored).
# Collected information includes associated space token description and ACL.
# Space reserved is substracted from total space of SA representing parent pool as GlueSAs are non overlapping
# chunks of physical space.
#
# Structure describing tokens is (from DPM 1.7.0 man page) :
#       struct dpm_space_metadata {
#            char      s_type;
#            char      s_token[CA_MAXDPMTOKENLEN+1];
#            uid_t          s_uid;
#            gid_t          s_gid;
#            char      ret_policy;
#            char      ac_latency;
#            char      u_token[256];
#            char      client_dn[256];
#            u_signed64     t_space;  /* Total space */
#            u_signed64     g_space;  /* Guaranteed space */
#            signed64  u_space;  /* Unused space */
#            char      poolname[CA_MAXPOOLNAMELEN+1];
#            time_t         a_lifetime;    /* Lifetime assigned */
#            time_t          r_lifetime;     /* Remaining lifetime */
#            int             nbgids;
#            gid_t           *gids;          /* restrict the space to given group(s) */
#       };
#
# Total size of space reservation is g_space. t_space only contains the value requested by the user and is
# not updated later (with updatespace). 

debug(1,"\nCollecting information about space reservations...")
tok_res, tokens = dpm.dpm_getspacetoken('') #get quotatokens
if tok_res < 0:
  debug(1,"No space reservation found");
else:
  md_res, tokens_md = dpm.dpm_getspacemd( list(tokens) ) #get quotatokens
  if md_res > -1:
    for md in tokens_md:
      if md.u_token == '' and options.gip_mode:
        debug (1, "Ignoring token %s: token description empty" % md.s_token)
      # Check for reservation being static is done by looking if DPNS host
      # name is present in md.client_dn. Not rock solid... may require improvement
      # like getting the current host DN from /etc/grid-security.
      elif ('client_dn' in dir(md)) and not re.search(dpns_server,md.client_dn):
        debug (1, "Ignoring token %s: not a static reservation (client_dn=%s)" % (md.u_token,md.client_dn))
      elif (md.s_gid == 0) and (md.s_uid != 0):
        debug (1, "Ignoring token %s: token restricted to a single user" % md.u_token)
      else:
        # If not using GIP mode, it is allowed to have a space reservation
        # without a token description. Use the token as the description in this case.
        if md.u_token == '':
          md.u_token = md.s_token
        all_vos = False
        debug (1, "Token = %s" % md.u_token)
        if (md.s_gid == 0):
          debug (1, "Token %s authorized to all VOs" % md.u_token)
          token_acbr = dpm_server.voList()#getVos
          all_vos = True
        else:
          token_acbr = []
          # DPM versions before 1.7 don't have md.gids but only one gid.
          # Unfortunatly md is not a tuple, so check against dir(md)
          if not 'gids' in dir(md):
            md.gids = [md.s_gid]
          for gid in md.gids:
            token_fqan = ''
            i = 256
            while i > 0:
              token_fqan += "\0"
              i -= 1
            status = dpm.dpns_getgrpbygid(gid, token_fqan)#get groups
            fqan_len = token_fqan.index("\0")
            token_acbr.append(token_fqan[:fqan_len])
          for fqan in token_acbr:
            debug (1, "Space %s authorized GID : %s" % (md.u_token,fqan))
        if md.poolname in pool_list:
          parent_pool = pool_list[md.poolname]
        else:
          debug(0, "FATAL: pool not found for space token %s (%s)" % (md.u_token,md.poolname))
          sys.exit(5)
        reservation_list[md.u_token] = SA(md.u_token,token_acbr,md.g_space,md.u_space,md.g_space,type=1,resid=md.s_token,
                                        space_type=md.s_type,linkedSA=parent_pool,lifetime=md.r_lifetime,retention=md.ret_policy,
                                        latency=md.ac_latency,server=dpm_server,allVos=all_vos)


# Display requested information if the appropriate format.
# For GIP mode, Create one GlueSA per DPM pool or space token and one GlueVOInfo for each VO with access to 
# the pool or ST. GlueSATotalxxxx is set based on actually usable space (readonly and disbled servers ignored
# for free space). GlueSAReservedxxxx is set based on reserved space size for space tokens and undefined for pools.
# GlueSA ACBR set to DPM ACL if defined else to the list of supported VOs.

# Pools

if options.parents:
  options.pools = []
  for reservation in options.reservations:
    for parent_name in reservation_list[reservation].linkedSA.keys():
      options.pools.append(parent_name)
    
header = True
if options.pools and not ('--all' in options.pools):
  options.pools.sort()
  for poolname in options.pools:
    if poolname in pool_list:
      pool_list[poolname].displaySA(options.gip_mode,header,options.details,options.legacy_mode,glue_versions)
      header = False
    else:
      print "ERROR: requested pool (%s) doesn't exist" % poolname
      sys.exit(3)
elif options.pools or not options.reservations:
  pool_names = pool_list.keys()
  pool_names.sort()
  for poolname in pool_names:
    pool_list[poolname].displaySA(options.gip_mode,header,options.details,options.legacy_mode,glue_versions)
    header = False

# Tokens
    
if options.children:
  options.reservations = []
  for pool in options.pools:
    for child_name in pool_list[pool].linkedSA.keys():
      options.reservations.append(child_name)

header = True
if options.reservations and not ('--all' in options.reservations):
  options.reservations.sort()
  for reservation in options.reservations:
    if reservation in reservation_list:
      reservation_list[reservation].displaySA(options.gip_mode,header,options.details,options.legacy_mode,glue_versions)
      header = False
    else:
      print "ERROR: requested reservation (%s) doesn't exist" % reservation
      sys.exit(3)
elif options.reservations or not options.pools:
  reservation_names = reservation_list.keys()
  reservation_names.sort()
  for reservation in reservation_names:
    reservation_list[reservation].displaySA(options.gip_mode,header,options.details,options.legacy_mode,glue_versions)
    header = False


# This option is only meaningful for GIP mode.
# Legcay mode is only for GLUE v1.
if options.gip_mode and options.glue_v1 and options.legacy_mode:
  debug(1,'Generating legacy GlueSA for each VO...')
  buildLegacySA(pool_list.values(),dpm_server.voList())


# Generate Glue information about SE if gip mode and --protocols
if options.gip_mode and options.protocols:
  debug(1,"Computing aggregated total/used space for %s..." % dpm_server.name)
  dpm_server.setSpace(pool_list.values())

  debug(1,"Generating GlueSE and GlueAccessProtocol for %s..." % dpm_server.name)
  dpm_server.glueSE(options.legacy_mode,glue_versions)
  dpm_server.glueAccess(glue_versions)
  dpm_server.glueControl(glue_versions)
  dpm_server.glueInterfaces(glue_versions)


