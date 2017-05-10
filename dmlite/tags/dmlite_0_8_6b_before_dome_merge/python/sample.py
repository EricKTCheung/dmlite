#!/usr/bin/env python

import pydmlite

def test():
    configFile =  '/etc/dmlite.conf'
    try:
      pluginManager = pydmlite.PluginManager()
      pluginManager.loadConfiguration(configFile)
    except Exception, e:
      print e
      return 

    try:
      securityContext = pydmlite.SecurityContext()
      group = pydmlite.GroupInfo()
      group.name = "root"
      group.setUnsigned("gid", 0)
      securityContext.user.setUnsigned("uid", 0)
      securityContext.groups.append(group)
    except Exception, e:
      print e
      return 

    try:
      stackInstance = pydmlite.StackInstance(pluginManager)
      stackInstance.setSecurityContext(securityContext)
    except Exception, e:
      print e
      return

    try:
      catalog = stackInstance.getCatalog()
    except Exception, e:
      print e
      return
    try:
      f = catalog.extendedStat("/", True)
      print f.stat.st_ino
      print f.stat.st_size
    except Exception, e:
      print e
      return

test()
