#!/usr/bin/env python
# dome.py
"""
This file implements the Dome CLI.
The CLI provides an easy access to many features of the Dome Service
"""
import os
import ConfigParser
import sys
from optparse import OptionParser
from executor import DomeExecutor

def main():
    # parse options
    parser = OptionParser("%prog [options]")
    parser.add_option("-e", "--execute", dest="execute", help="execute the given command", default = False)
    parser.add_option("-u", "--url", dest="url", help="the base url to contact", default = '')
    parser.add_option("--lfn", dest="lfn", help="the lfn", default = '')
    parser.add_option("--pfn", dest="pfn", help="the pfn", default = '')
    parser.add_option("--pool", dest="pool", help="the pool", default = '')
    parser.add_option("--space", dest="space", help="the space", default = '')
    parser.add_option("--desc", dest="desc", help="the quota token description", default = '')
    parser.add_option("--server", dest="server", help="the server", default = '')
    parser.add_option("--fs", dest="fs", help="the filtesystem", default = '')
    parser.add_option("--size", dest="size", help="the file size", default = '')
    parser.add_option("--clientDN", dest="clientDN", help="the clientDN to use", default = '/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=furano/CN=644746/CN=Fabrizio Furano')
    parser.add_option("--clientAddr",  dest="clientAddr", help="the clientAddr to use", default = '43.54.56.6')
    (options, args) = parser.parse_args()
    if args:
        parser.error("Incorrect number of arguments")

    if options.execute:
        if not options.url:
                print "Please specify the url  via --url option"
                sys.exit(1)
    executor = DomeExecutor("/etc/grid-security/dpmmgr/dpmcert.pem", "/etc/grid-security/dpmmgr/dpmkey.pem",
                            "/etc/grid-security/certificates", options.clientDN, options.clientAddr)
    # executor = DomeExecutor("/etc/grid-security/hostcert.pem", "/etc/grid-security/hostkey.pem",
                            # "/etc/grid-security/certificates", options.clientDN, options.clientAddr)

    if options.execute == 'get':
        if not options.lfn:
            print "Please specify the LFN  via --lfn option"
            sys.exit(1)
        if not options.pfn:
            print "Please specify the PFN  via --pfn option"
            sys.exit(1)
        if not options.server:
            print "Please specify the Server  via --server option"
            sys.exit(1)
        if not options.fs:
            print "Please specify the Filesystem  via --fs option"
            sys.exit(1)
        executor.get(options.url,options.lfn, options.pfn,options.server,options.fs)
    if options.execute == 'put':
        if not options.lfn:
            print "Please specify the LFN  via --lfn option"
            sys.exit(1)
        executor.put(options.url,options.lfn)
    elif options.execute == 'putdone':
        if not options.pfn:
            print "Please specify the PFN  via --pfn option"
            sys.exit(1)
        if not options.size:
            print "Please specify the Server  via --size option"
            sys.exit(1)
        executor.putDone(options.url, options.pfn,options.size)
    elif options.execute == 'getspaceinfo':
        executor.getSpaceInfo(options.url)
    elif options.execute == 'statpool':
        if not options.pool:
            print "Please specify the Pool to stat  via --pool option"
            sys.exit(1)
        executor.statPool(options.url,options.pool)
    elif options.execute == 'getquotatoken':
        if not options.lfn:
            print "Please specify the LFN  via --lfn option"
            sys.exit(1)
        executor.getquotatoken(options.url,options.lfn)
    elif options.execute == 'setquotatoken':
        if not options.lfn:
            print "Please specify the LFN  via --lfn option"
            sys.exit(1)
        if not options.pool:
            print "Please specify the Pool to set the quota  token  via --pool option"
            sys.exit(1)
        if not options.space:
            print "Please specify the Space for the quota token  via --space option"
            sys.exit(1)
        if not options.desc:
            print "Please specify the Space for the quota token description  via --desc option"
            sys.exit(1)
        executor.setquotatoken(options.url,options.lfn,options.pool, options.space,options.desc)
    elif options.execute == 'delquotatoken':
        if not options.lfn:
            print "Please specify the LFN  via --lfn option"
            sys.exit(1)
        if not options.pool:
            print "Please specify the Pool to set the quota  token  via --pool option"
            sys.exit(1)
        executor.delquotatoken(options.url,options.lfn,options.pool)
    elif options.execute == 'getdirspaces':
        if not options.lfn:
            print "Please specify the LFN  via --lfn option"
            sys.exit(1)
        executor.getdirspaces(options.url,options.lfn)
    elif options.execute == "pfnrm":
        if not options.pfn:
            print "Please specify the PFN  via --pfn option"
            sys.exit(1)
        executor.pfnrm(options.url,options.pfn)
    elif options.execute == "delreplica":
        if not options.pfn:
            print "Please specify the PFN  via --pfn option"
            sys.exit(1)
        if not options.server:
            print "Please specify the Server  via --server option"
            sys.exit(1)
        executor.delreplica(options.url,options.pfn,options.server)

# this is an executable module
if __name__ == '__main__':
        main()
