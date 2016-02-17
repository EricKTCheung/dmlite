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
    parser.add_option("--lfn", dest="lfn", help="the lfn", default = '')
    parser.add_option("--pfn", dest="pfn", help="the pfn", default = '')
    parser.add_option("--server", dest="server", help="the server", default = '')
    parser.add_option("--clientDN", dest="clientDN", help="the clientDN to use", default = '/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=furano/CN=644746/CN=Fabrizio Furano')
    parser.add_option("--clientAddr",  dest="clientAddr", help="the clientAddr to use", default = '43.54.56.6')
    (options, args) = parser.parse_args()
    if args:
        parser.error("Incorrect number of arguments")

    if options.execute:
	if options.execute == 'put':
		if not options.lfn:
			print "Please specify the LFN  via --lfn option"
			sys.exit(1)
		if not options.clientDN:
			print "Please specify the client DN via the --clientDN option"
			sys.exit(1)
		if not options.clientAddr:
        	        print "Please specify the client Address via the --clientAddr option"
			sys.exit(1)
		executor = DomeExecutor()
		executor.put(options.lfn, options.clientDN, options.clientAddr)
	elif options.execute == 'putdone':
		if not options.lfn:
                        print "Please specify the LFN  via --lfn option"
                        sys.exit(1)
		if not options.pfn:
                        print "Please specify the PFN  via --pfn option"
                        sys.exit(1)
		if not options.server:
                        print "Please specify the Server  via --server option"
                        sys.exit(1)
		executor = DomeExecutor()
		executor.putdone(options.lfn, options.clientDN, options.clientAddr,options.pfn,options.server)


# this is an executable module
if __name__ == '__main__':
	main()
