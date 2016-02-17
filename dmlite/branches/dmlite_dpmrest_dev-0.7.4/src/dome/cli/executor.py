from subprocess import Popen, PIPE, STDOUT
from pipes import quote
import os
import json

class DomeExecutor(object):
	'''It invokes the method via Davix CLI'''
	def __init__(self):
		self.baseArgs = ["davix-http", "-k","--cert", "/etc/grid-security/hostcert.pem","--key", "/etc/grid-security/hostkey.pem"]
	def putdone(self, lfn, clientDN, clientAddress,pfn,server):
        	args = self.baseArgs
                args.append("-X")
                args.append("POST")
		args.append(lfn)
		args = self.addClient(args,clientDN ,clientAddress)
		args.append("-H")
		args.append(quote('cmd: dome_putdone'))
		args.append("--data")
		data = {}
		data['pfn']=pfn
		data['server'] = server
		args.append(quote(json.dumps(data)))
		self.executeDavix(args)
	def put(self, lfn, clientDN, clientAddress):
		args = self.baseArgs
		args.append("-X")
		args.append("PUT")
		args.append(lfn)
		args = self.addClient(args,clientDN ,clientAddress)
		args.append("--data")
		args.append(quote("{}"))
		self.executeDavix(args)
	def executeDavix(self,args):
		args = (' ').join(args)
		print args
                proc = Popen(args, shell=True,stdout=PIPE)
		out = proc.communicate()[0]
                print out
	def addClient(self,args,clientDN,clientAddress):
                args.append("-H")
                args.append(quote("remoteclientdn: "+clientDN))
                args.append("-H")
                args.append(quote("remoteclientaddr: "+clientAddress))
		return args
