from subprocess import Popen, PIPE, STDOUT
from pipes import quote
import os
import json

class DomeExecutor(object):
	'''It invokes the method via Davix CLI'''
	def __init__(self,clientDN,clientAddress):
		self.clientDN = clientDN
		self.clientAddress = clientAddress
		self.baseArgs = ["davix-http", "-k","--cert", "/etc/grid-security/hostcert.pem","--key", "/etc/grid-security/hostkey.pem"]
	def putDone(self, url,pfn,size):
        	args = self.baseArgs
                args.append("-X POST")
		url = url + "/dome/command/dome_putdone"
		args.append(url)
		args = self.addClient(args)
		args.append("--data")
		data = {}
		data['pfn']= pfn
		data['size'] = size
		args.append(quote(json.dumps(data)))
		self.executeDavix(args)
	def put(self,url,lfn, additionalreplica='true', pool='pool01',host='domedisk-trunk.cern.ch',fs='/srv/dpm/02'):
		args = self.baseArgs
		args.append("-X POST")
		url = url + "/dome/command/dome_put"
		args.append(url)
		args = self.addClient(args)
		args.append("--data")
                data = {}
		data['lfn']= lfn
		data['additionalreplica']= additionalreplica
		data['pool'] = pool
		data['host'] = host
		data['fs'] = fs
 		args.append(quote(json.dumps(data)))
		self.executeDavix(args)
	def get(self,url,lfn,server,pfn,filesystem):
		args = self.baseArgs
                args.append("-X GET")
		url = url +"/dome/command/dome_get"
		args.append(url)
		args = self.addClient(args)
		args.append("--data")
		data = {}
		data['lfn'] = lfn
		data['server'] = server
		data['pfn'] = pfn
		data['filesystem'] = filesystem
                args.append(quote(json.dumps(data)))
                self.executeDavix(args)
	def pfnrm(self,url,pfn):
		args = self.baseArgs
                args.append("-X POST")
		args.append(url+"/")
		args = self.addClient(args)
		args.append("-H")
                args.append(quote('cmd: dome_pfnrm'))
		args.append("--data")
                data = {}
                data['pfn']=pfn
		args.append(quote(json.dumps(data)))
                self.executeDavix(args)
	def getSpaceInfo(self,url):
		args = self.baseArgs
		args.append("-X GET")
		args.append(url+ "/dome/command/dome_getspaceinfo")
		args = self.addClient(args)
		args.append("--data")
                args.append(quote("{}"))
                self.executeDavix(args)
	def statPool(self,url, pool):
		args = self.baseArgs
                args.append("-X GET")
                args.append(url+"/dome")
                args = self.addClient(args)
                args.append("-H")
                args.append(quote('cmd: dome_statpool'))
                args.append("--data")
		data = {}
		data['poolname']=pool
                args.append(quote(json.dumps(data)))
                self.executeDavix(args)
	def getquotatoken(self,url,lfn):
		args = self.baseArgs
		args.append("-X GET")
		url = url + "/dome/command/dome_getquotatoken"
                args.append(url)
		args = self.addClient(args)
                args.append("--data")
		data = {}
		data['path'] = lfn
		data['getsubdirs'] = True
                args.append(quote(json.dumps(data)))
                self.executeDavix(args)
	def setquotatoken(self,url,lfn,pool, space,desc):
                args = self.baseArgs
	        args.append("-X POST")
 		url = url + "/dome/command/dome_setquotatoken"
		args.append(url)
                args = self.addClient(args)
	        args.append("--data")
                data = {}
                data['path']=lfn
                data['poolname'] = pool
		data['quotaspace'] = space
		data['description']=desc
                args.append(quote(json.dumps(data)))
                self.executeDavix(args)
	def getdirspaces(self,url,lfn):
		args = self.baseArgs
                args.append("-X GET")
                url = url + "/dome/command/dome_getdirspaces"
                args.append(url)
                args = self.addClient(args)
                args.append("--data")
                data = {}
                data['path'] = lfn
                args.append(quote(json.dumps(data)))
                self.executeDavix(args)

	def delquotatoken(self,url,lfn,pool):
		args = self.baseArgs
                args.append("-X POST")
		url = url+"/dome/command/dome_delquotatoken"
		args.append(url)
		args = self.addClient(args)
		args.append("--data")
		data = {}
		data['path']=lfn
		data['poolname'] = pool
		args.append(quote(json.dumps(data)))
		self.executeDavix(args)
	def delreplica(self,url,pfn,server):
		args = self.baseArgs
		args.append("-X POST")
		args.append(url+"/")
		args = self.addClient(args)
		args.append("-H")
                args.append(quote('cmd: dome_delreplica'))
		args.append("--data")
                data = {}
                data['server']=server
                data['pfn'] = pfn
		args.append(quote(json.dumps(data)))
                self.executeDavix(args)
	def executeDavix(self,args):
		args = (' ').join(args)
		print args
                proc = Popen(args, shell=True,stdout=PIPE)
		out = proc.communicate()[0]
                print out
	def addClient(self,args):
                args.append("-H")
                args.append(quote("remoteclientdn: "+self.clientDN))
                args.append("-H")
                args.append(quote("remoteclientaddr: "+self.clientAddress))
		return args
