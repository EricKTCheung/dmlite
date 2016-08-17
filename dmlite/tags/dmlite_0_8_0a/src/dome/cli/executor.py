from subprocess import Popen, PIPE, STDOUT
from pipes import quote
import os
import json
import subprocess

class DomeCredentials(object):
    """Stores all credentials needed for a dome request"""

    def __init__(self, cert, key, capath, clientDN, clientAddress):
        self.cert = cert
        self.key = key
        self.capath = capath
        self.clientDN = clientDN
        self.clientAddress = clientAddress

class DomeTalker(object):
    """Issues requests to Dome"""

    @staticmethod
    def build_url(url, command):
        while url.endswith("/"):
            url = url[:-1]
        return "{0}/command/{1}".format(url, command)

    def __init__(self, creds, uri, verb, cmd):
        self.creds = creds
        self.uri = uri
        self.verb = verb
        self.cmd = cmd
    def execute(self, data):
        cmd = ["davix-http"]
        if self.creds.cert:
            cmd += ["--cert", self.creds.cert]
        if self.creds.key:
            cmd += ["--key", self.creds.key]
        if self.creds.clientDN:
            cmd += ["--header", "remoteclientdn: {0}".format(self.creds.clientDN)]
        if self.creds.clientAddress:
            cmd += ["--header", "remoteclientaddr: {0}".format(self.creds.clientAddress)]
        if self.creds.capath:
            cmd += ["--capath", self.creds.capath]

        cmd += ["--data", json.dumps(data)]
        cmd += ["--request", self.verb]

        cmd.append(DomeTalker.build_url(self.uri, self.cmd))

        print("'" + "' '".join(cmd) + "'")
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        out = proc.communicate()[0]
        print(out)

class DomeExecutor(object):
    """Wrapper around DomeTalker"""
    def __init__(self, cert, key, capath, clientDN, clientAddress):
        self.creds = DomeCredentials(cert, key, capath, clientDN, clientAddress)

    def putDone(self, url,pfn,size):
        talker = DomeTalker(self.creds, url, "POST", "dome_putdone")
        talker.execute({"pfn" : pfn, "size" : size})

    def put(self,url,lfn, additionalreplica='true', pool='pool01',host='domedisk-trunk.cern.ch',fs='/srv/dpm/02'):
        talker = DomeTalker(self.creds, url, "POST", "dome_put")
        talker.execute({ "lfn" : lfn, "additionalreplica" : additionalreplica,
                         "pool" : pool, "host" : host, "fs" : fs})

    def get(self,url,lfn,server,pfn,filesystem):
        talker = DomeTalker(self.creds, url, "GET", "dome_get")
        talker.execute()

    def pfnrm(self,url,pfn):
        talker = DomeTalker(self.creds, url, "POST", "dome_pfnrm")
        talker.execute({"pfn" : pfn})

    def getSpaceInfo(self,url):
        talker = DomeTalker(self.creds, url, "GET", "dome_getspaceinfo")
        talker.execute({})

    def statPool(self,url, pool):
        talker = DomeTalker(self.creds, url, "GET", "dome_statpool")
        talker.execute({"poolname" : pool})

    def getquotatoken(self,url,lfn):
        talker = DomeTalker(self.creds, url, "GET", "dome_getquotatoken")
        talker.execute( { "path" : lfn, "getsubdirs" : True })

    def setquotatoken(self,url,lfn,pool, space,desc):
        talker = DomeTalker(self.creds, url, "POST", "dome_setquotatoken")
        talker.execute({ "path" : lfn, "poolname" : pool,
                          "quotaspace" : space, "description" : desc })

    def getdirspaces(self,url,lfn):
        talker = DomeTalker(self.creds, url, "GET", "dome_getdirspaces")
        talker.execute({ "path" : lfn})

    def delquotatoken(self,url,lfn,pool):
        talker = DomeTalker(self.creds, url, "POST", "dome_delquotatoken")
        talker.execute({ "path" : lfn, "poolname" : pool})

    def delreplica(self,url,pfn,server):
        talker = DomeTalker(self.creds, url, "POST", "dome_delreplica")
        talker.execute({ "server" : server, "pfn" : pfn})
