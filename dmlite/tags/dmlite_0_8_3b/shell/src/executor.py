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

# filter out all keys for which the value is None
def filterDict(params):
    newdict = {}
    for key, value in params.items():
        if value: newdict[key] = value
    return newdict

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

        cmd += ["--data", json.dumps(filterDict(data))]
        cmd += ["--request", self.verb]

        cmd.append(DomeTalker.build_url(self.uri, self.cmd))

        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        out = proc.communicate()[0]
        return out

class DomeExecutor(object):
    """Wrapper around DomeTalker"""
    def __init__(self, cert, key, capath, clientDN, clientAddress):
        self.creds = DomeCredentials(cert, key, capath, clientDN, clientAddress)

    def addFsToPool(self,url,fs,pool,server,status):
        talker = DomeTalker(self.creds, url, "POST", "dome_addfstopool")
        return talker.execute({"fs" : fs, "poolname" : pool, "server" : server,
                               "status" : status})

    def modifyFs(self,url,fs,pool,server,status):
        talker = DomeTalker(self.creds, url, "POST", "dome_modifyfs")
        return talker.execute({"fs" : fs, "poolname" : pool, "server" : server,
                               "status" : status})

    def rmFs(self,url,fs,server):
        talker = DomeTalker(self.creds, url, "POST", "dome_rmfs")
        return talker.execute({"fs" : fs, "server" : server})

    def getSpaceInfo(self,url):
        talker = DomeTalker(self.creds, url, "GET", "dome_getspaceinfo")
        return talker.execute({})

    def statPool(self,url, pool):
        talker = DomeTalker(self.creds, url, "GET", "dome_statpool")
        return talker.execute({"poolname" : pool})

    def getquotatoken(self,url,lfn, getparentdirs,getsubdirs):
        talker = DomeTalker(self.creds, url, "GET", "dome_getquotatoken")
        return talker.execute( { "path" : lfn, "getsubdirs" : getsubdirs, "getparentdirs": getparentdirs })

    def setquotatoken(self,url,lfn,pool,space,desc,groups):
        talker = DomeTalker(self.creds, url, "POST", "dome_setquotatoken")
        return talker.execute({ "path" : lfn, "poolname" : pool,
                          "quotaspace" : space, "description" : desc, "groups" : groups})

    def modquotatoken(self,url,s_token,lfn,pool,space,desc,groups):
        talker = DomeTalker(self.creds, url, "POST", "dome_modquotatoken")
        return talker.execute({ "tokenid" : s_token, "path" : lfn, "poolname" : pool,
                          "quotaspace" : space, "description" : desc, "groups" : groups})

    def getdirspaces(self,url,lfn):
        talker = DomeTalker(self.creds, url, "GET", "dome_getdirspaces")
        return talker.execute({ "path" : lfn})

    def delquotatoken(self,url,lfn,pool):
        talker = DomeTalker(self.creds, url, "POST", "dome_delquotatoken")
        return talker.execute({ "path" : lfn, "poolname" : pool})
