# interpreter.py

import pydmlite
import shlex
import os
import pipes
import inspect
import sys
import re
import time
import dateutil.parser
import pycurl
import urllib
from dbutils import DPMDB
import threading
import Queue
import signal
import socket
from executor import DomeExecutor
import json
import pprint
import StringIO

try:
    import dpm2
except:
    pass

class DMLiteInterpreter:
  """
      A class taking commands as strings and passing them to DMLite via pydmlite.
  """

  def __init__(self, outputFunction, ConfigFile, quietMode = False):
    self.defaultConfigurationFile = ConfigFile
    self.write = outputFunction
    self.quietMode = quietMode
    self.lastCompleted = 0
    self.lastCompletedState = 0
    self.executor = DomeExecutor("/etc/grid-security/dpmmgr/dpmcert.pem", "/etc/grid-security/dpmmgr/dpmkey.pem",
                            "/etc/grid-security/certificates", "", "")
    self.domeheadurl = "https://" + socket.getfqdn() + "/domehead"
    global replicaQueue
    global replicaQueueLock
    global drainErrors


    # collect all available commands into commands-array
    self.commands = []
    for (cname, cobj) in inspect.getmembers(sys.modules[__name__]):
      if inspect.isclass(cobj) and cname.endswith('Command') and cname != 'ShellCommand':
        self.commands.append(cobj(self))

    # execute init command
    self.execute('init')

  def execute(self, inputline):
    """Execute the given command.
    Return value: (message, error, exit shell)
    Last return value can be found in self.return"""

    try:
      cmdline = shlex.split(inputline, True)
    except Exception, e:
      return self.error('Parsing error.')

    if not cmdline:

      # empty command
      return self.ok()

    # search for command and execute it
    for c in self.commands:
      if c.name == cmdline[0]:
        return c.execute(cmdline[1:])
        break
    else:
      # other, unknown command
      return self.error('Unknown command: ' + cmdline[0])

  def ok(self, msg = ''):
    """Writes a message to the output. Returns False on any previous errors."""
    if msg != '':
      self.write(msg + '\n')
    self.answered = True
    return not self.failed

  def error(self, error, msg = ''):
    """Writes an error message to the output. Returns False."""
    self.ok(msg)
    self.write(self.doIndentation(error, 'ERROR: ', '       ') + '\n')
    self.failed = True
    return not self.failed

  def doIndentation(self, msg, firstLine, indentation):
    exp = re.compile('\\n[^\\S\\r\\n]*') # selects all
    return exp.sub("\n" + indentation, firstLine + msg.lstrip())

  def exitShell(self, msg = ''):
    """Set the shell exit flag. Returns False on any previous errors."""
    self.ok(msg)
    self.exit = True
    return not self.failed

  def completer(self, text, state):
    """ Complete the given start of a command line."""

    if (self.lastCompleted != text) or (self.lastCompletedState > state):
      self.completionOptions = []
      self.lastCompleted = text
      self.lastCompletedState = state
      # check all commands if the provide completion options
      for c in self.commands:
        try:
          coptions = c.completer(text)
          self.completionOptions.extend(coptions)
        except Exception, e: # look out for errors!
          print e.__str__()

    # return the correct option
    try:
      return self.completionOptions[state]
    except IndexError:
      return None

  def prettySize(self, size=0):
      size = int(size)
      if size < 1024:
        prettySize = str(size) + 'B '
      elif size < 1024*1024:
        prettySize = '%.2fkB' % (size / 1024.)
      elif size < 1024*1024*1024:
        prettySize = '%.2fMB' % (size / 1024. / 1024.)
      elif size < 1024*1024*1024*1024:
        prettySize = '%.2fGB' % (size / 1024. / 1024. / 1024.)
      elif size < 1024*1024*1024*1024*1024:
        prettySize = '%.2fTB' % (size / 1024. / 1024. / 1024. / 1024.)
      else:
        prettySize = '%.2fPB' % (size / 1024. / 1024. / 1024. / 1024. / 1024.)
      return prettySize

  def prettyInputSize(self, prettysize):
      if 'PB' in prettysize:
        prettysize = prettysize.replace('PB','')
        size = int(prettysize) * 1024 * 1024 * 1024 * 1024 * 1024
      elif 'TB' in prettysize:
        prettysize = prettysize.replace('TB','')
        size = int(prettysize) * 1024 * 1024 * 1024 * 1024
      elif 'GB' in prettysize:
        prettysize = prettysize.replace('GB','')
        size = int(prettysize) * 1024 * 1024 * 1024
      elif 'MB' in prettysize:
        prettysize = prettysize.replace('MB','')
        size = int(prettysize) * 1024 * 1024
      elif 'KB' in prettysize:
        prettysize = prettysize.replace('kB','')
        size = int(prettysize) * 1024
      else:
        size = int(prettysize)
      return size

  def listDirectory(self, directory, readComments = False):
    # list information about files in a directory
    try:
      hDir = self.catalog.openDir(directory)
    except:
      return -1

    flist = []

    while True:
      finfo = {}
      try:
        f = self.catalog.readDirx(hDir)
        if f.stat.isDir():
          finfo['prettySize'] = '(dir)'
          finfo['size'] = 0
          finfo['isDir'] = True
        else:
          finfo['prettySize'] = self.prettySize(f.stat.st_size)
          finfo['size'] = f.stat.st_size
          finfo['isDir'] = False
        finfo['name'] = f.name
        finfo['isLnk'] = f.stat.isLnk()
        if finfo['isLnk']:
          finfo['link'] = self.catalog.readLink(os.path.join(directory, f.name))
        else:
          finfo['link'] = ''

        if readComments:
          try:
            finfo['comment'] = self.catalog.getComment(os.path.join(directory, f.name))
          except Exception, e:
            finfo['comment'] = ''

        flist.append(finfo)
      except:
        break

    self.catalog.closeDir(hDir)
    return flist


class ShellCommand:
  """
  An abstract class for deriving classes for supported shell commands.
  """
  drainProcess = None

  def signal_handler(self,signal, frame):
     if self.drainProcess:
               self.drainProcess.stopThreads()
     sys.exit(0)

  # functions stubs to override:
  def _init(self):
    """Override this to set e.g. self.parameters."""
    pass

  def _execute(self, given):
    """Override this to execute the command with the parameters in the given array."""
    return self.ok('Execute stub for ' + self.name + '...')


  def __init__(self, interpreter):
    """Initialisation for the command class. Do not override!"""
    self.interpreter = interpreter

    # self.name contains the name of the command
    # this is automatically extracted from the class name
    self.name = ((self.__class__.__name__)[:-7]).lower()

    # self.description contains a short desc of the command
    # this is automatically extracted from the class __doc__
    self.description = self.__doc__
    if self.description.find('\n') > 0:
      self.shortDescription = self.description[0:self.description.find('\n')]
    else:
      self.shortDescription = self.description

    # self.parameters contains a list of parameters in the syntax
    # *Tparameter name
    # where * is added for optional parameters and T defines the
    # type of the parameter (for automatic checks and completion):
    #   f local file/directory
    #   d DMLite file/directory
    #   c DMLite shell command
    #   o one of the options in the format ofieldname:option1:option2:option3
    #   ? other, no checks done
    # Note: use uppercase letter for a check if file/command/.. exists
    self.parameters = []

    self._init()

  def help(self):
    """Returns a little help text with the description of the command."""
    return ' ' + self.name + (' ' * (15 - len(self.name))) + self.shortDescription

  def syntax(self):
    """Returns the syntax description of the command."""
    return self.name + ' ' + ' '.join(self.prettyParameter(p) for p in self.parameters)

  def moreHelp(self):
    """Returns the syntax description of the command and a help text."""
    return self.syntax() + '\n' + self.interpreter.doIndentation(self.description, '  ', '  ')

  def prettyParameter(self, parameter):
    """Return the human readable format of a parameter"""
    if parameter.startswith('*'):
      return '[ ' + self.prettyParameter(parameter[1:]) + ' ]'
    if parameter.lower().startswith('o'):
      parameter = parameter.split(':')
      del parameter[0]
      return '/'.join(p for p in parameter)
    return '<' + parameter[1:] + '>'

  def prettySize(self, size=0):
      return self.interpreter.prettySize(size)

  def checkSyntax(self, given):
    if len(given) > len(self.parameters):
      return self.syntaxError()
    for i in range(len(self.parameters)):
      if self.parameters[i].startswith('*'):
        ptype = self.parameters[i][1:2]
        if i > len(given)-1:
          continue
      elif i > len(given)-1:
        return self.syntaxError()
      else:
        ptype = self.parameters[i][0:1]

      # check for type and check if correct type
      if ptype == 'C':
        if given[i] not in list(c.name for c in self.interpreter.commands):
          return self.syntaxError('Unknown command "' + given[i] + '".')
      elif ptype == 'F':
        if not os.path.exists(given[i]):
          return self.syntaxError('File "' + given[i] + '" does not exist.')
      elif ptype == 'D':
        # check if file exists in DMLite
        try:
          f = self.interpreter.catalog.extendedStat(given[i], False)
        except Exception, e:
          return self.syntaxError('File "' + given[i] + '" does not exist.')
      elif ptype == 't' or ptype == 'T':
        # check if a valid date/time is given
        try:
          if given[i].lower() == 'now':
            given[i] = time.localtime()
          else:
            given[i] = dateutil.parser.parse(given[i]).timetuple()
          time.mktime(given[i])
        except Exception, e:
          return self.syntaxError('Date/time expression expected.')
      elif ptype == 'G':
        # check if a valid group name or group ID is given
        try:
          groups = (g.name for g in self.interpreter.authn.getGroups())
          group = given[i]
          try:
            gid = int(group)
            groupID = pydmlite.boost_any()
            groupID.setUnsigned(gid)
            group = self.interpreter.authn.getGroup('gid',groupID).name
          except:
            pass
          if group not in groups:
            return self.syntaxError('Group name or group ID expected.')
        except Exception, e:
          pass
      elif ptype == 'U':
        # check if a valid user name or user ID is given
        try:
          users = (g.name for g in self.interpreter.authn.getUsers())
          user = given[i]
          try:
            uid = int(user)
            userID = pydmlite.boost_any()
            userID.setUnsigned(uid)
            user = self.interpreter.authn.getUser('uid',userID).name
          except:
            pass
          if user not in users:
            return self.syntaxError('User name or user ID expected.')
        except Exception, e:
          pass
      elif ptype == 'O':
        # list of possible options
        pOptions = self.parameters[i].split(':')[1:]
        if given[i] not in pOptions:
          return self.syntaxError('Expected one of the following options: ' + ', '.join(pOptions))

    return self.ok()

  def execute(self, given):
    """Executes the current command with the parameters in the given array."""
    signal.signal(signal.SIGINT, self.signal_handler)

    # reset result flags
    self.interpreter.answered = False
    self.interpreter.failed = False
    self.interpreter.exit = False

    # check syntax first
    if not self.checkSyntax(given): # syntax error occcurred!
      return
    self._execute(given)

  def completer(self, start):
    """Return a list of possible tab-completions for the string start."""

    # This has known issues. In complicated scenarios with " and spaces in
    # the commandline, this might fail, due to the complicated readline
    # behaviour. Also, it does only support completion at the end.

    if self.name.startswith(start):
      # complete the command
      if len(self.parameters) == 0:
        return [self.name]
      else:
        return [self.name + ' ']

    elif start.startswith(self.name):
      # complete parameters, analyse the already given parameters
      try:
        given = shlex.split(start + 'x', True) # add an x...
        quoted = False
      except Exception, e: # parsing failed
        # maybe because the user used an opening ' or " for the
        # current parameter?
        try:
          given = shlex.split(start+'"', True)
          quoted = '"'
        except Exception, e:
          try:
            given = shlex.split(start+"'", True)
            quoted = "'"
          except Exception, e:
            return [] # all parsing attempts failed

      if len(given)-1 > len(self.parameters):
        # too many parameters given already
        return []
      else:
        # extract current parameter type
        ptype = self.parameters[len(given)-2]
        if ptype.startswith('*'): # only plain parameter type in lower-case
          ptype = ptype[1:2].lower()
        else:
          ptype = ptype[0:1].lower()

        # and the currently given parameter
        if not quoted:
          lastgiven = given[len(given)-1][:-1] # remove 'x' at the end
        else:
          lastgiven = given[len(given)-1]

        # if the parameter contained slashes, we only need to return
        # the part after the last slash, because it is recognized as
        # a delimiter
        lastslashcut = lastgiven.rfind('/') + 1

        # workaround for readline bug: escaped whitespaces are also
        # recognized used as delimiters. Best we can do is display
        # only the part after the escaped whitespace...
        lastspacecut = lastgiven.rfind(' ') + 1
        if lastspacecut > lastslashcut:
          lastslashcut = lastspacecut


        if ptype == 'c': # command
          l = list(c.name for c in self.interpreter.commands if c.name.startswith(lastgiven))
        elif ptype == 'f': # file or folder
          if not lastgiven.startswith('/'):
            lastgiven = './' + lastgiven
          gfolder, gfilestart = os.path.split(lastgiven)
          groot, gdirs, gfiles = os.walk(gfolder).next()
          gfiles = gfiles + list((d + '/') for d in gdirs)
          l = list(f for f in gfiles if f.startswith(gfilestart))
        elif ptype == 'd': # dmlite file or folder
          gfolder, lastgiven = os.path.split(lastgiven)
          if gfolder == '':
            gfiles = self.interpreter.listDirectory(self.interpreter.catalog.getWorkingDir())
          else:
            gfiles = self.interpreter.listDirectory(gfolder)
          if gfiles == -1: # listing failed
            return []
          l = list( (os.path.join(gfolder, f['name']) + ('','/')[f['isDir']])[lastslashcut:] for f in gfiles if f['name'].startswith(lastgiven))
        elif ptype == 'g': # dmlite group
          l = list(g.name[lastslashcut:] for g in self.interpreter.authn.getGroups() if g.name.startswith(lastgiven))
        elif ptype == 'u': # dmlite user
          l = list(u.name[lastslashcut:] for u in self.interpreter.authn.getUsers() if u.name.startswith(lastgiven))
        elif ptype == 'o': # one of the given options
          pOptions = self.parameters[len(given)-2].split(':')[1:]
          l = list(option for option in pOptions if option.startswith(lastgiven))
        else:
          return []

        if not quoted:
          exp = re.compile('([\\"\' ])') # we still have to escape the characters \,",' and space
        else:
          exp = re.compile('([\\"\'])') # do not escape space in a quoted string
        l = list(exp.sub(r'\\\1', option) for option in l)

        if quoted and len(l) == 1:
          if lastslashcut > 0:
            return [l[0] + quoted] # close a quotation if no other possibility
          else:
            return [quoted + l[0] + quoted] # remeber to open and close quote
        return l
    else:
      # no auto completions from this command
      return []

  def ok(self, msg = ''):
    return self.interpreter.ok(msg)

  def error(self, error, msg = ''):
    return self.interpreter.error(error, msg)

  def exitShell(self, msg = ''):
    return self.interpreter.exitShell(msg)

  def syntaxError(self, msg = 'Bad syntax.' ):
    """Writes a syntax error message to the output. Returns False."""
    return self.error(msg + '\nExpected syntax is: ' + self.syntax())


### Specific commands to the DMLite Shell ###

class InitCommand(ShellCommand):
  """Initialise the DMLite shell with a given configuration file."""
  def _init(self):
    self.parameters = ['*OLogLevel=0:0:1:2:3:4', '*Fconfiguration_file=' + self.interpreter.defaultConfigurationFile]

  def _execute(self, given):
    if len(given) == 0:
        configFile =  self.interpreter.defaultConfigurationFile
        log = "0"
    elif len(given) == 1:
        configFile =  self.interpreter.defaultConfigurationFile
        log = given[0]
    else:
        log = given[0]
        configFile =  given[1]
    conf = open(configFile, 'r')
    confTmp = open("/tmp/dmlite.conf", 'w')
    for line in conf:
        if line.startswith("LogLevel"):
            confTmp.write("LogLevel %s\n" % log)
        else:
            confTmp.write(line)
    conf.close()
    confTmp.close()
    self.interpreter.configurationFile = "/tmp/dmlite.conf"

    # check the existance of the pydmlite library
    try:
      self.interpreter.API_VERSION = pydmlite.API_VERSION
      if not self.interpreter.quietMode:
        self.ok('DMLite shell v0.8.3 (using DMLite API v' + str(self.interpreter.API_VERSION) + ')')
    except Exception, e:
      return self.error('Could not import the Python module pydmlite.\nThus, no bindings for the DMLite library are available.')

    try:
      if 'pluginManager' not in dir(self.interpreter):
        self.interpreter.pluginManager = pydmlite.PluginManager()
      self.interpreter.pluginManager.loadConfiguration(self.interpreter.configurationFile)
    except Exception, e:
      return self.error('Could not initialise PluginManager with file "' + self.interpreter.configurationFile + '".\n' + e.__str__())

    try:
      self.interpreter.securityContext = pydmlite.SecurityContext()
      group = pydmlite.GroupInfo()
      group.name = "root"
      group.setUnsigned("gid", 0)
      self.interpreter.securityContext.user.setUnsigned("uid", 0)
      self.interpreter.securityContext.groups.append(group)
    except Exception, e:
      return self.error('Could not initialise root SecurityContext.\n' + e.__str__())

    try:
      self.interpreter.stackInstance = pydmlite.StackInstance(self.interpreter.pluginManager)
      self.interpreter.stackInstance.setSecurityContext(self.interpreter.securityContext)
    except Exception, e:
      return self.error('Could not initialise a StackInstance.\n' + e.__str__())

    try:
      self.interpreter.catalog = self.interpreter.stackInstance.getCatalog()
      self.interpreter.catalog.changeDir('')
    except Exception, e:
      return self.error('Could not initialise the file catalog.\n' + e.__str__())

    try:
      self.interpreter.authn = self.interpreter.stackInstance.getAuthn()
    except Exception, e:
      self.interpreter.authn = None
      if not self.interpreter.quietMode:
        self.ok('\nWARNING: Could not initialise the authentication interface. The functions like userinfo, groupinfo, entergrpmap, rmusrmap, ... will not work.\n' + e.__str__() + '\n')

    try:
      self.interpreter.poolManager = self.interpreter.stackInstance.getPoolManager()
    except Exception, e:
      self.interpreter.poolManager = None
      if not self.interpreter.quietMode:
        self.ok('\nWARNING: Could not initialise the pool manager. The functions like pools, modifypool, addpool, qryconf, ... will not work.\n' + e.__str__() + '\n')

    if not self.interpreter.quietMode:
      self.ok('Using configuration "' + configFile + '" as root.')

    if 'dpm2' not in sys.modules:
        self.ok("DPM-python has not been found. Please do 'yum install dpm-python' or the following commands will not work properly: drainfs, drainpool, drainserver .")

    if log != '0':
        self.ok("Log Level set to %s." % log)

    return self.ok()


class HelpCommand(ShellCommand):
  """Print a help text or descriptions for single commands."""
  def _init(self):
    self.parameters = ['*Ccommand']

  def _execute(self, given):
    if len(given) == 0:
      ch = []
      for c in self.interpreter.commands:
        self.ok(c.help())
      return self.ok(' ')

    else:
      for c in self.interpreter.commands:
        if c.name.startswith(given[0]):
          self.ok(c.moreHelp() + '\n')
      return self.ok()


class VersionCommand(ShellCommand):
  """Print the DMLite API Version."""
  def _execute(self, given):
    return self.ok(str(self.interpreter.API_VERSION))


class GetImplIdCommand(ShellCommand):
    """Give the ID of the implementation"""

    def _execute(self, given):
        try:
            self.ok('Implementation ID of Catalog:\t\t\t\t'+self.interpreter.catalog.getImplId())
            if self.interpreter.poolManager is None:
                self.error('There is no pool manager.')
            else:
                self.ok('Implementation ID of Pool Manager:\t\t\t'+self.interpreter.poolManager.getImplId())
            if self.interpreter.authn is None:
                self.error('There is no Authentification interface.')
            else:
                self.ok('Implementation ID of Authentification interface:\t'+self.interpreter.authn.getImplId())
            return self.ok()
        except Exception, e:
            return self.error(e.__str__() + '\nParameter(s): ' + ', '.join(given))


class ExitCommand(ShellCommand):
  """Exit the DMLite shell."""
  def _execute(self, given):
    return self.exitShell()

### Catalog commands ###

class PwdCommand(ShellCommand):
  """Print the current directory."""
  def _execute(self, given):
    try:
      return self.ok(self.interpreter.catalog.getWorkingDir())
    except Exception, e:
      return self.error(e.__str__() + '\nParameter(s): ' + ', '.join(given))


class CdCommand(ShellCommand):
  """Change the current directory."""
  def _init(self):
    self.parameters = ['Ddirectory']

  def _execute(self, given):
    # change directory
    try:
      path = given[0]
      if given[0][0] == '/':
        self.interpreter.catalog.changeDir('')
        path = path[1:]
      f = self.interpreter.catalog.extendedStat(path, True)
      if f.stat.isDir():
        self.interpreter.catalog.changeDir(path)
      else:
        return self.error('The given path is not a Directory:\nParameter(s): ' + ', '.join(given))

    except Exception, e:
      return self.error(e.__str__() + '\nParameter(s): ' + ', '.join(given))
    return self.ok()


class LsCommand(ShellCommand):
  """List the content of the current directory."""
  def _init(self):
    self.parameters = ['*Ddirectory']
    pass

  def _execute(self, given):
    # if no parameters given, list current directory
    given.append(self.interpreter.catalog.getWorkingDir())

    flist = self.interpreter.listDirectory(given[0], True)
    if flist == -1:
      return self.error('"' + given[0] + '" is not a directory.')

    fnamelen = 5 # find longest filename
    for f in flist:
      if len(f['name']) > fnamelen:
        fnamelen = len(f['name'])

    flist = ( f['name'] + (' '*(fnamelen+2-len(f['name']))) + (f['prettySize'], '-> ' + f['link'])[f['isLnk']] + '\t' + f['comment'] for f in flist)

    return self.ok('\n'.join(list(flist)))


class MkDirCommand(ShellCommand):
  """Create a new directory."""
  def _init(self):
    self.parameters = ['ddirectory','*Oparent:p:parent:parents:-p:--parent:--parents']

  def _execute(self, given):
    try:
      directory = given[0]
      self.interpreter.catalog.makeDir(directory, 0777)
    except Exception, e:
      msg = e.__str__()
      code = msg[msg.find('#')+1:msg.find(']')]
      code = int(float(code)*1000000)
      # Parent missing
      if code == 2:
        parent = False
        if len(given) == 2 and given[1].lower() in ('p','parent','parents','-p','--parent','--parents'):
          listDir = []
          while(directory != '' and directory != '/'):
              listDir.append(directory)
              directory = os.path.dirname(directory)
          for dir in reversed(listDir):
              try:
                self.interpreter.catalog.makeDir(dir, 0777)
              except:
                pass
          return self.ok()
        else:
          return self.error(e.__str__() + '\nParameter(s): ' + ', '.join(given))
      # Directory already existing
      elif code == 17:
        return self.error(e.__str__() + '\nParameter(s): ' + ', '.join(given))
    return self.ok()

class UnlinkCommand(ShellCommand):
  """Remove a file from the system."""
  def _init(self):
    self.parameters = ['Dfile','*Oforce:f:force:-f:--force']

  def _execute(self, given):
    try:
      try:
        filename = given[0]
        if not filename.startswith('/'):
          filename = os.path.normpath(os.path.join(self.interpreter.catalog.getWorkingDir(), filename))
        replicas = self.interpreter.catalog.getReplicas(filename)
      except:
        replicas = []
      if replicas and not (len(given)>1 and given[1].lower() in ['f', '-f', 'force', '--force']):
        return self.error("You can't unlink a file which still has replicas. To do it, use the force flag (or just f).")
      else:
        self.interpreter.catalog.unlink(given[0])
    except Exception, e:
      return self.error(e.__str__() + '\nParameter(s): ' + ', '.join(given))
    return self.ok()


class RmDirCommand(ShellCommand):
  """Delete a directory."""
  def _init(self):
    self.parameters = ['Ddirectory','*O--recursive:-r']

  def _execute(self, given):
    dirname = given[0]
    if not dirname.startswith('/'):
          dirname = os.path.normpath(os.path.join(self.interpreter.catalog.getWorkingDir(), dirname))
    if not (len(given)>1 and given[1].lower() in ['-r', '--recursive']):
        try:
                self.interpreter.catalog.removeDir(dirname)
        except Exception, e:
              return self.error(e.__str__() + '\nParameter(s): ' + ', '.join(given))

    else:
        #recursive mode
        try:
                f = self.interpreter.catalog.extendedStat(dirname, True)
                if f.stat.isDir():
                        return  self._remove_recursive(dirname)
                else:
                        self.error('The given parameter is not a folder: Parameter(s): ' + ', '.join(given))
        except Exception, e:
              return self.error(e.__str__() + '\nParameter(s): ' + ', '.join(given))

  def _remove_recursive(self, folder):
    gfiles = self._list_folder(folder)
    for f in gfiles:
      name = os.path.join(folder, f['name'])
      if f['isDir']:
        self._remove_recursive(name)
      else:
        self.interpreter.catalog.unlink(name)
    self.interpreter.catalog.removeDir(folder)

  def _list_folder(self,folder):
    try:
          hDir = self.interpreter.catalog.openDir(folder)
    except:
          self.error("cannot open the folder: " + folder)
          return []

    flist = []

    while True:
      finfo = {}
      try:
        f = self.interpreter.catalog.readDirx(hDir)
        if f.stat.isDir():
          finfo['isDir'] = True
        else:
          finfo['isDir'] = False
        finfo['name'] = f.name

        flist.append(finfo)
      except:
        break

    self.interpreter.catalog.closeDir(hDir)
    return flist

class MvCommand(ShellCommand):
  """Move or rename a file."""
  def _init(self):
    self.parameters = ['Dsource-file', 'ddestination-file']

  def _execute(self, given):
    try:
      self.interpreter.catalog.rename(given[0], given[1])
    except Exception, e:
      return self.error(e.__str__() + '\nParameter(s): ' + ', '.join(given))
    return self.ok()


class DuCommand(ShellCommand):
  """Determine the disk usage of a file or a directory."""
  def _init(self):
    self.parameters = ['*Dfile']

  def _execute(self, given):
    # if no parameters given, list current directory
    given.append(self.interpreter.catalog.getWorkingDir())

    f = self.interpreter.catalog.extendedStat(given[0], True)
    if f.stat.isDir():
      return self.ok(str(self.folderSize(given[0])) + 'B')
    else:
      return self.ok(str(f.stat.st_size) + 'B')

  def folderSize(self, folder):
    gfiles = self.interpreter.listDirectory(folder)
    size = 0
    for f in gfiles:
      if f['isDir']:
        self.ok(f['name'] + ':')
        size += self.folderSize(os.path.join(folder, f['name']))
      else:
        self.ok('\t' + str(f['size']) + '\t' + f['name'])
        size += f['size']
    return size


class LnCommand(ShellCommand):
  """Create a symlink."""
  def _init(self):
    self.parameters = ['Dtarget-file', 'dsymlink-file']

  def _execute(self, given):
    try:
      self.interpreter.catalog.symlink(given[0], given[1])
    except Exception, e:
      return self.error(e.__str__() + '\nParameter(s): ' + ', '.join(given))
    return self.ok()


class ReadLinkCommand(ShellCommand):
  """Show the target of a symlink."""
  def _init(self):
    self.parameters = ['Dsymlink']

  def _execute(self, given):
    try:
      return self.ok(self.interpreter.catalog.readLink(given[0]))
    except Exception, e:
      return self.error(e.__str__() + '\nParameter(s): ' + ', '.join(given))


class CommentCommand(ShellCommand):
  """Set or read file comments.
  Put comment in quotes. Reset file comment via comment <file> ""."""
  def _init(self):
    self.parameters = ['Dfile', '*?comment']

  def _execute(self, given):
    if len(given) == 2:
      try:
        self.interpreter.catalog.setComment(given[0], given[1])
      except Exception, e:
        return self.error(e.__str__() + '\nParameter(s): ' + ', '.join(given))
      return self.ok()
    else:
      try:
        return self.ok(self.interpreter.catalog.getComment(given[0]))
      except Exception, e:
        return self.ok(' ') # no comment

class InfoCommand(ShellCommand):
  """Print all available information about a file."""
  def _init(self):
    self.parameters = ['Dfile']

  def _execute(self, given):
    try:
      filename = given[0]
      if not filename.startswith('/'):
        filename = os.path.normpath(os.path.join(self.interpreter.catalog.getWorkingDir(), filename))
      self.ok(filename)
      self.ok('-' * len(filename))

      f = self.interpreter.catalog.extendedStat(filename, False)
      if f.stat.isDir():
        self.ok('File type:  Folder')
      elif f.stat.isReg():
        self.ok('File type:  Regular file')
      elif f.stat.isLnk():
        self.ok('File type:  Symlink')
        self.ok('            -> ' + self.interpreter.catalog.readLink(filename))
      else:
        self.ok('File type:  Unknown')

      self.ok('Size:       ' + str(f.stat.st_size) + 'B')
      if f.status == pydmlite.FileStatus.kOnline:
        self.ok('Status:     Online')
      elif f.status == pydmlite.FileStatus.kMigrated:
        self.ok('Status:     Migrated')
      else:
        self.ok('Status:     Unknown')

      try:
        comment = self.interpreter.catalog.getComment(filename)
        self.ok('Comment:    ' + comment)
      except:
        self.ok('Comment:    None')

      self.ok('GUID:       ' + str(f.guid))
      self.ok('Ino:        ' + str(f.stat.st_ino))
      self.ok('Mode:       ' + oct(f.stat.st_mode))
      self.ok('# of Links: ' + str(f.stat.st_nlink))

      try:
        uid = pydmlite.boost_any()
        uid.setUnsigned(f.stat.st_uid)
        u = self.interpreter.authn.getUser('uid', uid)
        uname = u.name
      except Exception, e:
         if f.stat.st_uid == 0:
            uname = "root"
         else:
            uname = '???'
      self.ok('User:       %s (ID: %d)' %(uname, f.stat.st_uid))

      try:
        gid = pydmlite.boost_any()
        gid.setUnsigned(f.stat.st_gid)
        g = self.interpreter.authn.getGroup('gid', gid)
        gname = g.name
      except Exception, e:
          if f.stat.st_gid == 0:
            gname = "root"
          else:
            gname = '???'
      self.ok('Group:      %s (ID: %d)' %(gname, f.stat.st_gid))
      self.ok('CSumType:   ' + str(f.csumtype))
      self.ok('CSumValue:  ' + str(f.csumvalue))
      self.ok('ATime:      ' + time.ctime(f.stat.getATime()))
      self.ok('MTime:      ' + time.ctime(f.stat.getMTime()))
      self.ok('CTime:      ' + time.ctime(f.stat.getCTime()))

      try:
        replicas = self.interpreter.catalog.getReplicas(filename)
      except:
        replicas = []

      if not replicas:
        self.ok('Replicas:   None')
      else:
        for r in replicas:
          self.ok('Replica:    ID:     ' + str(r.replicaid) )
          self.ok('        Server: ' + r.server)
          self.ok('        Rfn:    ' + r.rfn)
          self.ok('        Status: ' + str(r.status))
          self.ok('        Type:   ' + str(r.type))
          self.ok('        Replica Extended Attributes (Key, Value):')
          for k in r.getKeys():
            self.ok("            "+ k + ":\t" + r.getString(k,""))

      a=ACLCommand('/')
      self.ok('ACL:        ' + "\n            ".join(a.getACL(self.interpreter, filename)))

      self.ok('Extended Attributes (Key, Value):')
      for k in f.getKeys():
          self.ok("            "+ k + ":\t\t" + f.getString(k,""))
      return self.ok(' ')

    except Exception, e:
      return self.error(e.__str__() + '\nParameter(s): ' + ', '.join(given))


class CreateCommand(ShellCommand):
  """Create a new file."""
  def _init(self):
    self.parameters = ['dnew file', '*?mode=755']

  def _execute(self, given):
    given.append('755')
    try:
      mode = int(given[1], 8)
    except:
      return self.syntaxError('Expected: Octal mode number')

    try:
      self.interpreter.catalog.create(given[0], mode)
    except Exception, e:
      return self.error(e.__str__() + '\nParameter(s): ' + ', '.join(given))
    return self.ok()


class ChModCommand(ShellCommand):
  """Change the mode of a file."""
  def _init(self):
    self.parameters = ['Dfile', '?mode']

  def _execute(self, given):
    try:
      mode = int(given[1], 8)
    except:
      return self.syntaxError('Expected: Octal mode number')

    try:
      self.interpreter.catalog.setMode(given[0], mode)
    except Exception, e:
      return self.error(e.__str__() + '\nParameter(s): ' + ', '.join(given))
    return self.ok()


class ChOwnCommand(ShellCommand):
  """Change the owner of a file."""
  def _init(self):
    self.parameters = ['Dfile', 'Uuser']

  def _execute(self, given):
    if self.interpreter.authn is None:
      return self.error('There is no Authentification interface.')

    try:
      f = self.interpreter.catalog.extendedStat(given[0], False)
      gid = f.stat.st_gid
      uid = self.interpreter.authn.getUser(given[1]).getUnsigned('uid',0)
      self.interpreter.catalog.setOwner(given[0], uid, gid, False)
    except Exception, e:
      return self.error(e.__str__() + '\nParameter(s): ' + ', '.join(given))


class ChGrpCommand(ShellCommand):
  """Change the group of a file."""
  def _init(self):
    self.parameters = ['Dfile', 'Ggroup']

  def _execute(self, given):
    if self.interpreter.authn is None:
      return self.error('There is no Authentification interface.')

    try:
      f = self.interpreter.catalog.extendedStat(given[0], False)
      uid = f.stat.st_uid
      gid = self.interpreter.authn.getGroup(given[1]).getUnsigned('gid',0)
      self.interpreter.catalog.setOwner(given[0], uid, gid, False)
    except Exception, e:
      return self.error(e.__str__() + '\nParameter(s): ' + ', '.join(given))

class GetChecksumCommand(ShellCommand):
  """Get or calculate file checksum"""
  def _init(self):
    self.parameters = ['Dfile', '?checksumtype', '*?forcerecalc', '*?pfn']

  def _execute(self, given):
    if self.interpreter.authn is None:
      return self.error('There is no Authentification interface.')

    forcerecalc = False
    if len(given) > 2 and (given[2] == "true" or given[2] == "1"):
        forcerecalc = True

    pfn = ""
    if len(given) > 3:
        pfn = given[3]

    try:
      csumvalue = pydmlite.StringWrapper()
      self.interpreter.catalog.getChecksum(given[0], given[1], csumvalue, pfn, forcerecalc, 15)
      return self.ok(str(given[1]) + ': ' + str(csumvalue.s))
    except Exception, e:
      return self.error(e.__str__() + '\nParameter(s): ' + ', '.join(given))

class ChecksumCommand(ShellCommand):
  """Set or read file checksums. (DEPRECATED)"""
  def _init(self):
    self.parameters = ['Dfile', '*?checksum', '*?checksumtype']

  def _execute(self, given):
    if len(given) == 1:
      try:
        f = self.interpreter.catalog.extendedStat(given[0], False)
        return self.ok(str(f.csumtype) + ': ' + str(f.csumvalue))
      except Exception, e:
        return self.error(e.__str__() + '\nParameter(s): ' + ', '.join(given))
    else:
      given.append('')
      try:
        self.interpreter.catalog.setChecksum(given[0], given[2], given[1])
        return self.ok()
      except Exception, e:
        return self.error(e.__str__() + '\nParameter(s): ' + ', '.join(given))


class UtimeCommand(ShellCommand):
  """Set the access and modification time of a file.
  If no modification time is given, the access time will be used."""
  def _init(self):
    self.parameters = ['Dfile', 'taccess-time', '*tmodification-time']

  def _execute(self, given):
    given.append(given[1]) # if mod time not given, use access time
    tb = pydmlite.utimbuf()
    tb.actime = int(time.mktime(given[1]))
    tb.modtime = int(time.mktime(given[2]))
    try:
      self.interpreter.catalog.utime(given[0], tb)
      return self.ok()
    except Exception, e:
      return self.error(e.__str__() + '\nParameter(s): ' + ', '.join(given))


class ACLCommand(ShellCommand):
  """Set or read the ACL of a file.

The expected syntax is : acl <file> <ACL> command

Where command could be set, modify or delete.

The ACL can be specified in the following form

* user::rwx ( or u:)
* user:<DN>:rwx
* user:<USERID>:rwx
* group::rwx (or g:)
* group:<GROUP>:rw
* group:<GROUPID>:rw
* other::rwx ( or o:)
* mask::rwx ( mask wil apply to user and group acls

and multiple ACLs can be included separated by a comma

N.B. In case the DN or the GROUP contain spaces  the ACL expression should be sorrounded by either " or ' e.g. acl <file> '<ACL>' set

Default ACLs can be also set by specifying default: or d: in front of the ACL expression:

* default:user::rwx
* d:user::rwx"""

  def _init(self):
    self.parameters = ['Dfile', '*?ACL', '*Ocommand=modify:set:modify:delete']

  def getACL(self, interpreter, file):
    f = interpreter.catalog.extendedStat(file, False)
    if not f.acl:
        return ['No ACL']
    output = []
    perm = {0:"---",
            1:"--x",
            2:"-w-",
            3:"-wx",
            4:"r--",
            5:"r-x",
            6:"rw-",
            7:"rwx"}
    mask = "rwx"
    defaultmask = "rwx"
    for a in f.acl:
        if a.type == 5:
            mask = perm[a.perm]
        elif a.type == 37:
            defaultmask = perm[a.perm]
    for a in f.acl:
        line = ""
        if a.type > 32:
            line += "default:"
        if a.type in [1,2,33,34]:
            line += "user:"
        elif a.type in [3,4,35,36]:
            line += "group:"
        elif a.type in [5,37]:
            line += "mask:"
        elif a.type in [6,38]:
            line += "other:"
        if a.type in [2,34]:
            try:
                uid = pydmlite.boost_any()
                uid.setUnsigned(a.id)
                u = interpreter.authn.getUser('uid', uid)
                uname = u.name
            except Exception, e:
                if a.id == 0:
                    uname = "root"
                else:
                    uname = '??? (ID: %d)' % a.id
            line += uname
        elif a.type in [4, 36]:
            try:
                gid = pydmlite.boost_any()
                gid.setUnsigned(a.id)
                g = interpreter.authn.getGroup('gid', gid)
                gname = g.name
            except Exception, e:
                if a.id == 0:
                    gname = "root"
                else:
                    gname = '??? (ID: %d)' % a.id
            line += gname
        permission = perm[a.perm]
        line += ":" + permission

        effective = "\t\t\t#effective:"
        masked_perm = False
        if (a.type in [2,3,4] and permission != mask and mask !=  7) or (a.type in [34,35,36] and permission != defaultmask and defaultmask != 7):
            for p in ['r', 'w', 'x']:
                if p in permission:
                    if p not in mask:
                        effective += "-"
                        masked_perm = True
                    else:
                        effective += p
                else:
                    effective += "-"
        if masked_perm:
            line += effective
        output.append(line)
    #output.append(f.acl.serialize())
    return output

  def _execute(self, given):
      try:
          filename = given[0]
          f = self.interpreter.catalog.extendedStat(filename, False)

          # Set the ACL
          if len(given) > 1:
            list_acl = f.acl.serialize().split(',')
            try:
                command = given[2]
            except:
                command = "modify"
            if command == 'set':
                list_acl = []
            acls = given[1].split(',')
            for acl in acls:
                result = ""

                default = False
                if acl.startswith('default:') or acl.startswith('d:') :
                    acl = acl[acl.find(':')+1:]
                    default = True
                if acl.find(':') < 0:
                    continue
                tag = acl[:acl.find(':')]
                acl = acl[acl.find(':')+1:]
                if acl.find(':') < 0:
                    continue
                id = acl[:acl.rfind(':')]
                perm_rwx = acl[acl.find(':')+1:]
                perm_count = 0
                if 'r' in perm_rwx:
                    perm_count += 4
                if 'w' in perm_rwx:
                    perm_count += 2
                if 'x' in perm_rwx:
                    perm_count += 1
                perm = str(perm_count)

                if tag in ['user', 'u'] and not id:
                    if default:
                        result = "a" + perm + str(f.stat.st_uid)
                    else:
                        result = "A" + perm + str(f.stat.st_uid)
                elif tag in ['user', 'u']:
                    if not id.isdigit():
                        u = self.interpreter.authn.getUser(id)
                        id = u.getString('uid','')
                    if default:
                        result = "b" + perm + id
                    else:
                        result = "B" + perm + id
                elif tag in ['group', 'g'] and not id:
                    if default:
                        result = "c" + perm + str(f.stat.st_gid)
                    else:
                        result = "C" + perm + str(f.stat.st_gid)
                elif tag in ['group', 'g']:
                    if not id.isdigit():
                        g = self.interpreter.authn.getGroup(id)
                        id = g.getString('gid','')
                    if default:
                        result = "d" + perm + id
                    else:
                        result = "D" + perm + id
                elif tag in ['mask', 'm']:
                    if default:
                        result = "e" + perm + '0'
                    else:
                        result = "E" + perm + '0'
                elif tag in ['other', 'o']:
                    if default:
                        result = "f" + perm + '0'
                    else:
                        result = "F" + perm + '0'

                if command == 'delete':
                    for p in list_acl:
                        p2 = p[:1] + "[0-7]" + p[2:]
                        if re.search(p2, result):
                            list_acl.remove(p)
                            break
                else:
                    modification = False
                    for i, p in enumerate(list_acl):
                        p = p[:1] + "[0-7]" + p[2:]
                        if re.search(p, result):
                            list_acl[i] = result
                            modification = True
                            break
                    if not modification:
                        list_acl.append(result)
            list_acl.sort()
            myacl = pydmlite.Acl(','.join(list_acl))
            self.interpreter.catalog.setAcl(filename, myacl)



          # Get the ACL
          if not filename.startswith('/'):
              filename = os.path.normpath(os.path.join(self.interpreter.catalog.getWorkingDir(), filename))
          output = "# file: " + filename
          try:
              uid = pydmlite.boost_any()
              uid.setUnsigned(f.stat.st_uid)
              u = self.interpreter.authn.getUser('uid', uid)
              uname = u.name
          except Exception, e:
              if f.stat.st_uid == 0:
                  uname = "root"
              else:
                  uname = '???'
          output += "\n# owner: %s (ID: %d)" % (uname, f.stat.st_uid)
          try:
              gid = pydmlite.boost_any()
              gid.setUnsigned(f.stat.st_gid)
              g = self.interpreter.authn.getGroup('gid', gid)
              gname = g.name
          except Exception, e:
              if f.stat.st_gid == 0:
                  gname = "root"
              else:
                  gname = '???'
          output += "\n# group: %s (ID: %d)" % (gname, f.stat.st_gid)
          self.ok(output)
          return self.ok("\n".join(self.getACL(self.interpreter, filename)))

      except Exception, e:
          return self.error(e.__str__() + '\nParameter(s): ' + ', '.join(given))


class SetGuidCommand(ShellCommand):
  """Set the GUID of a file."""
  def _init(self):
    self.parameters = ['Dfile', '?GUID']

  def _execute(self, given):
    try:
      self.interpreter.catalog.setGuid(given[0], given[1])
      return self.ok()
    except Exception, e:
      return self.error(e.__str__() + '\nParameter(s): ' + ', '.join(given))


class ReplicaAddCommand(ShellCommand):
  """Add a new replica for a file."""
  def _init(self):
    self.parameters = ['Dfile', 'Ostatus:available:beingPopulated:toBeDeleted', 'Otype:volatile:permanent', '?rfn', '*?server', '*?pool']

  def _execute(self, given):
    if given[1].lower() in ('a', 'available', '-'):
      rstatus = pydmlite.ReplicaStatus.kAvailable
    elif given[1].lower() in ('p', 'beingpopulated'):
      rstatus = pydmlite.ReplicaStatus.kBeingPopulated
    elif given[1].lower() in ('d', 'tobedeleted'):
      rstatus = pydmlite.ReplicaStatus.kToBeDeleted
    else:
      return self.syntaxError('This is not a valid replica status.')

    if given[2].lower() in ('v', 'volatile'):
      rtype = pydmlite.ReplicaType.kVolatile
    elif given[2].lower() in ('p', 'permanent'):
      rtype = pydmlite.ReplicaType.kPermanent
    else:
      return self.syntaxError('This is not a valid replica type.')

    try:
      f = self.interpreter.catalog.extendedStat(given[0], False)
    except Exception, e:
      return self.error(e.__str__() + '\nParameter(s): ' + ', '.join(given))

    myreplica = pydmlite.Replica()
    myreplica.fileid = f.stat.st_ino
    myreplica.status = rstatus
    myreplica.type = rtype
    myreplica.rfn = given[3]

    if len(given) == 5:
      myreplica.server = given[4]
    elif given[3].find(':/') != -1:
      myreplica.server = given[3].split(':/')[0]
    else:
      return self.syntaxError('Invalid rfn field. Expected: server:/path/file')


    if len(given) == 6:
      myreplica.setString('pool', given[5])



    try:
      self.interpreter.catalog.addReplica(myreplica)
      return self.ok()
    except Exception, e:
      return self.error(e.__str__() + '\nParameter(s): ' + ', '.join(given))


class ReplicaModifyCommand(ShellCommand):
  """Update the replica of a file."""
  def _init(self):
    self.parameters = ['Dfile', '?replica-id | rfn', 'Onew-status:available:beingPopulated:toBeDeleted', 'Onew-type:volatile:permanent', '?new-rfn', '*?new-server']

  def _execute(self, given):
    try:
      self.interpreter.catalog.getReplicas
      replicas = self.interpreter.catalog.getReplicas(given[0])

      for r in replicas:
        if given[1] in (str(r.replicaid), r.rfn):
          # found specified replica. update it!

          if given[2].lower() in ('a', 'available', '-'):
            rstatus = pydmlite.ReplicaStatus.kAvailable
          elif given[2].lower() in ('p', 'beingpopulated'):
            rstatus = pydmlite.ReplicaStatus.kBeingPopulated
          elif given[2].lower() in ('d', 'tobedeleted'):
            rstatus = pydmlite.ReplicaStatus.kToBeDeleted
          else:
            return self.syntaxError('This is not a valid replica status.')

          if given[3].lower() in ('v', 'volatile'):
            rtype = pydmlite.ReplicaType.kVolatile
          elif given[3].lower() in ('p', 'permanent'):
            rtype = pydmlite.ReplicaType.kPermanent
          else:
            return self.syntaxError('This is not a valid replica type.')

          r.status = rstatus
          r.type = rtype
          r.rfn = given[4]
          if len(given) == 6:
            r.server = given[5]
          elif given[4].find(':/') != -1:
            r.server = given[4].split(':/')[0]
          else:
            return self.syntaxError('Invalid rfn field. Expected: server:/path/file')

          self.interpreter.catalog.updateReplica(r)
          break
      else:
        return self.error('The specified replica was not found.')
      return self.ok()

    except Exception, e:
      return self.error(e.__str__() + ' ' + given[0])


class ReplicaDelCommand(ShellCommand):
  """Delete a replica for a file."""
  def _init(self):
    self.parameters = ['Dfile', '?replica-id | rfn']

  def _execute(self, given):
    try:
      self.interpreter.catalog.getReplicas
      replicas = self.interpreter.catalog.getReplicas(given[0])

      for r in replicas:
        if given[1] in (str(r.replicaid), r.rfn):
          # found specified replica. delete it!
          self.interpreter.poolDriver = self.interpreter.stackInstance.getPoolDriver('filesystem')
          poolHandler = self.interpreter.poolDriver.createPoolHandler(r.getString('pool',''))
          poolHandler.removeReplica(r)
          try:
          #remove also from catalog to clean memcache
              self.interpreter.catalog.deleteReplica(r)
          except Exception:
          #do nothing cause if it fails does not hurt
              pass
          break
      else:
        return self.error('The specified replica was not found.')
      return self.ok()

    except Exception, e:
      return self.error(e.__str__() + '\nParameter(s): ' + ', '.join(given))

### Pools commands ###

class PoolAddCommand(ShellCommand):
    """Add a pool.
The pool_type values can be: 'filesystem', 'hdfs', 's3'

the s_pace type values can be: V (for Volatile), P (for Permanent).
The latter is the default.

"""

    def _init(self):
        self.parameters = ['?pool_name', 'Opool_type:filesystem:hdfs:s3' ,'*Ospace_type:V:P']

    def _execute(self, given):
        if self.interpreter.poolManager is None:
            return self.error('There is no pool manager.')

        try:
            pool = pydmlite.Pool()
            pool.name = given[0]
            pool.type = given[1]
            if len(given) > 2:
                pool.setString("s_type", given[2])
            else:
                pool.setString("s_type", 'P')
            self.interpreter.poolManager.newPool(pool)
            return self.ok()
        except Exception, e:
            return self.error(e.__str__() + '\nParameter(s): ' + ', '.join(given))

def pprint_dictionary(dpool, indent=4):
    ret = StringIO.StringIO()
    for key, value in dpool.iteritems():
        ret.write(" " * indent)
        ret.write(key)
        ret.write(": ")
        if type(value) is dict:
            ret.write(pprint_dictionary(value, indent+4))
        elif type(value) is list and len(value) > 0 and type(value[0]) is dict:
            for item in value:
                ret.write("\n")
                ret.write(pprint_dictionary(item, indent+4))
            ret.write("\n")
        else:
            ret.write(str(value))
            ret.write("\n")
    return ret.getvalue()

class PoolInfoCommand(ShellCommand):
  """List the pools."""

  mapping = {''  : pydmlite.PoolAvailability.kAny,
             'rw': pydmlite.PoolAvailability.kForBoth,
             'r' : pydmlite.PoolAvailability.kForRead,
             'w' : pydmlite.PoolAvailability.kForWrite,
             '!' : pydmlite.PoolAvailability.kNone}

  def _init(self):
    self.parameters = ['*Oavailability:rw:r:w:!']

  def _execute(self, given):
    if self.interpreter.poolManager is None:
      return self.error('There is no pool manager.')

    try:
        if (len(given) > 0):
            availability = PoolInfoCommand.mapping[given[0]]
        else:
            availability = PoolInfoCommand.mapping['']
        pools = self.interpreter.poolManager.getPools(availability)
        for pool in pools:
            dpool = json.loads(pool.serialize())
            self.ok("%s (%s)\n%s" % (pool.name, pool.type, pprint_dictionary(dpool)))
        return self.ok()
    except Exception, e:
        return self.error(e.__str__() + '\nParameter(s): ' + ', '.join(given))


class PoolModifyCommand(ShellCommand):
    """Modify a pool.

Valid attributes for every pool:
---> defsize, gc_start_thresh, gc_stop_thresh, def_lifetime, defpintime, max_lifetime, maxpintime.

Additionnal attributes for:
* s3:
---> redirect_port, backend_port, signedlinktimeout, hostname, backend_protocol, bucketsalt, s3accesskeyid, s3secretaccesskey, mode, buckettype, usetorrent.
* hdfs:
---> hostname, username, mode, port.
    """

    poolAttributes = {'filesystem': {'long': ['defsize',
                                              'gc_start_thresh',
                                              'gc_stop_thresh',
                                              'def_lifetime',
                                              'defpintime',
                                              'max_lifetime',
                                              'maxpintime'],
                                     'string': ['fss_policy',
                                                'gc_policy',
                                                'mig_policy',
                                                'rs_policy',
                                                'ret_policy',
                                                's_type']},
                      's3': {'long':['redirect_port',
                                    'backend_port',
                                    'signedlinktimeout'],
                             'string':['hostname',
                                       'backend_protocol',
                                       'bucketsalt',
                                       's3accesskeyid',
                                       's3secretaccesskey',
                                       'mode',
                                       'buckettype',
                                       'usetorrent',]},
                      'hdfs': {'long':[],
                               'string':['hostname',
                                       'username',
                                       'mode',
                                       'port']},
                    }

    def _init(self):
        self.parameters = ['?pool_name', '?attribute', '?value']

    def _execute(self, given):
        if self.interpreter.poolManager is None:
            return self.error('There is no pool manager.')

        try:
            name = given[0]
            attribute = given[1]
            value = given[2]
            pool = self.interpreter.poolManager.getPool(name)
            attrList = PoolModifyCommand.poolAttributes
            longList = list(attrList['filesystem']['long'])
            stringList = list(attrList['filesystem']['string'])
            if (pool.type in attrList.keys() and pool.type != 'filesystem'):
                longList += attrList[pool.type]['long']
                stringList += attrList[pool.type]['string']
            if attribute in longList:
                pool.setLong(attribute, long(value))
            elif attribute in stringList:
                pool.setString(attribute, value)
            else:
                return self.error(attribute+' is a wrong attribute. Type "help modifypool" for more help.')
            self.interpreter.poolManager.updatePool(pool)
            return self.ok()
        except Exception, e:
            return self.error(e.__str__() + '\nParameter(s): ' + ', '.join(given))


class PoolDelCommand(ShellCommand):
    """Delete a pool."""

    def _init(self):
        self.parameters = ['?pool_name']

    def _execute(self, given):
        if self.interpreter.poolManager is None:
            return self.error('There is no pool manager.')
        try:
            pool = self.interpreter.poolManager.getPool(given[0])
            self.interpreter.poolManager.deletePool(pool)
            return self.ok()
        except Exception, e:
            return self.error(e.__str__() + '\nParameter(s): ' + ', '.join(given))


class QryConfCommand(ShellCommand):
    """Query the configuration of the pools."""

    def _execute(self, given):
        if self.interpreter.poolManager is None:
            return self.error('There is no pool manager.')

        try:
            availability = pydmlite.PoolAvailability.kAny
            pools = self.interpreter.poolManager.getPools(availability)
            info,err = self.interpreter.executor.getSpaceInfo(self.interpreter.domeheadurl)
            data  = json.loads(info)
            for pool in pools:
                output = "POOL %s " % (pool.name)
                for key in ['defsize', 'gc_start_thresh', 'gc_stop_thresh', 'def_lifetime', 'defpintime', 'max_lifetime', 'maxpintime']:
                    value = str(pool.getLong(key, -1))+" "
                    if value < 0:
                        value = ""
                    output += key.upper()+" "+value
                for key in ['groups', 'fss_policy', 'gc_policy', 'mig_policy', 'rs_policy', 'ret_policy', 's_type']:
                    value = pool.getString(key, "")+" "
                    output += key.upper()+" "+value
                self.ok(output)

                try:
                    self.interpreter.poolDriver = self.interpreter.stackInstance.getPoolDriver(pool.type)
                except Exception, e:
                    self.interpreter.poolDriver = None
                    return self.error('Could not initialise the pool driver.\n' + e.__str__())
                poolHandler = self.interpreter.poolDriver.createPoolHandler(pool.name)
                capacity = poolHandler.getTotalSpace()
                free = poolHandler.getFreeSpace()
                if capacity != 0:
                    rate = round(float(100 * free ) / capacity,1)
                else:
                    rate = 0
                self.ok('\t\tCAPACITY %s FREE %s ( %.1f%%)' % (self.prettySize(capacity), self.prettySize(free), rate))

                for _pool in data['poolinfo'].keys():
                    if _pool == pool.name:
                        try:
                            for server in data['poolinfo'][_pool]['fsinfo'].keys():
                                for _fs in data['poolinfo'][_pool]['fsinfo'][server].keys():
                                    fs =  data['poolinfo'][_pool]['fsinfo'][server][_fs]
                                    if int(fs['physicalsize']) != 0:
                                        rate = round(float(100 * float(fs['freespace'])) / float(fs['physicalsize']),1)
                                    else:
                                        rate = 0
                                    if int(fs['fsstatus']) == 2:
                                        status = 'RDONLY'
                                    elif int (fs['fsstatus']) == 1:
                                        status = 'DISABLED'
                                    else:
                                        status = ''
                                    self.ok("\t%s %s CAPACITY %s FREE %s ( %.1f%%) %s" % (server, _fs, self.prettySize(fs['physicalsize']), self.prettySize(fs['freespace']), rate, status))
                        except Exception, e:
                            pass
            return self.ok()
        except Exception, e:
            return self.error(e.__str__() + '\nParameter(s): ' + ', '.join(given))

### User and Group commands ###

class GroupAddCommand(ShellCommand):
  """Add a new group."""
  def _init(self):
    self.parameters = ['?group name']

  def _execute(self, given):
    if self.interpreter.authn is None:
      return self.error('There is no Authentification interface.')

    try:
        self.interpreter.authn.newGroup(given[0])
        self.ok('Group added')
    except Exception, e:
        return self.error(e.__str__() + '\nParameter(s): ' + ', '.join(given))

class UserAddCommand(ShellCommand):
  """Add a new user."""
  def _init(self):
    self.parameters = ['?user name']

  def _execute(self, given):
    if self.interpreter.authn is None:
      return self.error('There is no Authentification interface.')

    try:
        self.interpreter.authn.newUser(given[0])
        self.ok('User added')
    except Exception, e:
        return self.error(e.__str__() + '\nParameter(s): ' + ', '.join(given))


class GroupInfoCommand(ShellCommand):
  """List the informations about all the groups or about the specified group."""
  def _init(self):
    self.parameters = ['*Ggroup name or ID']

  def _execute(self, given):
    if self.interpreter.authn is None:
      return self.error('There is no Authentification interface.')

    try:
      if given:
        try:
            gid = int(given[0])
            groupID = pydmlite.boost_any()
            groupID.setUnsigned(gid)
            g = self.interpreter.authn.getGroup('gid',groupID)
        except:
            g = self.interpreter.authn.getGroup(given[0])
            gid = g.getLong('gid',-1)
        ban = g.getLong('banned',0)
        if ban == 1:
          status = 'Banned - ARGUS_BAN'
        elif ban == 2:
          status = 'Banned - LOCAL_BAN'
        else:
          status = 'Not Banned'
        self.ok('Group:  %s' % (g.name))
        self.ok('Gid:    %s' % (str(gid)))
        self.ok('Status: %s' % (status))
      else:
        groups = self.interpreter.authn.getGroups()
        for g in groups:
          gid = g.getLong('gid',-1)
          ban = g.getLong('banned',0)
          if ban == 1:
            status = ' (BANNED - ARGUS_BAN)'
          elif ban == 2:
            status = ' (BANNED - LOCAL_BAN)'
          else:
            status = ''
          self.ok(' - %s\t(ID: %d)\t%s' % (g.name, gid, status))
    except Exception, e:
      return self.error(e.__str__() + '\nParameter(s): ' + ', '.join(given))


class UserInfoCommand(ShellCommand):
  """List the informations about all the users or about the specified user."""
  def _init(self):
    self.parameters = ['*Uuser name or ID']

  def _execute(self, given):
    if self.interpreter.authn is None:
      return self.error('There is no Authentification interface.')

    try:
      if given:
        try:
            uid = int(given[0])
            userID = pydmlite.boost_any()
            userID.setUnsigned(uid)
            u = self.interpreter.authn.getUser('uid',userID)
        except:
            u = self.interpreter.authn.getUser(given[0])
            uid = u.getLong('uid',-1)
        ban = u.getLong('banned',0)
        if ban == 1:
          status = 'Banned - ARGUS_BAN'
        elif ban == 2:
          status = 'Banned - LOCAL_BAN'
        else:
          status = 'Not Banned'
        self.ok('User:   %s' % (u.name))
        self.ok('Uid:    %s' % (str(uid)))
        self.ok('Status: %s' % (status))
      else:
        users = self.interpreter.authn.getUsers()
        for u in users:
          uid = u.getLong('uid',-1)
          ban = u.getLong('banned',0)
          if ban == 1:
            status = ' (BANNED - ARGUS_BAN)'
          elif ban == 2:
            status = ' (BANNED - LOCAL_BAN)'
          else:
            status = ''
          self.ok(' - %s\t(ID: %d)\t%s' % (u.name, uid, status))
    except Exception, e:
      return self.error(e.__str__() + '\nParameter(s): ' + ', '.join(given))


class GroupBanCommand(ShellCommand):
  """Modify the ban status of a group.

Give as parameter the name or ID of the group you want to modify and its new status:
NO_BAN or 0, ARGUS_BAN or 1, LOCAL_BAN or 2"""
  def _init(self):
    self.parameters = ['Ggroup name or ID','Ostatus:NO_BAN:0:ARGUS_BAN:1:LOCAL_BAN:2']

  def _execute(self, given):
    if self.interpreter.authn is None:
      return self.error('There is no Authentification interface.')

    try:
        try:
            gid = int(given[0])
            groupID = pydmlite.boost_any()
            groupID.setUnsigned(gid)
            g = self.interpreter.authn.getGroup('gid',groupID)
        except:
            g = self.interpreter.authn.getGroup(given[0])
        if given[1] == 'LOCAL_BAN' or given[1] == '2':
            g.setLong('banned',2)
        elif given[1] == 'ARGUS_BAN' or given[1] == '1':
            g.setLong('banned',1)
        else:
            g.setLong('banned',0)
        self.interpreter.authn.updateGroup(g)
        self.ok('Status modified')
    except Exception, e:
        return self.error(e.__str__() + '\nParameter(s): ' + ', '.join(given))


class UserBanCommand(ShellCommand):
  """Modify the ban status of a user.

Give as parameter the name or ID of the user you want to modify and its new status:
NO_BAN or 0, ARGUS_BAN or 1, LOCAL_BAN or 2"""
  def _init(self):
    self.parameters = ['Uuser name or ID','Ostatus:NO_BAN:0:ARGUS_BAN:1:LOCAL_BAN:2']

  def _execute(self, given):
    if self.interpreter.authn is None:
      return self.error('There is no Authentification interface.')

    try:
        try:
            uid = int(given[0])
            userID = pydmlite.boost_any()
            userID.setUnsigned(uid)
            u = self.interpreter.authn.getUser('uid',userID)
        except:
            u = self.interpreter.authn.getUser(given[0])
        if given[1] == 'LOCAL_BAN' or given[1] == '2':
            u.setLong('banned',2)
        elif given[1] == 'ARGUS_BAN' or given[1] == '1':
            u.setLong('banned',1)
        else:
            u.setLong('banned',0)
        self.interpreter.authn.updateUser(u)
        self.ok('Status modified')
    except Exception, e:
        return self.error(e.__str__() + '\nParameter(s): ' + ', '.join(given))


class GroupDelCommand(ShellCommand):
  """Delete a group."""
  def _init(self):
    self.parameters = ['Ggroup name or ID']

  def _execute(self, given):
    if self.interpreter.authn is None:
      return self.error('There is no Authentification interface.')

    try:
        try:
            gid = int(given[0])
            groupID = pydmlite.boost_any()
            groupID.setUnsigned(gid)
            groupname = self.interpreter.authn.getGroup('gid',groupID).name
        except:
            groupname = given[0]
        self.interpreter.authn.deleteGroup(groupname)
        return self.ok('Group deleted')
    except Exception, e:
        return self.error(e.__str__() + '\nParameter(s): ' + ', '.join(given))


class UserDelCommand(ShellCommand):
  """Delete a user."""
  def _init(self):
    self.parameters = ['Uuser name or ID']

  def _execute(self, given):
    if self.interpreter.authn is None:
      return self.error('There is no Authentification interface.')

    try:
        try:
            uid = int(given[0])
            userID = pydmlite.boost_any()
            userID.setUnsigned(uid)
            username = self.interpreter.authn.getUser('uid',userID).name
        except:
            username = given[0]
        self.interpreter.authn.deleteUser(username)
        return self.ok('User deleted')
    except Exception, e:
        return self.error(e.__str__() + '\nParameter(s): ' + ', '.join(given))


restart_dpm_reminder = ("\n\n" + "*" * 80 + "\n"
  "If your storage system is using the legacy DPM stack, please don't forget\n"
  "to restart the DPM daemon after any filesystem changes.\n"
  "Running 'service dpm restart' should be enough.\n"
  + "*" * 80 + "\n")

class FsAddCommand(ShellCommand):
    """Add a filesystem. Dome needs to be installed and running. (package dmlite-dome)"""
    def _init(self):
        self.parameters = ['?filesystem name', '?pool name', '?server']

    def _execute(self, given):
        if self.interpreter.poolManager is None:
            return self.error('There is no pool manager.')

        try:
            # the pool might not have filesystems yet, and even though it exists in the
            # db, dome does not report it.
            try:
                pool = self.interpreter.poolManager.getPool(given[1])
                if pool.type != 'filesystem':
                    return self.error('The pool is not compatible with filesystems.')
            except:
                pass

            status = 0

            fs = given[0]
            pool = given[1]
            server = given[2]

            out,err = self.interpreter.executor.addFsToPool(self.interpreter.domeheadurl, fs, pool, server, status)
            if err:
                return self.error(out)
            else:
                print(out)
                return self.ok(restart_dpm_reminder)
        except Exception, e:
            return self.error(e.__str__() + '\nParameter(s): ' + ', '.join(given))


class FsModifyCommand(ShellCommand):
    """Modify a filesystem. Dome needs to be installed and running. (package dmlite-dome)

Status must have one of the following values: 0 for ENABLED, 1 for DISABLED, 2 for RDONLY.

"""
    def _init(self):
        self.parameters = ['?filesystem name', '?server', '?pool name', '?status']

    def _execute(self, given):
        if self.interpreter.poolManager is None:
            return self.error('There is no pool manager.')

        try:
            fs = given[0]
            server = given[1]
            pool = given[2]
            status = int(given[3])

            if status > 2 or status < 0:
                return self.error('Unknown status value: ' + str(status))

            out,err = self.interpreter.executor.modifyFs(self.interpreter.domeheadurl, fs, pool, server,
                                                     status)
            if err:
                return self.error(out)
            else:
                print(out)
                return self.ok(restart_dpm_reminder)
        except Exception, e:
            return self.error(e.__str__() + '\nParameter(s): ' + ', '.join(given))

class FsDelCommand(ShellCommand):
    """Delete a filesystem. Dome needs to be installed and running. (package dmlite-dome)"""

    def _init(self):
        self.parameters = ['?filesystem name', '?server']

    def _execute(self, given):
        if self.interpreter.poolManager is None:
            return self.error('There is no pool manager.')

        try:
            fs = given[0]
            server = given[1]

            out,err = self.interpreter.executor.rmFs(self.interpreter.domeheadurl, fs, server)
            if err:
                return self.error(out)
            else:
                print(out)
                return self.ok(restart_dpm_reminder)
        except Exception, e:
            return self.error(e.__str__() + '\nParameter(s): ' + ', '.join(given))

### Replicate and Drain commands ###

class Util(object):
        #check if mysql plugin is properly configured
        @staticmethod
        def checkConf():
            try :
                conf = open("/etc/dmlite.conf.d/mysql.conf", 'r')
            except Exception, e:
                print e.__str__()
                return False

            adminUserName = None
            dnisroot = None
            hostcert = False

            for line in conf:
                if line.startswith("AdminUsername"):
                    adminUserName = line[len("AdminUserName")+1:len(line)].strip()
                if line.startswith("HostDNIsRoot"):
                    dnisroot=  line[len("HostDNIsRoot")+1:len(line)].strip()
                if line.startswith("HostCertificate"):
                    hostcert = True
            conf.close()

            if (dnisroot is None) or (dnisroot == 'no'):
                print 'HostDNIsRoot must be set to yes on the configuration files'
                return False
            if  adminUserName is None:
                print 'No AdminUserName defined on the configuration files'
                return False
            if not hostcert:
                print 'No HostCertificate defined on the configuration files'

            return adminUserName

        @staticmethod
        def setFSReadonly(dpm2,interpreter,sourceFS):
            #check which implementations are loaded

            catalogImpl = interpreter.catalog.getImplId()
            if 'DomeAdapterHeadCatalog' not in catalogImpl:
                if not dpm2.dpm_modifyfs(sourceFS.server, sourceFS.name, 2, sourceFS.weight):
                    return 0
                else:
                    interpreter.error('Not possible to set Filesystem '+ sourceFS.server +"/" +sourceFS.name + " To ReadOnly. Exiting.")
                    return 1
            else:
                    out, err = interpreter.executor.modifyFs(interpreter.domeheadurl, sourceFS.name, sourceFS.poolname, sourceFS.server,2)
                    if err:
                        interpreter.error('Not possible to set Filesystem '+ sourceFS.server +"/" +sourceFS.name + " To ReadOnly. Exiting.")
                        return 1
                    else:
                        return 0

        @staticmethod
        def printComments(interpreter):
            interpreter.ok('')
            interpreter.ok('===================================================================================================================================================================================')
            interpreter.ok("The process is running in dryrun mode, please add the option 'dryrun false' to effectively perform the drain process")
            interpreter.ok('\n')
            interpreter.ok("Make sure to have LCGDM-Dav properly setup on your infrastructure. The process contacts the Headnode by default via https on the 443 port and the disknodes via http on the 80 port")
            interpreter.ok('\n')
            interpreter.ok("If your infrastructure has different ports configured please use the DPM_HTTPS_PORT and DPM_HTTP_PORT env variabile to configure the drain process accordingly")
            interpreter.ok('\n')
            interpreter.ok("The disknodes should ALL have the same port configured")
            interpreter.ok('\n')
            interpreter.ok("Please also monitor the draining logs, and in case of errors due to timeouts/daemons overloaded please adjust accordingly the number of draining threads( Default = 5)")
            interpreter.ok('===================================================================================================================================================================================')
            interpreter.ok('\n')




class Response(object):
  """ utility class to collect the response """
  def __init__(self):
    self.chunks = []
    self.markers = []
    self.header_dict = {}
  def callback(self, chunk):
    self.chunks.append(chunk)
  def content(self):
    return ''.join(self.chunks)
   ## Callback function invoked when body data is ready
  def body(self,buf):
    self.markers.append(buf)
  def headers(self):
    s = ''.join(self.chunks)
    for line in s.split('\r\n'):
      try:
        key,val = line.split(':',1)
        self.header_dict[key] = val
      except:
        pass
    return self.header_dict
  def printHeaders(self):
    if not self.header_dict:
        self.headers()
    for key, value in self.header_dict.iteritems():
        print key+"="+ value
  def checkMarkers(self):
        for marker in self.markers:
            if 'Success' in marker:
                return 0
            elif 'Failed' in marker:
                return marker
        return "Error Contacting the remote disknode"
  def printMarkers(self):
        for marker in self.markers:
            print marker

class Replicate(object):
    """Replicate a File to a specific pool/filesystem, used by other commands so input validation has been already done"""
    def __init__(self,interpreter, filename, spacetoken, parameters):
        self.interpreter=interpreter
        self.parameters =parameters
        self.spacetoken = spacetoken
        self.filename= filename
        self.interpreter.replicaQueueLock.acquire()
        securityContext= self.interpreter.stackInstance.getSecurityContext()
        securityContext.user.name = self.parameters['adminUserName']
        self.interpreter.stackInstance.setSecurityContext(securityContext)

        replicate = pydmlite.boost_any()
        replicate.setBool(True)
        self.interpreter.stackInstance.set("replicate",replicate)
        try:
                self.interpreter.stackInstance.erase("pool")
                self.interpreter.stackInstance.erase("filesystem")
                self.interpreter.stackInstance.erase("f_type")
                self.interpreter.stackInstance.erase("lifetime")
                self.interpreter.stackInstance.erase("SpaceToken")
        except Exception,e:
                self.interpreter.replicaQueueLock.release()
        self.interpreter.replicaQueueLock.release()

    def run(self):
        self.interpreter.replicaQueueLock.acquire()

        if 'poolname' in self.parameters:
                poolname = pydmlite.boost_any()
                poolname.setString(self.parameters['poolname'])
                self.interpreter.stackInstance.set("pool",poolname)
        if 'filesystem' in self.parameters:
                #filesystem
                filesystem = pydmlite.boost_any()
                filesystem.setString(self.parameters['filesystem'])
                self.interpreter.stackInstance.set("filesystem",filesystem)
        if 'filetype' in self.parameters:
                #filetype
                filetype = pydmlite.boost_any()
                #check if the file type is correct
                if (self.parameters['filetype']  ==  'V') or (self.parameters['filetype'] ==  'D') or (self.parameters['filetype'] ==  'P'):
                        filetype.setString(self.parameters['filetype'])
                else:
                        self.interpreter.error('Incorrect file Type, it should be P (permanent), V (volatile) or D (Durable)')
                        self.interpreter.replicaQueueLock.release()
                        return (False, None, 'Incorrect file Type, it should be P (permanent), V (volatile) or D (Durable)')

                self.interpreter.stackInstance.set("f_type",filetype)
        if 'lifetime' in self.parameters:

                #lifetime
                lifetime = pydmlite.boost_any()
                _lifetime = self.parameters['lifetime']
                if _lifetime == 'Inf':
                        _lifetime= 0x7FFFFFFF
                elif _lifetime.endswith('y'):
                        _lifetime=_lifetime[0:_lifetime.index('y')]
                        _lifetime= long(_lifetime) * 365 * 86400
                elif given[4].endswith('m'):
                        _lifetime=_lifetime[0:_lifetime.index('m')]
                        _lifetime= long(_lifetime) * 30 * 86400
                elif given[4].endswith('d'):
                        _lifetime=_lifetime[0:_lifetime.index('d')]
                        _lifetime= long(_lifetime) * 86400
                elif given[4].endswith('h'):
                        _lifetime=_lifetime[0:_lifetime.index('h')]
                        _lifetime= long(_lifetime) * 3600

                lifetime.setLong(_lifetime)
                self.interpreter.stackInstance.set("lifetime",lifetime)
        if self.spacetoken:
                #spacetoken
                _spacetoken = pydmlite.boost_any()
                _spacetoken.setString(self.spacetoken)
                self.interpreter.stackInstance.set("SpaceToken",_spacetoken)

        self.interpreter.replicaQueueLock.release()

        try:
                loc = self.interpreter.poolManager.whereToWrite(self.filename)
        except Exception, e:
                self.interpreter.error(e.__str__())
                return (False, None, e.__str__())

        #checking ports to use
        http_port = 80
        try:
                http_port = os.environ['DPM_HTTP_PORT']
        except Exception, e:
                pass

        https_port = 443
        try:
                https_port = os.environ['DPM_HTTPS_PORT']
        except Exception, e:
                pass


        destination = loc[0].url.toString()
        destination = urllib.unquote(destination)
        #create correct destination url and SFN
        sfn = destination[0:destination.index(':')+1] + destination[destination.index(':')+1:destination.index('?')]
        destination = destination[0:destination.index(':')+1]+str(http_port)+destination[destination.index(':')+1:len(destination)]

        destination = 'http://'+destination
        res = Response()
        c = pycurl.Curl()
        #using DPM cert locations
        c.setopt(c.SSLKEY,'/etc/grid-security/dpmmgr/dpmkey.pem')
        c.setopt(c.SSLCERT, '/etc/grid-security/dpmmgr/dpmcert.pem')
        c.setopt(c.HEADERFUNCTION, res.callback)
        c.setopt(c.WRITEFUNCTION, res.body)
        c.setopt(c.SSL_VERIFYPEER, 0)
        c.setopt(c.SSL_VERIFYHOST, 2)
        c.setopt(c.CUSTOMREQUEST, 'COPY')
        c.setopt(c.HTTPHEADER, ['Destination: '+destination, 'X-No-Delegate: true'])
        c.setopt(c.FOLLOWLOCATION, 1)
        c.setopt(c.URL, 'https://'+socket.getfqdn()+':'+str(https_port)+'/'+self.filename)

        try:
                c.perform()
        except Exception, e:
                self.interpreter.error(e.__str__())
                return (False,sfn, e.__str__())
        #check markers
        try:
                outcome = res.checkMarkers()
                if not outcome:
                        return (True,sfn, None)
                else:
                        return (False,sfn, outcome)
        except Exception, e:
                self.interpreter.error(e.__str__())
                return (False,sfn, e.__str__())


class DrainThread (threading.Thread):
    """ Thread running a portion of the draining activity"""
    def __init__(self, interpreter,threadID,parameters):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.interpreter = interpreter
        self.parameters = parameters
        self.stopEvent =  threading.Event()

    def run(self):
        self.drainReplica(self.threadID)

    def stop(self):
        self.stopEvent.set()

    def drainReplica(self,threadName):
        while not self.stopEvent.isSet():
            self.interpreter.replicaQueueLock.acquire()
            if not self.interpreter.replicaQueue.empty():
                replica = self.interpreter.replicaQueue.get()
                self.interpreter.replicaQueueLock.release()
                drainreplica = DrainFileReplica(self.threadID,self.interpreter,replica,self.parameters)
                drainreplica.drain();
            else:
                self.interpreter.replicaQueueLock.release()
                self.stopEvent.set()


class ReplicaMoveCommand(ShellCommand):
    """Move a specified rfn folder to a new filesystem location

The replicamove command accepts the following parameters:

* <sourceFileSystem>        : the source fileystem where to move the replicas from ( in the form servername:fsname)
* <sourceFolder>            : the source folder
* <destFilesystem>          : the filesystem where to move the file to ( in the form as servername:fsname)
* filetype <filetype>       : the filetype of the new replica, it could be P (permanent), D (durable), V (volatile) (optional, default = P )
* lifetime <lifetime>       : the lifetime of the new replica, it can be specified as a multiple of y,m,d,h or Inf (infinite) (optional, default = Inf)
* nthreads <threads>        : the number of threads to use in the process (optional, default = 5)
* dryrun <true/false>       : if set to true just print the statistics (optional, default = true)

ex:
        replicamove dpmdisk01.cern.ch:/srv/dpm/01 /dteam/2015-11-25/ dpmdisk02.cern.ch:/srv/dpm/01
"""

    def _init(self):
        self.parameters = ['?sourceFilesystem', '?sourceFolder', '?destFilesystem',
                '*Oparameter:filetype:lifetime:nthreads:dryrun',  '*?value',
                '*Oparameter:filetype:lifetime:nthreads:dryrun',  '*?value',
                '*Oparameter:filetype:lifetime:nthreads:dryrun',  '*?value',
                '*Oparameter:filetype:lifetime:nthreads:dryrun',  '*?value',
                '*Oparameter:filetype:lifetime:nthreads:dryrun',  '*?value' ]

    def _execute(self,given):
        if self.interpreter.stackInstance is None:
            return self.error('There is no stack Instance.')

        if self.interpreter.poolManager is None:
            return self.error('There is no pool manager.')

        adminUserName = Util.checkConf()

        if not adminUserName:
            return self.error("DPM configuration is not correct")

        if len(given)%2 == 0:
            return self.error("Incorrect number of parameters")

        #default
        parameters = {}
        parameters['nthreads'] = 5
        parameters['dryrun'] = True
        parameters['adminUserName'] = adminUserName
        parameters['group'] = 'ALL'
        parameters['size'] = 100
        parameters['move'] = True

        try:
            sourceServer,sourceFilesystem = given[0].split(':')
            sourceFolder = given[1]
            parameters['filesystem']  =  given[2]
            for i in range(1, len(given),2):
                if given[i] == "filetype":
                    parameters['filetype'] = given[i+1]
                elif given[i] == "lifetime":
                    parameters['lifetime'] = given[i+1]
                elif given[i] == "nthreads":
                    nthreads = int(given[i+1])
                    if nthreads < 1 or nthreads > 10:
                        return self.error("Incorrect number of Threads: it must be between 1 and 10")
                    else:
                        parameters['nthreads'] = nthreads
                elif given[i] == "dryrun":
                    if given[i+1] == "False" or given[i+1] == "false" or given[i+1] == "0":
                        parameters['dryrun']  = False

        except Exception, e:
            return self.error(e.__str__() + '\nParameter(s): ' + ', '.join(given))

        #instantiating DPMDB
        try:
            db = DPMDB()
        except Exception, e:
            return self.error(e.__str__() + '\nParameter(s): ' + ', '.join(given))

        try:
            #check if destination FS exists and it's not disabled/readonly
            availability = pydmlite.PoolAvailability.kAny
            pools = self.interpreter.poolManager.getPools(availability)

            destServer,destFS =  parameters['filesystem'].split(':')
            doMove = False
            sourceFS = None
            for pool in pools:
                listFS = db.getFilesystems(pool.name)
                for fs in listFS:
                    if fs.name == destFS and fs.server == destServer and  fs.status == 0:
                        doMove = True
                    if fs.name == sourceFilesystem and fs.server == sourceServer:
                        sourceFS = fs

                if not doMove:
                    return self.error("The specified destination filesystem has not been found in the DPM configuration or it's not available for writing")
                if not sourceFS:
                    return self.error("The specified source filesystem has not been found in the DPM configuration")

            #set as READONLY the FS  to drain
            if not parameters['dryrun']:
                if Util.setFSReadonly(dpm2,self.interpreter,sourceFS):
                    return
            else:
                Util.printComments(self.interpreter)


            self.ok("Calculating Replicas to Move..")
            self.ok()

            #step 2 : get all FS associated to the pool to drain and get the list of replicas
            listTotalFiles = db.getReplicaInFSFolder(sourceFilesystem,sourceServer,sourceFolder)

            #step 3 : for each file call the drain method of DrainFileReplica
            self.interpreter.replicaQueue = Queue.Queue(len(listTotalFiles))
            self.interpreter.replicaQueue.queue.clear()
            self.interpreter.replicaQueueLock = threading.Lock()

            self.drainProcess = DrainReplicas(self.interpreter, db, listTotalFiles, parameters)
            self.drainProcess.drain()

        except Exception, e:
            return self.error(e.__str__() + '\nParameter(s): ' + ', '.join(given))

class ReplicateCommand(ShellCommand):
    """Replicate a File to a specific pool/filesystem

The replicate command accepts the following parameters:

* <filename>                            : the file to replicate (absolute path)
* poolname      <poolname>              : the pool where to replicate the file to (optional)
* filesystem    <filesystemname>        : the filesystem where to replicate the file to, specified as servername:fsname (optional)
* filetype      <filetype>              : the filetype of the new replica, it could be P (permanent), D (durable), V (volatile) (optional, default = P )
* lifetime      <lifetime>              : the lifetime of the new replica, it can be specified as a multiple of y,m,d,h or Inf (infinite) (optional, default = Inf)
* spacetoken    <spacetoken>            : the spacetoken ID to assign the new replica to (optional)
* dryrun        <true/false>            : if set to true just print the info (optional, default = true)
"""

    def _init(self):
        self.parameters = ['Dfilename', '*Oparameter:poolname:filesystem:filetype:lifetime:spacetoken:dryrun',  '*?value',
            '*Oparameter:poolname:filesystem:filetype:lifetime:spacetoken:dryrun',  '*?value',
            '*Oparameter:poolname:filesystem:filetype:lifetime:spacetoken:dryrun',  '*?value',
            '*Oparameter:poolname:filesystem:filetype:lifetime:spacetoken:dryrun',  '*?value',
            '*Oparameter:poolname:filesystem:filetype:lifetime:spacetoken:dryrun',  '*?value' ]

    def printComments(self):
        self.interpreter.ok('')
        self.interpreter.ok('===================================================================================================================================================================================')
        self.interpreter.ok("Your are running in dryrun mode, please add the option 'dryrun false' to effectively perform the file replication")
        self.interpreter.ok('\n')
        self.interpreter.ok("Make sure to have LCGDM-Dav properly setup on your infrastructure. The process contacts the Headnode by default via https on the 443 port and the disknodes via http on the 80 port")
        self.interpreter.ok('\n')
        self.interpreter.ok("If your infrastructure has different ports configured please use the DPM_HTTPS_PORT and DPM_HTTP_PORT env variabile to configure the drain process accordingly")
        self.interpreter.ok('\n')
        self.interpreter.ok("The disknodes should have ALL the same port configured")
        self.interpreter.ok('\n')
        self.interpreter.ok('Please also note that if the file is associated to a spacetoken the new replica is not going to be added to that spacetoken unless you specify it via the \'spacetoken\' parameter')
        self.interpreter.ok('===================================================================================================================================================================================')
        self.interpreter.ok('\n')

    def _execute(self, given):
        if self.interpreter.stackInstance is None:
            return self.error('There is no stack Instance.')

        if self.interpreter.poolManager is None:
            return self.error('There is no pool manager.')

        adminUserName = Util.checkConf()
        if not adminUserName:
            return self.error("DPM configuration is not correct")

        if len(given)%2 == 0:
            return self.error("Incorrect number of parameters")

        parameters = {}
        self.interpreter.replicaQueueLock = threading.Lock()
        filename = given[0]
        if filename.endswith('/'):
            return self.error("The File to replicate cannot end with /: " +filename+"\n")
        if not filename.startswith('/'):
            filename = os.path.normpath(os.path.join(self.interpreter.catalog.getWorkingDir(), filename))
        parameters['filename'] = filename
        parameters['adminUserName'] = adminUserName
        parameters['move'] = False
        dryrun = True
        spacetoken = None

        for i in range(1, len(given),2):
            parameters[given[i]] = given[i+1]

        try:
            spacetoken= parameters['spacetoken']
        except:
            pass

        try:
            if parameters['dryrun'] == "False" or parameters['dryrun'] == "false" or parameters['dryrun'] == "0":
                dryrun = False
        except:
            pass

        error = None
        destination = None
        replicated = None

        if dryrun:
            self.printComments()
            return 1

        try:
            replicate = Replicate(self.interpreter,filename, spacetoken, parameters)
            (replicated,destination, error) = replicate.run()
        except Exception, e:
            self.error(e.__str__())
            self.error("Error Replicating file: " +filename+"\n")
            if error:
                self.error(error)
            if destination:
                #logging only need to clean pending replica
                self.error("Error while copying to SFN: " +destination+"\n")

            else:
                return 1
        cleanReplica = False
        if not replicated:
            if error:
                self.error(error)
            if destination:
                #logging only need to clean pending replica
                self.error("Error while copying to SFN: " +destination+"\n")
                cleanReplica = True
            else:
                self.error("Error Replicating file: " +filename+"\n")
                return 1
        replica = None
        if replicated:
            #check replica status
            try:
                replica = self.interpreter.catalog.getReplicaByRFN(destination)
                if replica.status != pydmlite.ReplicaStatus.kAvailable:
                    cleanReplica = True
                    self.error("Error while updating the replica status\n")
                else:
                    self.ok("The file has been correctly replicated to: "+ destination+"\n")
            except Exception, e:
                self.error("Error while checking the replica status\n")
                cleanReplica = True

        if cleanReplica:
            if not replica:
                try:
                    replica = self.interpreter.catalog.getReplicaByRFN(destination)
                except Exception, e:
                    self.error("Error while checking the replica status\n")
                    self.error('Please remove manually the replica with rfn: ' + destination)
                    return 1
            try:
                self.interpreter.poolDriver = self.interpreter.stackInstance.getPoolDriver('filesystem')
            except Exception, e:
                self.error('Could not initialise the pool driver to clean the replica\n' + e.__str__())
                self.error('Please remove manually the replica with rfn: ' + destination)
                return 1
            try:
                if replica.getString('pool',''):
                    poolHandler = self.interpreter.poolDriver.createPoolHandler(replica.getString('pool',''))
                    poolHandler.removeReplica(replica)
                else:
                    self.error('Could not clean the replica.\n' + e.__str__())
                    self.error('Please remove manually the replica with rfn: ' + destination)
                    return 1
            except Exception, e:
                self.error('Could not clean the replica.\n' + e.__str__())
                self.error('Please remove manually the replica with rfn: ' + destination)
                return 1
        return 0




class DrainFileReplica(object):
    """implement draining of a file replica"""
    def __init__(self, threadID,interpreter , fileReplica, parameters):
        self.threadID = threadID
        self.interpreter= interpreter
        self.fileReplica = fileReplica
        self.parameters = parameters

    def getThreadID(self):
        return "Thread " + str(self.threadID) +": "

    def logError(self, msg):
        return self.interpreter.error(self.getThreadID()+msg)

    def logOK(self, msg):
        return self.interpreter.ok(self.getThreadID()+msg)

    def drain(self):
        filename = self.fileReplica.lfn
        #step 4 : check the status, pinned  and see if they the replica can be drained
        if self.fileReplica.status != "-":
                if self.fileReplica.status =="P":
                        self.logError("The file with replica sfn: "+ self.fileReplica.sfn + " is under population, ignored\n")
                        self.interpreter.drainErrors.append ( (filename, self.fileReplica.sfn, "The file is under population"))
                        return 1
                else:
                        #new behaviour, in case the file is in status D we should remove the file and the replicas
                        self.logOK("The file with replica sfn: "+ self.fileReplica.sfn + " is under deletion, it can be safely removed\n")
                        self.interpreter.catalog.unlink(filename)
                        return 0
        currenttime= int(time.time())
        if (self.fileReplica.pinnedtime > currenttime):
                self.logError("The replica sfn: "+ self.fileReplica.sfn + " is currently pinned, ignored\n")
                self.interpreter.drainErrors.append ( (filename, self.fileReplica.sfn, "The file is pinned"))
                return 1

        #step 5-1: check spacetoken parameters,if set use that one
        if self.fileReplica.setname is not "":
                self.spacetoken = self.fileReplica.setname
                self.logOK("The file with replica sfn: "+ self.fileReplica.sfn + " belongs to the spacetoken: " + self.fileReplica.setname +"\n")
        else:
                self.spacetoken = None

        replicate = Replicate(self.interpreter,filename, self.spacetoken,self.parameters)

        self.logOK("Trying to replicate file: %s\n" % filename);

        replicated = None
        destination = None
        error = None
        try:
                (replicated,destination, error) = replicate.run()
        except Exception, e:
                self.logError(e.__str__())
                self.logError("Error moving Replica for file: " +filename+"\n")
                self.interpreter.drainErrors.append ((filename, self.fileReplica.sfn, e.__str__()))
                if destination:
                        #logging only need to clean pending replica
                        self.logError("Error while copying to SFN: " +destination+"\n")
                else:
                        return 1
        cleanReplica = False
        if not replicated:
                if destination:
                        #logging only need to clean pending replica
                        self.logError("Error while copying to SFN: " +destination+"\n")
                        self.interpreter.drainErrors.append ((filename, self.fileReplica.sfn, "Error while copying to SFN: " +destination +" with error: " +str(error)))
                        cleanReplica = True
                else:
                        self.logError("Error moving Replica for file: " +filename+"\n")
                        self.interpreter.drainErrors.append ((filename, self.fileReplica.sfn, error))
                        return 1
        replica = None
        if replicated:
                #check replica status
                try:
                        replica = self.interpreter.catalog.getReplicaByRFN(destination)
                        if replica.status != pydmlite.ReplicaStatus.kAvailable:
                                cleanReplica = True
                                self.logError("Error while updating the replica status for file: "+ filename+"\n")
                                self.interpreter.drainErrors.append ((filename, self.fileReplica.sfn, "Error while updating the replica status"))
                        else:
                                self.logOK("The file has been correctly replicated to: "+ destination+"\n")
                except Exception, e:
                        self.logError("Error while checking the replica status for file  "+ filename+"\n")
                        self.interpreter.drainErrors.append ((filename, self.fileReplica.sfn, "Error while checking the replica status"))
                        cleanReplica = True


        #step 6 : remove drained replica file if correctly replicated or erroneus drained file
        if not cleanReplica:
                try:
                        replica = self.interpreter.catalog.getReplicaByRFN(self.fileReplica.sfn)
                        pool = self.interpreter.poolManager.getPool(self.fileReplica.poolname)
                        self.interpreter.poolDriver = self.interpreter.stackInstance.getPoolDriver(pool.type)
                except Exception, e:
                        self.interpreter.drainErrors.append ((filename, self.fileReplica.sfn, "Error while getting the original replica from the catalog, cannot drain"))
                        return self.logError('Error while getting the original replica from the catalog for file: '+ filename+', cannot drain.\n' + e.__str__())

        else:
                try:
                        replica = self.interpreter.catalog.getReplicaByRFN(destination)
                        pool = self.interpreter.poolManager.getPool(replica.getString('pool',''))
                        self.interpreter.poolDriver = self.interpreter.stackInstance.getPoolDriver(pool.type)
                except Exception, e:
                        self.interpreter.drainErrors.append ((filename, self.fileReplica.sfn, "Error while getting the new replica from the catalog, cannot clean"))
                        return self.logError('Error while getting the new replica from the catalog for file: '+ filename+', cannot clean.\n' + e.__str__())
        #retry 3 times:
        for i in range(0,3):
                try:
                        poolHandler = self.interpreter.poolDriver.createPoolHandler(pool.name)
                        poolHandler.removeReplica(replica)
                except Exception, e:
                        if i == 2:
                                if not cleanReplica:
                                        self.interpreter.drainErrors.append ((filename, self.fileReplica.sfn, "Could not remove the original replica"))
                                        return self.logError('Could not remove the original replica for file: '+ filename+'\n' + e.__str__())
                                else:
                                        self.interpreter.drainErrors.append ((filename, self.fileReplica.sfn, "Could not clean the new replica"))
                                        return self.logError('Could not remove the new replica for file: '+ filename+'\n' + e.__str__())
                        else:
                                 continue
                #cleaning catalog ( not throwing exception if fails though could it could be already cleaned by the poolhandler.removereplica
                try:
                        self.interpreter.catalog.deleteReplica(replica)
                except Exception:
                        pass
                break

class DrainReplicas(object):
    """implement draining of a list of replicas"""
    def __init__(self, interpreter , db,fileReplicas, parameters):
        self.interpreter= interpreter
        self.fileReplicas=fileReplicas
        self.db = db
        self.parameters= parameters
        self.interpreter.drainErrors = []
        self.threadpool = []

    def stopThreads(self):
        self.interpreter.ok('Drain process Stopped, Waiting max 10 seconds for each running thread to end...')
        for t in self.threadpool:
                t.stop()
        for t in self.threadpool:
                t.join(10)
        self.printDrainErrors()


    def printDrainErrors(self):
        if len(self.interpreter.drainErrors) > 0:
                self.interpreter.ok("List of Errors:\n")
        for (file, sfn, error) in self.interpreter.drainErrors:
                self.interpreter.ok("File: " + file+ "\tsfn: " +sfn +"\tError: " +error)

    def drain(self):
        gid = None
        #filter by group
        try:
                if self.parameters['group'] != "ALL":
                        gid = self.db.getGroupIdByName(self.parameters['group'])
                numFiles = 0
                fileSize = 0

                for file in self.fileReplicas:
                        # filter on group check if the filereplica match
                        if (self.parameters['group'] != "ALL"):
                                if file.gid != gid:
                                    continue
                        filename = self.db.getLFNFromSFN(file.sfn)
                        if not filename:
                            continue
                        file.lfn = filename
                        numFiles = numFiles+1
                        fileSize = fileSize + file.size
                if self.parameters['move']:
                        self.interpreter.ok("Total replicas to move: " + str(numFiles))
                        self.interpreter.ok("Total capacity to move: " + str(fileSize/1024) + " KB")
                else:
                        self.interpreter.ok("Total replicas installed in the FS to drain: " + str(numFiles))
                        self.interpreter.ok("Total capacity installed in the FS to drain: " + str(fileSize/1024) + " KB")

                #in case the size is != 100, we should limit the number of replicas to drain
                sizeToDrain = fileSize
                if self.parameters['size'] != 100:
                        sizeToDrain = sizeToDrain*self.parameters['size']/100
                if not self.parameters['move']:
                        self.interpreter.ok("Percentage of capacity to drain: " + str(self.parameters['size'])+ " %")
                        self.interpreter.ok("Total capacity to drain: " + str(sizeToDrain/1024)+ " KB")

                for file in self.fileReplicas:
                        if (self.parameters['group'] != "ALL"):
                                if file.gid != gid:
                                        continue
                        if self.parameters['size']  != 100:
                                if sizeToDrain > 0:
                                        sizeToDrain= sizeToDrain-file.size
                                else:
                                        break
                        self.interpreter.replicaQueue.put(file)

                if self.parameters['dryrun'] :
                        return

                for i in range(0,self.parameters['nthreads'] ):
                        thread = DrainThread(self.interpreter, i,self.parameters)
                        thread.setDaemon(True)
                        thread.start()
                        self.threadpool.append(thread)

                while not self.interpreter.replicaQueue.empty():
                        pass

                for t in self.threadpool:
                        t.stop()
                if self.parameters['move']:
                        self.interpreter.ok("Move Process completed\n")
                else:
                        self.interpreter.ok("Drain Process completed\n")

                self.printDrainErrors()

        except Exception, e:
                return self.interpreter.error(e.__str__())


class DrainPoolCommand(ShellCommand):
    """Drain a specific pool

The drainpool command accepts the following parameters:

* <poolname>                    : the pool to drain
* group         <groupname>     : the group the files to drain belongs to  (optional, default = ALL)
* size          <size>          : the percentage of size to drain (optional, default = 100)
* nthreads      <threads>       : the number of threads to use in the drain process (optional, default = 5)
* dryrun        <true/false>    : if set to true just print the drain statistics (optional, default = true)"""

    def _init(self):
        self.parameters = ['?poolname', '*Oparameter:group:size:nthreads:dryrun',  '*?value',
                '*Oparameter:group:size:nthreads:dryrun',  '*?value',
                '*Oparameter:group:size:nthreads:dryrun',  '*?value',
                '*Oparameter:group:size:nthreads:dryrun',  '*?value' ]
    def _execute(self, given):
        if self.interpreter.stackInstance is None:
            return self.error('There is no stack Instance.')
        if self.interpreter.poolManager is None:
            return self.error('There is no pool manager.')
        if 'dpm2' not in sys.modules:
            return self.error("DPM-python is missing. Please do 'yum install dpm-python'.")

        adminUserName = Util.checkConf()
        if not adminUserName:
            return self.error("DPM configuration is not correct")

        if len(given)%2 == 0:
            return self.error("Incorrect number of parameters")

        #default
        parameters = {}
        parameters['group'] = 'ALL'
        parameters['size'] = 100
        parameters['nthreads']= 5
        parameters['dryrun'] = True
        parameters['adminUserName'] = adminUserName
        parameters['move'] = False

        try:
                poolname = given[0]
                for i in range(1, len(given),2):
                        if given[i] == "group":
                                parameters['group'] = given[i+1]
                        elif given[i] == "size":
                                size = int(given[i+1])
                                if size > 100 or size < 1:
                                        return self.error("Incorrect Drain size: it must be between 1 and 100")
                                parameters['size']  = size
                        elif given[i] == "nthreads":
                                nthreads = int(given[i+1])
                                if nthreads < 1 or nthreads > 10:
                                        return self.error("Incorrect number of Threads: it must be between 1 and 10")
                                parameters['nthreads'] = nthreads
                        elif given[i] == "dryrun":
                                if given[i+1] == "False" or given[i+1] == "false" or given[i+1] == "0":
                                        parameters['dryrun'] = False
        except Exception, e:
                return self.error(e.__str__() + '\nParameter(s): ' + ', '.join(given))

        #instantiating DPMDB
        try:
                db = DPMDB()
        except Exception, e:
                return self.error(e.__str__() + '\nParameter(s): ' + ', '.join(given))


        #get information on pools
        try:
                availability = pydmlite.PoolAvailability.kAny
                pools = self.interpreter.poolManager.getPools(availability)

                poolToDrain = None
                poolForDraining = None
                for pool in pools:
                        if pool.name==poolname:
                                poolToDrain = pool
                        else:
                                poolForDraining = pool
                if poolToDrain is None:
                        return self.error("The poolname to drain has not been found in the configuration");
                if poolForDraining  is None:
                        return self.error("The poolname for draining has not been found in the configuration");

                listFStoDrain = db.getFilesystems(poolToDrain.name)


                #step 1 : set as READONLY all FS in the pool to drain
                if not parameters['dryrun']:
                        for fs in listFStoDrain:
                                if Util.setFSReadonly(dpm2,self.interpreter,fsToDrain):
                                        return
                else:
                        Util.printComments(self.interpreter)
                self.ok("Calculating Replicas to Drain..")
                self.ok()

                #step 2 : get all FS associated to the pool to drain and get the list of replicas
                listTotalFiles = db.getReplicasInPool(poolToDrain.name)

                #step 3 : for each file call the drain method of DrainFileReplica
                self.interpreter.replicaQueue = Queue.Queue(len(listTotalFiles))
                self.interpreter.replicaQueue.queue.clear()
                self.interpreter.replicaQueueLock = threading.Lock()

                self.drainProcess = DrainReplicas(self.interpreter, db, listTotalFiles, parameters)
                self.drainProcess.drain()

        except Exception, e:
                return self.error(e.__str__() + '\nParameter(s): ' + ', '.join(given))

class DrainFSCommand(ShellCommand):
    """Drain a specific filesystem

The drainfs command accepts the following parameters:

* <servername>                          : the FQDN of the server to drain
* group         <groupname>             : the group the files to drain belongs to  (optional, default = ALL)
* size          <size>                  : the percentage of size to drain (optional, default = 100)
* nthreads      <threads>               : the number of threads to use in the drain process (optional, default = 5)
* dryrun        <true/false>            : if set to true just print the drain statistics (optional, default = true)"""

    def _init(self):
        self.parameters = ['?server', '?filesystem' , '*Oparameter:group:size:nthreads:dryrun',  '*?value',
                        '*Oparameter:group:size:nthreads:dryrun',  '*?value',
                        '*Oparameter:group:size:nthreads:dryrun',  '*?value',
                        '*Oparameter:group:size:nthreads:dryrun',  '*?value' ]
    def _execute(self, given):
        if self.interpreter.stackInstance is None:
            return self.error('There is no stack Instance.')
        if self.interpreter.poolManager is None:
            return self.error('There is no pool manager.')
        if 'dpm2' not in sys.modules:
            return self.error("DPM-python is missing. Please do 'yum install dpm-python'.")

        adminUserName = Util.checkConf()
        if not adminUserName:
            return self.error("DPM configuration is not correct")

        if len(given)%2 != 0:
            return self.error("Incorrect number of parameters")

        #default
        parameters = {}
        parameters['group'] = 'ALL'
        parameters['size'] = 100
        parameters['nthreads']= 5
        parameters['dryrun'] = True
        parameters['adminUserName'] = adminUserName
        parameters['move'] = False

        try:
                servername = given[0]
                filesystem = given[1]
                for i in range(2, len(given),2):
                        if given[i] == "group":
                                parameters['group']= given[i+1]
                        elif given[i] == "size":
                                size = int(given[i+1])
                                if size > 100 or size < 1:
                                        return self.error("Incorrect Drain size: it must be between 1 and 100")
                                parameters['size'] = size
                        elif given[i] == "nthreads":
                                nthreads = int(given[i+1])
                                if nthreads < 1 or nthreads > 10:
                                        return self.error("Incorrect number of Threads: it must be between 1 and 10")
                                parameters['nthreads'] = nthreads
                        elif given[i] == "dryrun":
                                if given[i+1] == "False" or given[i+1] == "false" or given[i+1] == "0":
                                        parameters['dryrun'] = False
        except Exception, e:
                return self.error(e.__str__() + '\nParameter(s): ' + ', '.join(given))

        #instantiating DPMDB
        try:
                db = DPMDB()
        except Exception, e:
                return self.error(e.__str__() + '\nParameter(s): ' + ', '.join(given))

        #check if the filesystem is ok and also check if other filesystems are available
        try:
                availability = pydmlite.PoolAvailability.kAny
                pools = self.interpreter.poolManager.getPools(availability)

                fsToDrain = None
                doDrain = False

                for pool in pools:
                        listFS = db.getFilesystems(pool.name)
                        for fs in listFS:
                            if fs.name == filesystem and fs.server == servername:
                                fsToDrain = fs
                            elif fs.status == 0 :
                                doDrain = True

                if doDrain  and fsToDrain:
                        pass
                else:
                        if not doDrain:
                                return self.error("There are no other availalble Filesystems for Draining.")
                        if not fsToDrain:
                                return self.error("The specified filesystem has not been found in the DPM configuration")

                #set as READONLY the FS  to drain
                if not parameters['dryrun']:
                        if Util.setFSReadonly(dpm2,self.interpreter,fsToDrain):
                                return
                else:
                        Util.printComments(self.interpreter)
                self.ok("Calculating Replicas to Drain..")
                self.ok()
                #get all files to drain
                listFiles = db.getReplicasInFS(fsToDrain.name, fsToDrain.server)

                #step 3 : for each file call the drain method of DrainFileReplica
                self.interpreter.replicaQueue = Queue.Queue(len(listFiles))
                self.interpreter.replicaQueue.queue.clear()
                self.interpreter.replicaQueueLock = threading.Lock()

                self.drainProcess = DrainReplicas( self.interpreter, db,listFiles,parameters)
                self.drainProcess.drain()

        except Exception, e:
                return self.error(e.__str__() + '\nParameter(s): ' + ', '.join(given))

class DrainServerCommand(ShellCommand):
    """Drain a specific Disk Server

The drainserver command accepts the following parameters:

* <servername>                  : the FQDN of the server to drain
* group         <groupname>     : the group the files to drain belongs to (optional, default = ALL)
* size          <size>          : the percentage of size to drain (optional, default = 100)
* nthreads      <threads>       : the number of threads to use in the drain process (optional, default = 5)
* dryrun        <true/false>    : if set to true just print the drain statistics (optional, default = true)"""

    def _init(self):
        self.parameters = ['?server',  '*Oparameter:group:size:nthreads:dryrun',  '*?value',
                                       '*Oparameter:group:size:nthreads:dryrun',  '*?value',
                                       '*Oparameter:group:size:nthreads:dryrun',  '*?value',
                                       '*Oparameter:group:size:nthreads:dryrun',  '*?value' ]

    def _execute(self, given):
        if self.interpreter.stackInstance is None:
            return self.error('There is no stack Instance.')
        if self.interpreter.poolManager is None:
            return self.error('There is no pool manager.')
        if 'dpm2' not in sys.modules:
            return self.error("DPM-python is missing. Please do 'yum install dpm-python'.")

        adminUserName = Util.checkConf()
        if not adminUserName:
            return self.error("DPM configuration is not correct")

        if len(given)%2 == 0:
            return self.error("Incorrect number of parameters")

        #default
        parameters = {}
        parameters['group'] = 'ALL'
        parameters['size'] = 100
        parameters['nthreads']= 5
        parameters['dryrun'] = True
        parameters['adminUserName'] = adminUserName
        parameters['move'] = False

        try:
                servername = given[0]
                for i in range(1, len(given),2):
                        if given[i] == "group":
                                parameters['group']= given[i+1]
                        elif given[i] == "size":
                                size = int(given[i+1])
                                if size > 100 or size < 1:
                                        return self.error("Incorrect Drain size: it must be between 1 and 100")
                                parameters['size'] = size
                        elif given[i] == "nthreads":
                                nthreads = int(given[i+1])
                                if nthreads < 1 or nthreads > 10:
                                        return self.error("Incorrect number of Threads: it must be between 1 and 10")
                                parameters['nthreads'] = nthreads
                        elif given[i] == "dryrun":
                                if given[i+1] == "False" or given[i+1] == "false" or given[i+1] == "0":
                                        parameters['dryrun'] = False

        except Exception, e:
                return self.error(e.__str__() + '\nParameter(s): ' + ', '.join(given))

        #instantiating DPMDB
        try:
                db = DPMDB()
        except Exception, e:
                return self.error(e.__str__() + '\nParameter(s): ' + ', '.join(given))

        #check if the server is ok and also check if other filesystems in other diskservers are available
        try:
                availability = pydmlite.PoolAvailability.kAny
                pools = self.interpreter.poolManager.getPools(availability)

                serverToDrain = False
                doDrain = False

                for pool in pools:
                        listServers= db.getServers(pool.name)
                        for server in listServers:
                                if server == servername:
                                        serverToDrain = True
                                #check if other filesystems in other disk servers are avaialble
                                else:
                                        for fs in db.getFilesystemsInServer(server):
                                                if fs.status == 0 :
                                                       doDrain = True

                if doDrain  and serverToDrain:
                        pass
                else:
                        if not doDrain:
                                return self.error("There are no other Filesystems available in different Disk Servers for Draining.")
                        if not serverToDrain:
                                return self.error("The specified server has not been found in the DPM configuration")
                #set as READONLY the FS  to drain
                if not parameters['dryrun']:
                        for fs in db.getFilesystemsInServer(servername):
                                if Util.setFSReadonly(dpm2,self.interpreter,fs):
                                        return
                else:
                        Util.printComments(self.interpreter)
                self.ok("Calculating Replicas to Drain..")
                self.ok()
                #get all files to drain
                listFiles = db.getReplicasInServer(servername)

                #step 3 : for each file call the drain method of DrainFileReplica
                self.interpreter.replicaQueue = Queue.Queue(len(listFiles))
                self.interpreter.replicaQueue.queue.clear()
                self.interpreter.replicaQueueLock = threading.Lock()

                self.drainProcess = DrainReplicas( self.interpreter, db,listFiles,parameters)
                self.drainProcess.drain()
        except Exception, e:
                return self.error(e.__str__() + '\nParameter(s): ' + ', '.join(given))


### Quota Token Commands###

class QuotaTokenGetCommand(ShellCommand):
    """List the quota tokens for the given path

The command accepts the following paramameter:

* <path>        : the path
* -s            : the command will print the quota token associated to the subfolders of the given path (optional)"""

    def _init(self):
         self.parameters = ['Dpath','*O--subfolders:-s']

    def _execute(self, given):
         if len(given) < 1:
             return self.error("Incorrect number of parameters")
         path = given[0]
         getsubdirs = False
         getparentdirs = True
         if (len(given) == 2 and given[1].lower() in ['-s', '--subfolders']):
             getsubdirs = True
             getparentdirs = False

         out,err =  self.interpreter.executor.getquotatoken(self.interpreter.domeheadurl,path,getparentdirs,getsubdirs)
         try:
             data  = json.loads(out)
         except ValueError:
             self.error("No quota token defined for the path: " +path)
             return
         for token in data.keys():
             self.ok("\n")
             if path == data[token]['path']:
                 self.ok("****** The Quota Token associated to your folder ******")
             else:
                 self.ok("*******************************************************")
             self.ok("\n")
             self.ok("Token ID:\t" + token)
             self.ok("Token Name:\t" + data[token]['quotatkname'])
             self.ok("Token Path:\t" + data[token]['path'])
             self.ok("Token Pool:\t" + data[token]['quotatkpoolname'])
             self.ok("Token Total Space:\t" + self.interpreter.prettySize(data[token]['quotatktotspace']))
             self.ok("Pool Total Space:\t" + self.interpreter.prettySize(data[token]['pooltotspace']))
             self.ok("Path Used Space:\t" + self.interpreter.prettySize(data[token]['pathusedspace']))
             self.ok("Path Free Space:\t" + self.interpreter.prettySize(data[token]['pathfreespace']))
             self.ok("Groups:")#sometimes the groups are not avaialble immediately after a quotatoken is created
             try:
                for group in data[token]['groups'].keys():
                    self.ok("\tID:\t" + group + "\tName:\t" + data[token]['groups'][group])
             except:
                pass


class QuotaTokenModCommand(ShellCommand):
    """Modify a quota token, given its id

The command accepts the following parameters:

* token_id           : the token id
* path <path>        : the path
* pool <poolname>    : the pool name associated to the token
* size <size>        : the quota size and the corresponding unit of measure (kB, MB, GB, TB, PB), e.g. 2TB , 45GB
* desc <description> : a description of the token
* groups <groups>    : a comma-separated list of the groups that have write access to this quotatoken"""

    def _init(self):
        self.parameters = ['?token_id',  'Oparameter:path:pool:size:desc:groups',  '*?value',
                                       '*Oparameter:path:pool:size:desc:groups',  '*?value',
                                       '*Oparameter:path:pool:size:desc:groups',  '*?value',
                                       '*Oparameter:path:pool:size:desc:groups',  '*?value',
                                       '*Oparameter:path:pool:size:desc:groups',  '*?value' ]

    def _execute(self, given):
        if len(given) < 3 or len(given) % 2 == 0:
            return self.error("Incorrect number of parameters")
        s_token = given[0]
        lfn = None
        pool = None
        size = None
        desc = None
        groups = None

        try:
            for i in range(1, len(given),2):
                if given[i] == "pool":
                    pool =  given[i+1]
                elif given[i] == "size":
                    size = self.interpreter.prettyInputSize(given[i+1])
                    if size < 0:
                        return self.error("Incorrect size: it must be a positive integer")
                elif given[i] == "desc":
                    desc = given[i+1]
                elif given[i] == "groups":
                    groups = given[i+1]
                elif given[i] == "path":
                    lfn = given[i+1]
                    if lfn.endswith('/'):
                        return self.error("The path cannot end with /: " +lfn+"\n")
                    if not lfn.startswith('/'):
                        lfn = os.path.normpath(os.path.join(self.interpreter.catalog.getWorkingDir(), lfn))

        except Exception, e:
          return self.error(e.__str__() + '\nParameter(s): ' + ', '.join(given))

        out, err = self.interpreter.executor.modquotatoken(self.interpreter.domeheadurl,s_token,lfn,pool,size,desc,groups)
        if err:
            self.error(out)
        else:
            self.ok(out)

class QuotaTokenSetCommand(ShellCommand):
    """Set a quota token for the given path

The command accepts the following parameter:

* <path>             : the path
* pool <poolname>    : the pool name associated to the token
* size <size>        : the quota size and the corresponding unit of measure (kB, MB, GB, TB, PB), e.g. 2TB , 45GB
* desc <description> : a description of the token
* groups <groups>    : a comma-separated list of the groups that have write access to this quotatoken"""


    def _init(self):
        self.parameters = ['Dpath','Oparameter:pool:size:desc',  '*?value',
                                       'Oparameter:pool:size:desc:groups',  '*?value',
                                       'Oparameter:pool:size:desc:groups',  '*?value',
                                       'Oparameter:pool:size:desc:groups',  '*?value' ]

    def _execute(self, given):
        if len(given) < 4:
            return self.error("Incorrect number of parameters")
        lfn = given[0]
        if lfn.endswith('/'):
            return self.error("The path cannot end with /: " +lfn+"\n")
        if not lfn.startswith('/'):
            lfn = os.path.normpath(os.path.join(self.interpreter.catalog.getWorkingDir(), lfn))
        try:
            for i in range(1, len(given),2):
                if given[i] == "pool":
                    pool =  given[i+1]
                elif given[i] == "size":
                    size = self.interpreter.prettyInputSize(given[i+1])
                    if size < 0:
                        return self.error("Incorrect size: it must be a positive integer")
                elif given[i] == "desc":
                    desc = given[i+1]
                elif given[i] == "groups":
                    groups = given[i+1]
        except Exception, e:
          return self.error(e.__str__() + '\nParameter(s): ' + ', '.join(given))

        out, err = self.interpreter.executor.setquotatoken(self.interpreter.domeheadurl,lfn,pool,size,desc,groups)
        if err:
            self.error(out)
        else:
            self.ok(out)

class QuotaTokenDelCommand(ShellCommand):
    """Del the quota token for the given path

The command accepts the following parameters:

* <path>        : the path to remove the quota from
* <pool>        : the pool associated to the quota token to remove"""

    def _init(self):
         self.parameters = ['Dpath','?pool']

    def _execute(self, given):
         if len(given) < 2:
             return self.error("Incorrect number of parameters")
         lfn = given[0]
         pool = given[1]
         out, err = self.interpreter.executor.delquotatoken(self.interpreter.domeheadurl,lfn,pool)
         if err:
            self.error(out)
         else:
            self.ok(out)
 
class FindCommand(ShellCommand):
    """Find a file in the namespace based on the given pattern

The command accepts the following parameters:

* <patter>  : the string to look for in the namespace
* -d        : retrieve folder instead of files (default is file)

in order to retrieve all the namespace elemenent yuo can use this options

find ""

for folders

find "" -d
"""


    def _init(self):
         self.parameters = ['?name','*?-d']

    def _execute(self, given):
         if len(given) < 1:
             return self.error("Incorrect number of parameters")
         pattern = given[0]
         folder = False
         if (len(given) > 1 and given[1].lower() in ['-d']) :
             folder = True;
         ret = list()
         try:
             db = DPMDB()
             ret = db.find(pattern,folder)
         except Exception, e:
            return self.error(e.__str__() + '\nParameter(s): ' + ', '.join(given))
         if ret != None:
             if len(ret) > 0:
                 return self.ok(('\n'.join(ret)))
             else:
                 return self.error("Cannot find files with the given pattern")
         else: 
             return self.error("Cannot find files with the given pattern")

class GetLfnCommand(ShellCommand):
    """Retrieve the LFN associated to the given SFN"""
  
    def _init(self):
         self.parameters = ['?sfn']

    def _execute(self, given):
         if len(given) < 1:
             return self.error("Incorrect number of parameters")
         sfn = given[0]
         lfn = ""
         try:
             db = DPMDB()
             lfn = db.getLFNFromSFN(sfn)
         except Exception, e:
             return self.error("Cannot find the given SFN")
         self.ok(lfn)
