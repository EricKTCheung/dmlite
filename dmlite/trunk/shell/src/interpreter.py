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
    # f local file/directory
    # d DMLite file/directory
    #   c DMLite shell command
    #   o one of the options in the format ofieldname:option1:option2:option3
    # ? other, no checks done
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
      parameter = parameter.split(':')[0]
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
    self.parameters = ['*fconfiguration file']
    
  def _execute(self, given):
    # if config file parameter not given, use default one
    given.append(self.interpreter.defaultConfigurationFile)   

    # initialise the necessary dmlite objects with the configuration file
    if not os.path.isfile(given[0]):
      return self.syntaxError('Configuration file "' + given[0] + '" is not a valid file.')
    self.interpreter.configurationFile = given[0]

    # check the existance of the pydmlite library
    try:
      self.interpreter.API_VERSION = pydmlite.API_VERSION
      if not self.interpreter.quietMode:
        self.ok('DMLite shell v0.7.1 (using DMLite API v' + str(self.interpreter.API_VERSION) + ')')
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
      self.interpreter.catalog.changeDir('/')
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
      self.ok('Using configuration "' + self.interpreter.configurationFile + '" as root.')

    if 'dpm2' not in sys.modules:
        self.ok("DPM-python has not been found. Please do 'yum install dpm-python' or the following commands will not work: fsadd, fsdel, fsmodify (and qryconf partially).")

    return self.ok() 


class HelpCommand(ShellCommand):
  """Print a help text or descriptions for single commands."""
  def _init(self):
    self.parameters = ['*ccommand']

  def _execute(self, given):
    if len(given) == 0:
      ch = []
      for c in self.interpreter.commands:
        self.ok(c.help())
      return self.ok(' ')

    else:
      foundOne = False
      for c in self.interpreter.commands:
        if c.name.startswith(given[0]):
          foundOne = True
          self.ok(c.moreHelp() + '\n')

      if not foundOne:
        return self.error('Command "' + given[0] + '" not found.')
      else:
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
      if given[0][0] == '/':
        workingDir = ''
      else:
        workingDir = os.path.normpath(self.interpreter.catalog.getWorkingDir())
        if workingDir[-1] != '/':
          workingDir += '/'
      path = os.path.normpath(workingDir + given[0])
      self.interpreter.catalog.changeDir(path)
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
    self.parameters = ['ddirectory','*?parent']
    
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
        if len(given) == 2 and given[1].lower() in ('p','parent', '-p', '--parent'):
          listDir = []
          while(directory != ''):
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
  """Remove a file from the database."""
  def _init(self):
    self.parameters = ['Dfile','*?force']
    
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
    self.parameters = ['Ddirectory']
    
  def _execute(self, given):
    try:
      self.interpreter.catalog.removeDir(given[0])
    except Exception, e:
      return self.error(e.__str__() + '\nParameter(s): ' + ', '.join(given))
    return self.ok()


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
          self.ok('            Server: ' + r.server)
          self.ok('            Rfn:    ' + r.rfn)
          self.ok('            Status: ' + str(r.status))
          self.ok('            Type:   ' + str(r.type))
      
      a=ACLCommand('/')
      self.ok('ACL:        ' + "\n            ".join(a.getACL(self.interpreter, filename)))
      
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


class ChecksumCommand(ShellCommand):
  """Set or read file checksums."""
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
        self.interpreter.catalog.setChecksum(given[0], given[1], given[2])
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
  """Set or read the ACL of a file."""
  def _init(self):
    self.parameters = ['Dfile', '*?ACL']

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
      if len(given) == 1: # Get the ACL
        filename = given[0]
        f = self.interpreter.catalog.extendedStat(filename, False)
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
      else: # Set the ACL
        myacl = pydmlite.Acl(given[1])
        self.interpreter.catalog.setAcl(given[0], myacl)
        return self.ok()
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
    self.parameters = ['Dfile', 'Ostatus:available:beingPopulated:toBeDeleted', 'Otype:volatile:permanent', '?rfn', '*?server']
    
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
          self.interpreter.catalog.deleteReplica(r)
          break
      else:
        return self.error('The specified replica was not found.')
      return self.ok()

    except Exception, e:
      return self.error(e.__str__() + '\nParameter(s): ' + ', '.join(given))

### Pools commands ###

class PoolAddCommand(ShellCommand):
    """Add a pool."""

    def _init(self):
        self.parameters = ['?pool_name', '?pool_type']

    def _execute(self, given):
        if self.interpreter.poolManager is None:
            return self.error('There is no pool manager.')

        try:
            pool = pydmlite.Pool()
            pool.name = given[0]
            pool.type = given[1]
            self.interpreter.poolManager.newPool(pool)
            return self.ok()
        except Exception, e:
            return self.error(e.__str__() + '\nParameter(s): ' + ', '.join(given))


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
        self.ok("%s (%s)\n\t%s" % (pool.name, pool.type, pool.serialize()))
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
            for pool in pools:
                output = "POOLS %s " % (pool.name)
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

                if (pool.type == 'filesystem' and 'dpm2' in sys.modules):
                    fileSystems = dpm2.dpm_getpoolfs(pool.name)
                    for fs in fileSystems:
                        if fs.capacity != 0:
                            rate = round(float(100 * fs.free ) / fs.capacity,1)
                        else:
                            rate = 0
                        if fs.status == 2:
                            status = 'RDONLY'
                        elif fs.status == 1:
                            status = 'DISABLED'
                        else:
                            status = ''
                        self.ok("\t%s %s CAPACITY %s FREE %s ( %.1f%%) %s" % (fs.server, fs.fs, self.prettySize(fs.capacity), self.prettySize(fs.free), rate, status))
                elif (pool.type == 'filesystem' and 'dpm2' not in sys.modules):
                    self.ok("\tCouldn't get any information about filesystems.")

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


class FsAddCommand(ShellCommand):
    """Add a filesystem.

Needs DPM-python to be installed. Please do 'yum install dpm-python'."""
    def _init(self):
        self.parameters = ['?filesystem name', '?pool name', '?server']

    def _execute(self, given):
        if self.interpreter.poolManager is None:
            return self.error('There is no pool manager.')

        if 'dpm2' not in sys.modules:
            return self.error("DPM-python is missing. Please do 'yum install dpm-python'.")

        try:
            pool = self.interpreter.poolManager.getPool(given[1])
            if (pool.type == 'filesystem'):
                status = 0
                weight = 1
                if not dpm2.dpm_addfs(pool.name, given[2], given[0], status, weight):
                    self.ok('Filesystem added')
                else:
                    self.error('Filesystem not added.')
            else:
                self.error('The pool is not compatible with filesystems.')
        except Exception, e:
            return self.error(e.__str__() + '\nParameter(s): ' + ', '.join(given))


class FsModifyCommand(ShellCommand):
    """Modify a filesystem.

Needs DPM-python to be installed. Please do 'yum install dpm-python'.

Valid attributes are status and weight.
Status must have one of the following values: 0, 1 or DISABLED, 2 or RDONLY.
Weight must be a positive integer (recommended <10). It is used during the filesystem selection.
"""
    def _init(self):
        self.parameters = ['?filesystem name', '?server', '?attribute', '?value']

    def _execute(self, given):
        if self.interpreter.poolManager is None:
            return self.error('There is no pool manager.')

        if 'dpm2' not in sys.modules:
            return self.error("DPM-python is missing. Please do 'yum install dpm-python'.")

        try:
            if given[2] == 'status':
                if given[3] == 'DISABLED':
                    status = 1
                elif given[3] == 'RDONLY':
                    status = 2
                else:
                    try:
                        status = int(given[3])
                    except:
                        return self.error('The value is not valid (0, 1 or DISABLED, 2 or RDONLY).')
                    if status not in [0,1,2]:
                        return self.error('The value is not valid (0, 1 or DISABLED, 2 or RDONLY).')
                weight = -1
            elif given[2] == 'weight':
                try:
                    weight = int(given[3])
                except:
                    return self.error('The value is not a valid integer.')
                if weight < 0:
                    return self.error('The value must be positive.')
                status = -1
            else:
                return self.error('Non-valid attribute.')
            if not dpm2.dpm_modifyfs(given[1], given[0], status, weight):
                return self.ok('Filesystem modified')
            else:
                return self.error('Filesystem not modified.')
        except Exception, e:
            return self.error(e.__str__() + '\nParameter(s): ' + ', '.join(given))


class FsDelCommand(ShellCommand):
    """Delete a filesystem.

Needs DPM-python to be installed. Please do 'yum install dpm-python'."""
    def _init(self):
        self.parameters = ['?filesystem name', '?server']

    def _execute(self, given):
        if self.interpreter.poolManager is None:
            return self.error('There is no pool manager.')

        if 'dpm2' not in sys.modules:
            return self.error("DPM-python is missing. Please do 'yum install dpm-python'.")

        try:
            if not dpm2.dpm_rmfs(given[1], given[0]):
                self.ok('Filesystem deleted')
            else:
                self.error('Filesystem deleted.')
        except Exception, e:
            return self.error(e.__str__() + '\nParameter(s): ' + ', '.join(given))
