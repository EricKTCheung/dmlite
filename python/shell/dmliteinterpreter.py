# dmliteinterpreter.py

try:
  import pydmlite
except:
  pass
import shlex
import os
import pipes
import inspect
import sys
import re
import time
import dateutil.parser


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
          if f.stat.st_size < 1024:
            finfo['prettySize'] = str(f.stat.st_size) + 'B '
          elif f.stat.st_size < 1024*1024:
            finfo['prettySize'] = '%.1fkB' % (f.stat.st_size / 1024.)
          elif f.stat.st_size < 1024*1024*1024:
            finfo['prettySize'] = '%.1fMB' % (f.stat.st_size / 1024. / 1024.)
          elif f.stat.st_size < 1024*1024*1024*1024:
            finfo['prettySize'] = '%.1fGB' % (f.stat.st_size / 1024. / 1024. / 1024.)
          elif f.stat.st_size < 1024*1024*1024*1024*1024:
            finfo['prettySize'] = '%.1fTB' % (f.stat.st_size / 1024. / 1024. / 1024. / 1024.)
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
            Sfinfo['comment'] = ''

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
    return ' ' + self.name + (' ' * (12 - len(self.name))) + self.shortDescription
  
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
        # check if a valid group is given
        try:
          if given[i] not in (g.name for g in self.interpreter.authn.getGroups()):
            return self.syntaxError('Group name expected.')
        except Exception, e:
          pass
      elif ptype == 'U':
        # check if a valid user name is given
        try:
          if given[i] not in (g.name for g in self.interpreter.authn.getUsers()):
            return self.syntaxError('User name expected.')
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


class ExitCommand(ShellCommand):
  """Exits the DMLite shell."""
  def _execute(self, given):
    return self.exitShell()


class HelpCommand(ShellCommand):
  """Prints a help text or descriptions for single commands."""
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


class InitCommand(ShellCommand):
  """Initialises the DMLite shell with a given configuration file."""
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
        self.ok('DMLite shell v0.1 (using DMLite API v' + str(self.interpreter.API_VERSION) + ')')
    except Exception, e:
      return self.error('Could not import the Python module pydmlite.\nThus, no bindings for the DMLite library are available.')
    
    try:
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
      return self.error('Could not initialise the authentication interface.\n' + e.__str__())

    try:
      self.interpreter.poolManager = self.interpreter.stackInstance.getPoolManager()
    except Exception, e:
      self.interpreter.poolManager = None
      self.error('Could not initialise the pool manager.\n' + e.__str__())

    if not self.interpreter.quietMode:
      self.ok('Using configuration "' + self.interpreter.configurationFile + '" as root.')
    else:
      return self.ok()
    

class PwdCommand(ShellCommand):
  """Prints the current directory."""
  def _execute(self, given):
    return self.ok(self.interpreter.catalog.getWorkingDir())


class CdCommand(ShellCommand):
  """Changes the current directory."""
  def _init(self):
    self.parameters = ['Ddirectory']
    
  def _execute(self, given):
    # change directory
    try:
      self.interpreter.catalog.changeDir(given[0])
    except Exception, e:
      return self.error(e.__str__() + given[0])
    return self.ok()


class LsCommand(ShellCommand):
  """Lists the content of the current directory."""
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
  """Creates a new directory."""
  def _init(self):
    self.parameters = ['ddirectory']
    
  def _execute(self, given):
    try:
      self.interpreter.catalog.makeDir(given[0], 0777)
    except Exception, e:
      return self.error(e.__str__() + given[0])
    return self.ok()


class UnlinkCommand(ShellCommand):
  """Removes a file."""
  def _init(self):
    self.parameters = ['Dfile']
    
  def _execute(self, given):
    try:
      self.interpreter.catalog.unlink(given[0])
    except Exception, e:
      return self.error(e.__str__() + given[0])
    return self.ok()


class RmDirCommand(ShellCommand):
  """Removes a directory."""
  def _init(self):
    self.parameters = ['Ddirectory']
    
  def _execute(self, given):
    try:
      self.interpreter.catalog.removeDir(given[0])
    except Exception, e:
      return self.error(e.__str__() + given[0])
    return self.ok()


class MvCommand(ShellCommand):
  """Moves or renames a file."""
  def _init(self):
    self.parameters = ['Dsource-file', 'ddestination-file']
    
  def _execute(self, given):
    try:
      self.interpreter.catalog.rename(given[0], given[1])
    except Exception, e:
      return self.error(e.__str__() + given[0] + ' / ' + given[1])
    return self.ok()


class DuCommand(ShellCommand):
  """Determines the disk usage of a file or a directory."""
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
  """Creates a symlink."""
  def _init(self):
    self.parameters = ['Dtarget-file', 'dsymlink-file']
    
  def _execute(self, given):
    try:
      self.interpreter.catalog.symlink(given[0], given[1])
    except Exception, e:
      return self.error(e.__str__() + given[0] + ' / ' + given[1])
    return self.ok()


class ReadLinkCommand(ShellCommand):
  """Shows the target of a symlink."""
  def _init(self):
    self.parameters = ['Dsymlink']
    
  def _execute(self, given):
    try:
      return self.ok(self.interpreter.catalog.readLink(given[0]))
    except Exception, e:
      return self.error(e.__str__() + given[0])


class CommentCommand(ShellCommand):
  """Sets or reads file comments.
  Put comment in quotes. Reset file comment via comment <file> ""."""
  def _init(self):
    self.parameters = ['Dfile', '*?comment']
    
  def _execute(self, given):
    if len(given) == 2:
      try:
        self.interpreter.catalog.setComment(given[0], given[1])
      except Exception, e:
        return self.error(e.__str__() + given[0] + ' / ' + given[1])
      return self.ok()
    else:
      try:
        return self.ok(self.interpreter.catalog.getComment(given[0]))
      except Exception, e:
        return self.ok(' ') # no comment

class InfoCommand(ShellCommand):
  """Prints all available information about a file."""
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
        uname = '???'
      self.ok('User ID:    ' + str(f.stat.st_uid) + ' (' + uname + ')')

      try:
        gid = pydmlite.boost_any()
        gid.setUnsigned(f.stat.st_gid)
        g = self.interpreter.authn.getGroup('gid', gid)
        gname = g.name
      except Exception, e:
        gname = '???'
      self.ok('Group ID:   ' + str(f.stat.st_gid) + ' (' + gname + ')')
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
      
      if not f.acl:
        self.ok('ACL:        Empty')
      else:
        self.ok('ACL:        ' + f.acl.serialize())
      
      return self.ok(' ')
      
    except Exception, e:
      return self.error(e.__str__())


class CreateCommand(ShellCommand):
  """Creates a new file."""
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
      return self.error(e.__str__() + given[0])
    return self.ok()


class ChModCommand(ShellCommand):
  """Changes the mode of a file."""
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
      return self.error(e.__str__() + given[0])
    return self.ok()


class VersionCommand(ShellCommand):
  """Prints the DMLite API Version."""
  def _execute(self, given):
    return self.ok(str(self.interpreter.API_VERSION))


class ChecksumCommand(ShellCommand):
  """Sets or reads file checksums."""
  def _init(self):
    self.parameters = ['Dfile', '*?checksum', '*?checksumtype']
    
  def _execute(self, given):
    if len(given) == 1:
      try:
        f = self.interpreter.catalog.extendedStat(given[0], False)
        return self.ok(str(f.csumtype) + ': ' + str(f.csumvalue))
      except Exception, e:
        return self.error(e.__str__() + given[0])
    else:
      given.append('')
      try:
        self.interpreter.catalog.setChecksum(given[0], given[1], given[2])
        return self.ok()
      except Exception, e:
        return self.error(e.__str__() + given[0] + ' / ' + given[1] + ' / ' + given[2])


class UtimeCommand(ShellCommand):
  """Sets the access and modification time of a file.
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
      return self.error(e.__str__() + given[0] + ' / ' + given[1] + ' / ' + given[2])


class ACLCommand(ShellCommand):
  """Sets or reads the ACL of a file."""
  def _init(self):
    self.parameters = ['Dfile', '*?ACL']
    
  def _execute(self, given):
    if len(given) == 1:
      try:
        f = self.interpreter.catalog.extendedStat(given[0], False)
        return self.ok(f.acl.serialize())
      except Exception, e:
        return self.error(e.__str__() + given[0])
    else:
      try:
        myacl = pydmlite.Acl(given[1])
        self.interpreter.catalog.setAcl(given[0], myacl)
        return self.ok()
      except Exception, e:
        return self.error(e.__str__() + given[0] + ' / ' + given[1])


class AddReplicaCommand(ShellCommand):
  """Adds a new replica for a file."""
  def _init(self):
    self.parameters = ['Dfile', 'Ostatus:available:beingPopulated:toBeDeleted', 'otype:volatile:permanent', '?rfn', '*?server']
    
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
      return self.error(e.__str__() + given[0])
    
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
      return self.error(e.__str__() + given[0])


class RmReplicaCommand(ShellCommand):
  """Deletes a replica for a file."""
  def _init(self):
    self.parameters = ['Dfile', '?replica-id | rfn']
    
  def _execute(self, given):
    try:
      self.interpreter.catalog.getReplica
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
      return self.error(e.__str__() + given[0])


class ChReplicaCommand(ShellCommand):
  """Updates the replica of a file."""
  def _init(self):
    self.parameters = ['Dfile', '?replica-id | rfn', 'Onew-status:available:beingPopulated:toBeDeleted', 'onew-type:volatile:permanent', '?new-rfn', '*?new-server']
    
  def _execute(self, given):
    try:
      self.interpreter.catalog.getReplica
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


class SetGuidCommand(ShellCommand):
  """Sets the GUID of a file."""
  def _init(self):
    self.parameters = ['Dfile', '?GUID']
    
  def _execute(self, given):
    try:
      self.interpreter.catalog.setGuid(given[0], given[1])
      return self.ok()
    except Exception, e:
      return self.error(e.__str__())


class GroupInfoCommand(ShellCommand):
  """Lists all groups or information about one group."""
  def _init(self):
    self.parameters = ['*Ggroup']
  
  def _execute(self, given):
    if given:
      try:
        g = self.interpreter.authn.getGroup(given[0])
        self.ok('Group: ' + g.name)
        self.ok(g.serialize())
      except Exception, e:
        return self.error(e.__str__() + ' ' + given[0])
    else:
      try:
        groups = self.interpreter.authn.getGroups()
        for g in groups:
          gid = '-'
          info = ''
          try:
            gid = g.getString('gid')
          except Exception, e:
            pass

          try:
            if g.getBool('banned'):
              info = ' (BANNED)'
          except Exception, e:
            pass
          
          self.ok( ' '*(8-len(gid)) + gid + '  ' + str(g.name) + info)
          
        self.ok(' ')
        
      except Exception, e:
        return self.error(e.__str__())


class UserInfoCommand(ShellCommand):
  """Lists all users or information about one user."""
  def _init(self):
    self.parameters = ['*Uuser']
  
  def _execute(self, given):
    if given:
      try:
        u = self.interpreter.authn.getUser(given[0])
        self.ok('User: ' + u.name)
        self.ok(u.serialize())
      except Exception, e:
        return self.error(e.__str__() + ' ' + given[0])
    else:
      try:
        users = self.interpreter.authn.getUsers()
        for u in users:
          uid = '-'
          info = ''
          try:
            uid = u.getString('uid')
          except Exception, e:
            pass

          try:
            if u.getString('ca'):
              info = ' (' + u.getString('ca') + ')'
          except Exception, e:
            pass

          try:
            if u.getBool('banned'):
              info = info + ' (BANNED)'
          except Exception, e:
            pass
          
          self.ok( ' '*(8-len(uid)) + uid + '  ' + str(u.name) + info)
          
        self.ok(' ')
        
      except Exception, e:
        return self.error(e.__str__())


class ChOwnCommand(ShellCommand):
  """Changes the owner of a file."""
  def _init(self):
    self.parameters = ['Dfile', 'Uuser']
  
  def _execute(self, given):
    try:
      f = self.interpreter.catalog.extendedStat(given[0], False)
      gid = f.stat.st_gid
      uid = self.interpreter.authn.getUser(given[1]).getUnsigned('uid')
      self.interpreter.catalog.setOwner(given[0], uid, gid, False)
    except Exception, e:
      return self.error(e.__str__())


class ChGrpCommand(ShellCommand):
  """Changes the group of a file."""
  def _init(self):
    self.parameters = ['Dfile', 'Ggroup']
  
  def _execute(self, given):
    try:
      f = self.interpreter.catalog.extendedStat(given[0], False)
      uid = f.stat.st_uid
      gid = self.interpreter.authn.getGroup(given[1]).getUnsigned('gid')
      self.interpreter.catalog.setOwner(given[0], uid, gid, False)
    except Exception, e:
      return self.error(e.__str__())

class PoolsCommand(ShellCommand):
  """List the pools."""

  mapping = {''  : pydmlite.PoolAvailability.kAny,
             'rw': pydmlite.PoolAvailability.kForBoth,
             'r' : pydmlite.PoolAvailability.kForRead,
             'w' : pydmlite.PoolAvailability.kForWrite,
             '!' : pydmlite.PoolAvailability.kNone}

  def _init(self):
    self.parameters = ['*Oavailability:rw:r:w:!']

  def _execute(self, give):
    if self.interpreter.poolManager is None:
      return self.error('There is no pool manager')

    try:
      if (len(give) > 0):
        availability = PoolsCommand.mapping[give[0]]
      else:
        availability = PoolsCommand.mapping['']
      pools = self.interpreter.poolManager.getPools(availability)
      for pool in pools:
        self.ok("%s (%s)\n\t%s" % (pool.name, pool.type, pool.serialize()))
      return self.ok()
    except Exception, e:
      return self.error(e.__str__())

