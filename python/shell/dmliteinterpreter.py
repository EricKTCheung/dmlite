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


class DMLiteInterpreter:
	"""
	A class taking commands as strings and passing them to DMLite via pydmlite.
	"""
	
	def __init__(self, ConfigFile):
		self.defaultConfigurationFile = ConfigFile
		self.lastCompleted = 0
		self.lastCompletedState = 0
		
		# collect all available commands into commands-array
		self.commands = []
		for (cname, cobj) in inspect.getmembers(sys.modules[__name__]):
			if inspect.isclass(cobj) and cname.endswith('Command') and cname != 'ShellCommand':
				self.commands.append(cobj())
		
		# execute init command
		self.execute('init')
		
	def execute(self, inputline):
		"""Execute the given command.
		Return value: (message, error, exit shell)
		Last return value can be found in self.return"""
		
		try:
			cmdline = shlex.split(inputline, True)
		except Exception, e:
			self.result = ('ERROR: Parsing error.', True, False)
			return self.result
		
		if not cmdline:
			# empty command
			self.result = ('', False, False)
			return self.result
		
		# search for command and execute it
		for c in self.commands:
			if c.name == cmdline[0]:
				self.result = c.checkSyntax(cmdline[1:], self)
				if self.result[1]: # syntax error occcurred!
					break
				self.result = c.execute(cmdline[1:], self)
				break
		else:
			# other, unknown command
			self.result = ('ERROR: Unknown command: ' + cmdline[0], True, False)
		
		return self.result
		
	def completer(self, text, state):
		""" Complete the given start of a command line."""
		
		if (self.lastCompleted != text) or (self.lastCompletedState > state):
			self.completionOptions = []
			self.lastCompleted = text
			self.lastCompletedState = state
			# check all commands if the provide completion options
			for c in self.commands:
				try:
					coptions = c.completer(text, self)
					self.completionOptions.extend(coptions)
				except Exception, e: # look out for errors!
					print e

		# return the correct option
		try:
			return self.completionOptions[state]
		except IndexError:
			return None
	
	def listDirectory(self, directory):
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
	def __init__(self):
		"""Initialisation for the command class. Do not override!"""

		# self.name contains the name of the command
		# this is automatically extracted from the class name
		self.name = ((self.__class__.__name__)[:-7]).lower()
		
		# self.description contains a short desc of the command
		# this is automatically extracted from the class __doc__
		self.description = self.__doc__
		
		# self.parameters contains a list of parameters in the syntax
		# *Tparameter name
		# where * is added for optional parameters and T defines the
		# type of the parameter (for automatic checks and completion):
		#	f local file/directory
		#	d DMLite file/directory
		#   c DMLite shell command
		#	? other, no checks done
		# Note: use uppercase letter for a check if file/command/.. exists
		self.parameters = []
		
		self.init()
	
	def init(self):
		"""Override this to set e.g. self.parameters."""
		pass
	
	def completer(self, start, interpreter):
		"""Return a list of possible tab-completions for the string start."""

		if self.name.startswith(start):
			# complete the command
			if len(self.parameters) == 0:
				return [self.name]
			else:
				return [self.name + ' ']
			
		elif start.startswith(self.name):
			# complete parameters, analyse the already given parameters
			try:
				given = shlex.split(start+'x', True)
			except Exception, e:
				return [] # parsing failed

			if len(given)-1 > len(self.parameters):
				# too many parameters given already
				return []
			else:
				# extract current parameter type
				ptype = self.parameters[len(given)-2]
				lastgiven = given[len(given)-1][:-1]
				if ptype.startswith('*'):
					ptype = ptype[1:2].lower()
				else:
					ptype = ptype[0:1].lower()
				
				if ptype == 'c': # command
					return list(c.name for c in interpreter.commands if c.name.startswith(lastgiven))
				elif ptype == 'f': # file or folder
					if lastgiven == '':
						lastgiven = './'
					gfolder, gfilestart = os.path.split(lastgiven)
					groot, gdirs, gfiles = os.walk(gfolder).next()
					gfiles = gfiles + list((d + '/') for d in gdirs)
					return list(f for f in gfiles if f.startswith(gfilestart))
				elif ptype == 'd': # dmlite file or folder
					gfolder, lastgiven = os.path.split(lastgiven)
					if gfolder == '':
						gfiles = interpreter.listDirectory(interpreter.catalog.getWorkingDir())
					else:
						gfiles = interpreter.listDirectory(gfolder)
					if gfiles == -1: # listing failed
						return []
					l = list(f['name'] + ('','/')[f['isDir']] for f in gfiles if f['name'].startswith(lastgiven))
					return l
				else:
					return []
		else:
			# no auto completions from this command
			return []
	
	def shortDescription(self):	
		if self.description.find('\n'):
			return self.description[0:self.description.find('\n')]
		else:
			return self.description

	def help(self):
		"""Returns a little help text with the description of the command."""
		return ' ' + self.name + (' ' * (14 - len(self.name))) + self.shortDescription()
	
	def moreHelp(self):
		"""Returns the syntax description of the command and a help text."""
		return self.name + ' ' + ' '.join(self.prettyParameter(p) for p in self.parameters) + '\n' + self.description
	
	def prettyParameter(self, parameter):
		"""Return the human readable format of a parameter"""
		if parameter.startswith('*'):
			return '[ ' + self.prettyParameter(parameter[1:]) + ' ]'
		return '<' + parameter[1:] + '>'
	
	def checkSyntax(self, given, interpreter):
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
				if given[i] not in list(c.name for c in interpreter.commands):
					return self.syntaxError('Unknown command "' + given[i] + '".')
			elif ptype == 'F':
				if not os.path.exists(given[i]):
					return self.syntaxError('File "' + given[i] + '" does not exist.')
			elif ptype == 'D':
				# check if file exists in DMLite
				try:
					f = interpreter.catalog.extendedStat(given[i], True)
				except Exception, e:
					return self.syntaxError('File "' + given[i] + '" does not exist.')
				
		return self.ok()
	
	def execute(self, given, interpreter):
		return self.ok('Execute stub for ' + self.name + '...')
		
	def ok(self, msg = ''):
		return (msg, False, False)
		
	def error(self, error, msg = ''):
		if msg == '':
			return ('ERROR: ' + error, True, False)
		else:
			return (msg + '\nERROR: ' + error, True, False)
		
	def syntaxError(self, msg = 'Bad syntax.' ):
		return ('ERROR: ' + msg + '\nExpected syntax is: ' + self.moreHelp(), True, False)
		
	def exitShell(self, msg = ''):
		return (msg, False, True)


class ExitCommand(ShellCommand):
	"""Exits the DMLite shell."""
	def execute(self, given, interpreter):
		return self.exitShell()


class HelpCommand(ShellCommand):
	"""Prints a help text or descriptions for single commands."""
	def init(self):
		self.parameters = ['*ccommand']
		
	def execute(self, given, interpreter):
		if len(given) == 0:
			ch = []
			for c in interpreter.commands:
				ch.append(c.help())
			return ('\n'.join(ch), False, False)
			
		else:
			ch = list(c.moreHelp() for c in interpreter.commands if c.name.startswith(given[0]))
			if not ch:
				return self.error('Command "' + given[0] + '" not found.')
			else:
				return self.ok('\n\n'.join(ch))


class InitCommand(ShellCommand):
	"""Initialises the DMLite shell with a given configuration file."""
	def init(self):
		self.parameters = ['*fconfiguration file']
		
	def execute(self, given, interpreter):
		
		# if config file parameter not given, use default one
		given.append(interpreter.defaultConfigurationFile)		
		
		# initialise the necessary dmlite objects with the configuration file
		if not os.path.isfile(given[0]):
			return self.syntaxError('Configuration file "' + given[0] + '" is not a valid file.')
		interpreter.configurationFile = given[0]
		
		# check the existance of the pydmlite library
		try:
			interpreter.API_VERSION = pydmlite.API_VERSION
			message = 'DMLite shell v0.1 (using DMLite API v' + str(interpreter.API_VERSION) + ')'
		except Exception, e:
			return self.error('Could not import the Python module pydmlite.\nThus, no bindings for the DMLite library are available.')
		
		try:
			interpreter.pluginManager = pydmlite.PluginManager()
			interpreter.pluginManager.loadConfiguration(interpreter.configurationFile)
		except Exception, e:
			return self.error('Could not initialise PluginManager with file "' + interpreter.configurationFile + '".\n' + e.__str__(), message)
		
		try:
			interpreter.securityContext = pydmlite.SecurityContext()
			group = pydmlite.GroupInfo()
			group.name = "root"
			group.setUnsigned("gid", 0)
			interpreter.securityContext.user.setUnsigned("uid", 0)
			interpreter.securityContext.groups.append(group)
		except Exception, e:
			return self.error('Could not initialise root SecurityContext.\n' + e.__str__(), message)
		
		try:
			interpreter.stackInstance = pydmlite.StackInstance(interpreter.pluginManager)
			interpreter.stackInstance.setSecurityContext(interpreter.securityContext)
		except Exception, e:
			return self.error('Could not initialise a StackInstance.\n' + e.__str__(), message)
		
		try:
			interpreter.catalog = interpreter.stackInstance.getCatalog()
			interpreter.catalog.changeDir('/')
		except Exception, e:
			return self.error('Could not initialise a the file catalog.\n' + e.__str__(), message)

		return self.ok(message + '\nUsing configuration "' + interpreter.configurationFile + '" as root.')
		

class PwdCommand(ShellCommand):
	"""Prints the current directory."""
	def execute(self, given, interpreter):
		return self.ok(interpreter.catalog.getWorkingDir())

		
class CdCommand(ShellCommand):
	"""Changes the current directory."""
	def init(self):
		self.parameters = ['Ddirectory']
		
	def execute(self, given, interpreter):
		# change directory
		try:
			interpreter.catalog.changeDir(given[0])
		except Exception, e:
			return self.error(e.__str__() + given[0])
		return self.ok()


class LsCommand(ShellCommand):
	"""Lists the content of the current directory."""
	def init(self):
		self.parameters = ['*Ddirectory']
		pass
	
	def execute(self, given, interpreter):
		# if no parameters given, list current directory
		given.append(interpreter.catalog.getWorkingDir())
		
		flist = interpreter.listDirectory(given[0])
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
	def init(self):
		self.parameters = ['ddirectory']
		
	def execute(self, given, interpreter):
		try:
			interpreter.catalog.makeDir(given[0], 0777)
		except Exception, e:
			return self.error(e.__str__() + given[0])
		return self.ok()


class UnlinkCommand(ShellCommand):
	"""Removes a file."""
	def init(self):
		self.parameters = ['Dfile']
		
	def execute(self, given, interpreter):
		try:
			interpreter.catalog.unlink(given[0])
		except Exception, e:
			return self.error(e.__str__() + given[0])
		return self.ok()


class RmDirCommand(ShellCommand):
	"""Removes a directory."""
	def init(self):
		self.parameters = ['Ddirectory']
		
	def execute(self, given, interpreter):
		try:
			interpreter.catalog.removeDir(given[0])
		except Exception, e:
			return self.error(e.__str__() + given[0])
		return self.ok()


class MvCommand(ShellCommand):
	"""Moves or renames a file."""
	def init(self):
		self.parameters = ['Dsource-file', 'ddestination-file']
		
	def execute(self, given, interpreter):
		try:
			interpreter.catalog.rename(given[0], given[1])
		except Exception, e:
			return self.error(e.__str__() + given[0] + ' / ' + given[1])
		return self.ok()


class DuCommand(ShellCommand):
	"""Determines the disk usage of a file or a directory."""
	def init(self):
		self.parameters = ['*Dfile']
		
	def execute(self, given, interpreter):
		# if no parameters given, list current directory
		given.append(interpreter.catalog.getWorkingDir())
		
		f = interpreter.catalog.extendedStat(given[0], True)
		if f.stat.isDir():
			return self.ok(str(self.folderSize(given[0], interpreter)) + 'B')
		else:
			return self.ok(str(f.stat.st_size) + 'B')
		
	def folderSize(self, folder, interpreter):
		gfiles = interpreter.listDirectory(folder)
		size = 0
		for f in gfiles:
			print f['name']
			if f['isDir']:
				size += self.folderSize(os.path.join(folder, f['name']), interpreter)
			else:
				size += f['size']
		return size


class LnCommand(ShellCommand):
	"""Creates a symlink."""
	def init(self):
		self.parameters = ['Dtarget-file', 'dsymlink-file']
		
	def execute(self, given, interpreter):
		try:
			interpreter.catalog.symlink(given[0], given[1])
		except Exception, e:
			return self.error(e.__str__() + given[0] + ' / ' + given[1])
		return self.ok()


class CommentCommand(ShellCommand):
	"""Sets and reads file comments.\nPut comment in quotes. Reset file comment via comment <file> ""."""
	def init(self):
		self.parameters = ['Dfile', '*?comment']
		
	def execute(self, given, interpreter):
		if len(given) == 2:
			try:
				interpreter.catalog.setComment(given[0], given[1])
			except Exception, e:
				return self.error(e.__str__() + given[0] + ' / ' + given[1])
			return self.ok()
		else:
			try:
				return self.ok(interpreter.catalog.getComment(given[0]))
			except Exception, e:
				return self.ok(' ') # no comment
