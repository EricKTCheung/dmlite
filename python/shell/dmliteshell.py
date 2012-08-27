#! /usr/bin/env python
# dmliteshell.py

"""
This file implements the DMLite shell.
The shell provides an easy access to many features of the DMLite library
via the Python module pydmlite.
"""


import os
import sys
import readline
import atexit
import ConfigParser
from optparse import OptionParser

import dmliteinterpreter


def main():
	
	# parse options
	parser = OptionParser("%prog [options]")
	parser.add_option("-c", "--config", dest="configfile", help="file name of the configuration file;\ndefault value is /etc/dmlite.conf", default = '/etc/dmlite.conf')
	parser.add_option("-e", "--execute", dest="command", help="execute single command and exit", default = '')
	parser.add_option("-s", "--script", dest="scriptfile", help="execute the given script file line by line.", default = '')
	(options, args) = parser.parse_args()
	if args:
		parser.error("Incorrect number of arguments")
    
	# init interpreter
	quietMode = (options.command) or (options.scriptfile) or (not sys.__stdin__.isatty())
	interpreter = dmliteinterpreter.DMLiteInterpreter(sys.stdout.write, options.configfile, quietMode)
	if interpreter.failed: # initialisation failed
		return

	if options.command:
		interpreter.execute(options.command)

	elif options.scriptfile:
		print 'executing script: ' + options.scriptfile
	
	else:
		# init dmlite shell
		init_history()
		
		# init the shell auto completion functionality
		readline.set_completer_delims(' \t//')
		readline.set_completer((lambda text, state: interpreter.completer(readline.get_line_buffer(), state)))
		readline.parse_and_bind('tab: complete')
		
		# dmlite shell loop
		while not interpreter.exit:
			
			# get next command
			try:
				if sys.__stdin__.isatty():
					cmdline = raw_input('> ')
				else:
					cmdline = raw_input()
				cmdline = cmdline.strip()
			except EOFError:
				# all commands from input have been executed, exit...
				break 
			except KeyboardInterrupt:
				# on CTRL-C
				if readline.get_line_buffer() == '':
					# exit if CTRL-C on empty line
					break
				else:
					print
					continue
			
			# execute next command, result = (message, error, exit shell)
			interpreter.execute(cmdline)
			
			# in non-interactive mode an error stops execution
			if (not sys.__stdin__.isatty()) and interpreter.failed:
				break
	
	# exit... clear line
	print


def init_history():
	# init the shell history functionality
	# the history is saved in ~/.dmlite_history
	history_file = os.path.join(os.path.expanduser('~'), '.dmlite_history')
	try:
	    readline.read_history_file(history_file)
	except IOError:
	    pass

	atexit.register(readline.write_history_file, history_file)


# this is an executable module
if __name__ == '__main__':
    main()
