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

import dmliteinterpreter


def main():
	# init dmlite shell
	init_history()
	
	# init interpreter
	interpreter = dmliteinterpreter.DMLiteInterpreter('/etc/dmlite.conf')
	print interpreter.result[0]
	if interpreter.result[1]: # initialisation failed
		return

	# init the shell auto completion functionality
	readline.set_completer_delims(' \t//')
	readline.set_completer((lambda text, state: interpreter.completer(readline.get_line_buffer(), state)))
	readline.parse_and_bind('tab: complete')
	
	# dmlite shell loop
	while not interpreter.result[2]:
		
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
		if interpreter.result[0] != '':	
			print interpreter.result[0]
		
		# in non-interactive mode an error stops execution
		if (not sys.__stdin__.isatty()) and interpreter.result[1]:
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
