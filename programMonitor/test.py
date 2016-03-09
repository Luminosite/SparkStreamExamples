import os, sys, time, commands
from ConfigHandler import *

_configPath = "config.xml"
_program = ""
_command = ""
_daemon = ""

def getProgramPid():
	result = commands.getoutput(
		"ps aux | grep %s | grep -v grep | awk '{print $2}'" % _program)
	return result

def getDaemonPid():
	result = commands.getoutput("ps aux | grep python \
		| grep '%s monitor' | grep -v grep | awk '{print $2}'" % _daemon)
	return result

def startProgram():
	p_pid = getProgramPid()
	if p_pid != '':
		print("It seems %s is already running..." % _program)
	else:
		print("starting program...")
		if os.system('nohup %s &' % _command) == 0:
			print("%s starts successfully and pid is %s" % (_program, getProgramPid()))
		else:
			print("%s can't be started!" % _program)

def stopProgram():
	p_pid = getProgramPid()
	if p_pid != '':
		os.system('kill '+p_pid)
		print("Program %s stopped." % _program)
	else:
		print("It seems this program is not running...")



if __name__ == '__main__':
	config = getConfig(_configPath)
	if not config is None:
		_command = config.getCommand()
		_program = config.getProgram()
		startProgram()
		stopProgram()
