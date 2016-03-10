import os, sys, time, commands
from ConfigHandler import *

class ProcessManager:

  def __init__(self, config_path=""):
    if config_path=="":
      self._configPath = "config.xml"
    else:
      self._configPath = config_path
    self.config = getConfig(self._configPath)
    if self.config is not None:
      self._program = self.config.getProgram()
      self._command = self.config.getCommand()
      self._daemon = ""

  def is_available(self):
    return self.config is not None

  def __getProgramPid(self):
    result = commands.getoutput(
      "ps aux | grep %s | grep -v grep | awk '{print $2}'" % self._program)
    return result


  def __getDaemonPid(self):
    result = commands.getoutput("ps aux | grep python \
      | grep '%s monitor' | grep -v grep | awk '{print $2}'" % self._daemon)
    return result


  def start_program(self):
    if self.is_available():
      p_pid = self.__getProgramPid()
      if p_pid != '':
        print("It seems %s is already running..." % _program)
      else:
        print("starting program...")
        if os.system('nohup %s &' % _command) == 0:
          print("%s starts successfully and pid is %s" % (_program, self.__getProgramPid()))
        else:
          print("%s can't be started!" % _program)
    else:
      print ("Configuration is not available.")

  def stop_program(self):
    p_pid = self.__getProgramPid()
    if p_pid != '':
      os.system('kill ' + p_pid)
      print("Program %s stopped." % _program)
    else:
      print("It seems this program is not running...")

if __name__ == '__main__':

  manager = ProcessManager()
  if not manager.is_available():
    print("No available configuration!")
  else:
    if len(sys.argv)==2 :
      args = sys.argv[1]

      _command = config.getCommand()
      _program = config.getProgram()

      if args=="-start":
        manager.start_program()
      elif args=="-stop":
        manager.stop_program()

    else:
      print("wrong parameter number")
