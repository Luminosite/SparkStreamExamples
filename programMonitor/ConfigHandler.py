import xml.sax


class ConfigHandler(xml.sax.ContentHandler):
  def __init__(self):
    self.configMark = False
    self.skip = False
    self.config = Config()
    self.content = ""
    self.dataType = ""

  def startElement(self, tag, attr):
    if self.skip:
      self.config = None
    elif not self.configMark:
      if tag == "runnerConfig":
        self.configMark = True
      else:
        self.skip = True
    else:
      self.dataType = tag

  def endElement(self, tag):
    if self.skip:
      self.config = None
    elif self.dataType == "command":
      self.config.setCommand(self.content.strip())
    elif self.dataType == "program":
      self.config.setProgram(self.content.strip())
    self.dataType = ""
    self.content = ""

  def characters(self, content):
    if self.skip:
      self.config = None
    else:
      self.content += content

  def getConfig(self):
    return self.config


class Config:
  def __init__(self):
    self.run = ""
    self.deamon = ""
    self.program = ""

  def getProgram(self):
    return self.program

  def setProgram(self, program):
    self.program = program

  def getCommand(self):
    return self.run

  def setCommand(self, command):
    self.run = command


def getConfig(path):
  parser = xml.sax.make_parser()
  # turn off namespaces
  parser.setFeature(xml.sax.handler.feature_namespaces, 0)

  handler = ConfigHandler()
  parser.setContentHandler(handler)

  parser.parse(path)

  return handler.getConfig()


if __name__ == "__main__":
  config = getConfig("config.xml")
  if not config is None:
    print(config.getCommand(), "d")
