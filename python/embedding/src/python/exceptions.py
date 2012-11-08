class PyDmException(Exception):
  def __init__(self, *args):
    self.e = DmException(*args)

  def __getattr__(self, name):
    return self.e.__getattribute__(name)

  def __str__(self):
    return type(self).__name__ + ': ' + self.e.what()
