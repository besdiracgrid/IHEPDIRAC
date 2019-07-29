import os

from DIRAC import gConfig, gLogger

class LocalMount(object):
  def __init__(self):
    super(LocalMount, self).__init__()
    self._directlyRead = True

    self.__mountPoint = gConfig.getValue('/Resources/Applications/DataLocation/LocalMount/Prefix', '/dcache/bes')
    gLogger.debug('Mount point:', self.__mountPoint)

  def _available(self):
    return os.path.isdir(self.__mountPoint)

  def _lfnToRemote(self, lfn):
    return self.__mountPoint + lfn
