import os

from DIRAC import gLogger
from DIRAC.Interfaces.API.Dirac import Dirac

from IHEPDIRAC.Badger.private.output.getfile.GetFile import GetFile

class DfcGetFile(GetFile):
  def __init__(self):
    super(DfcGetFile, self).__init__()

  def _available(self):
    return True

  def _downloadSingleFile(self, remotePath, localPath):
    gLogger.debug('getfile from %s to %s' % (remotePath, localPath))
    dirac = Dirac()
    result = dirac.getFile(remotePath, os.path.dirname(localPath))
    return result['OK']
