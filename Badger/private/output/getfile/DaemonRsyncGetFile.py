import time
import subprocess

from DIRAC import gConfig, gLogger

from IHEPDIRAC.Badger.private.output.getfile.GetFile import GetFile
from IHEPDIRAC.Badger.private.output.getfile.Rsync   import Rsync

class DaemonRsyncGetFile(Rsync, GetFile):
  def __init__(self):
    super(DaemonRsyncGetFile, self).__init__()

    self.__rsyncUrl = gConfig.getValue('/Resources/Applications/DataLocation/RsyncEndpoints/Url', 'rsync://localhost/bes-srm')
    gLogger.debug('Rsync daemon url:', self.__rsyncUrl)

  def _available(self):
    args = ['rsync', self.__rsyncUrl]
    p = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = p.communicate()
    ret = p.returncode

    return ret == 0 and out

  def _lfnToRemote(self, lfn):
    return self.__rsyncUrl + lfn
