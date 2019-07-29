import subprocess

from DIRAC import gLogger

from IHEPDIRAC.Badger.private.output.getfile.GetFile import GetFile
from IHEPDIRAC.Badger.private.output.getfile.LocalMount import LocalMount

class CpGetFile(LocalMount, GetFile):
  def __init__(self):
    super(CpGetFile, self).__init__()

  def _downloadSingleFile(self, remotePath, localPath):
    gLogger.debug('cp from %s to %s' % (remotePath, localPath))

    args = ['cp', remotePath, localPath]
    p = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = p.communicate()
    ret = p.returncode

    return ret == 0
