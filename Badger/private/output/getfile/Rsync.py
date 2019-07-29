import subprocess

from DIRAC import gLogger

class Rsync(object):
  def __init__(self):
    super(Rsync, self).__init__()

    # rsync could validate data by itself
#    self._localValidation = False

  def _downloadSingleFile(self, remotePath, localPath):
    gLogger.debug('rsync from %s to %s' % (remotePath, localPath))

    args = ['rsync', '-z', remotePath, localPath]
    p = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = p.communicate()
    ret = p.returncode

    return ret == 0
