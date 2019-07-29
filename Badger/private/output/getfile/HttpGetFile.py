import os
import urllib
import urllib2
import shutil

from DIRAC import gConfig, gLogger

from IHEPDIRAC.Badger.private.output.getfile.GetFile import GetFile

class HttpGetFile(GetFile):
  def __init__(self):
    super(HttpGetFile, self).__init__()

    self.__url = gConfig.getValue('/Resources/Applications/DataLocation/Http/Url', 'http://bes-srm.ihep.ac.cn:2880/bes')
    gLogger.debug('HTTP url:', self.__url)

  def _lfnToRemote(self, lfn):
    return self.__url + lfn

  def _available(self):
    try:
      req = urllib2.urlopen(urllib.quote(self.__url, '/:'))
    except Exception, e:
      return False
    return True

  def _downloadSingleFile(self, remotePath, localPath):
    gLogger.debug('getfile from %s to %s' % (remotePath, localPath))
    try:
      req = urllib2.urlopen(urllib.quote(remotePath, '/:'))
    except Exception, e:
      gLogger.debug('HTTP get file error:', e)
      return False

    with open(localPath, 'wb') as fp:
      shutil.copyfileobj(req, fp)
    return True
