import os
import time
import imp

from DIRAC import gLogger

from IHEPDIRAC.Badger.private.output.MergeFile import MergeFile

class GetOutputHandler(object):
  def __init__(self, lfnList, method, localValidation, useChecksum):
    self.__lfnList = lfnList

    if type(method) is list:
      self.__method = self.__decideAvailableMethod(method)
      gLogger.debug('Decide available method:', (method, self.__method))
    else:
      self.__method = method

    try:
      self.__createGetFile()
    except Exception, e:
      gLogger.error('CreateGetFile error:', self.__method)
      raise Exception('Method could not be imported: %s' % self.__method)

    self.__getFile.setUseChecksum(useChecksum)
    self.__getFile.setLocalValidation(localValidation)

    self.__mergeFile = MergeFile()

  def getMethod(self):
    return self.__method

  def getAvailNumber(self):
    return len(self.__lfnAvailList)

  def checkRemote(self):
    allRemoteAttributes = self.__getFile.getAllRemoteAttributes(self.__lfnList)
    self.__lfnAvailList = [lfn for lfn in self.__lfnList if lfn in allRemoteAttributes and allRemoteAttributes[lfn]]

  def download(self, downloadDir, downloadCallback=None):
    count = {}

    for lfn in self.__lfnAvailList:
      result = self.__getFile.getFile(lfn, downloadDir)
      if result['status'] in count:
        count[result['status']] += 1
      else:
        count[result['status']] = 1

      if downloadCallback is not None:
        downloadCallback(lfn, result)

    return count

  def downloadAndMerge(self, downloadDir, mergeDir, mergeName, mergeExt, mergeMaxSize, removeDownload,
                       downloadCallback=None, mergeCallback=None, removeCallback=None):
    if self.__getFile.directlyRead() and removeDownload:
      ret = self.__mergeFromRemote(mergeDir, mergeName, mergeExt, mergeMaxSize, mergeCallback)
      if removeDownload and ret:
        self.__removeLocalDownloaded(downloadDir, removeCallback)
    else:
      count = self.download(downloadDir, downloadCallback)
      if mergeMaxSize > 0:
        if 'error' not in count or count['error'] == 0:
          ret = self.__mergeFromLocal(downloadDir, mergeDir, mergeName, mergeExt, mergeMaxSize, mergeCallback)
          if removeDownload and ret:
            self.__removeLocalDownloaded(downloadDir, removeCallback)


  def __createGetFile(self):
    getFileClassName = ''.join(w.capitalize() for w in self.__method.split('_')) + 'GetFile'
    getFileModuleName = 'IHEPDIRAC.Badger.private.output.getfile.%s' % getFileClassName

    try:
      self.__getFile = self.__loadClass(getFileModuleName, getFileClassName)
    except ImportError, e:
      raise Exception('Could not find method %s' % self.__method)

  def __decideAvailableMethod(self, methodList):
    for method in methodList:
      getFileClassName = ''.join(w.capitalize() for w in method.split('_')) + 'GetFile'
      getFileModuleName = 'IHEPDIRAC.Badger.private.output.getfile.%s' % getFileClassName

      try:
        getFile = self.__loadClass(getFileModuleName, getFileClassName)
      except ImportError, e:
        gLogger.debug('Could not find method:', self.__method)
        continue

      if getFile.available():
        return method

    return 'dfc'


  def __mergeFromLocal(self, downloadDir, mergeDir, mergeName, mergeExt, mergeMaxSize, mergeCallback):
    mergeList = self.__lfnAvailList[:]
    localMergeList = [self.__getFile.lfnToLocal(downloadDir, lfn) for lfn in mergeList]
    localMergeList.sort()
    return self.__mergeFile.merge(localMergeList, mergeDir, mergeName, mergeExt, mergeMaxSize, mergeCallback)

  def __mergeFromRemote(self, mergeDir, mergeName, mergeExt, mergeMaxSize, mergeCallback):
    mergeList = self.__lfnAvailList[:]
    remoteMergeList = [self.__getFile.lfnToRemote(lfn) for lfn in mergeList]
    remoteMergeList.sort()
    return self.__mergeFile.merge(remoteMergeList, mergeDir, mergeName, mergeExt, mergeMaxSize, mergeCallback)

  def __removeLocalDownloaded(self, downloadDir, removeCallback):
    for lfn in self.__lfnAvailList:
      localPath = self.__getFile.lfnToLocal(downloadDir, lfn)
      if os.path.isfile(localPath):
        os.remove(localPath)
        if removeCallback is not None:
          removeCallback(localPath)

  def __loadClass(self, moduleName, className):
    try:
      m = __import__(moduleName, globals(), locals(), [className])
    except ImportError, e:
      raise Exception('Could not from %s import %s' % (moduleName, className))
    return getattr(m, className)()
