import os
import time

import subprocess

from DIRAC import gLogger

class MergeFile(object):
  def __init__(self):
    self._directlyRead = False
    self._localValidation = True

  def merge(self, fileList, outputDir, mergeName, mergeExt, mergeMaxSize, mergeCallback):
    allFileSize = self.__getAllFileSize(fileList)

    count = 0
    tempSize = 0
    tempList = []
    for i in range(len(fileList)):
      fn = fileList[i]
      tempSize += allFileSize[fn]
      tempList.append(fn)
      if i == len(fileList) - 1 or tempSize > mergeMaxSize or tempSize + allFileSize[fileList[i+1]] > mergeMaxSize:
        count += 1
        mergePath = os.path.join(outputDir, '%s_%04d%s' % (mergeName, count, mergeExt))

        startTime = time.time()
        ret = self.__doMerge(tempList, mergePath)
        endTime = time.time()

        if mergeCallback is not None:
          mergeCallback(tempList, mergePath, tempSize, endTime-startTime, ret)
        if not ret:
          return False

        tempSize = 0
        tempList = []

    return True

  def __doMerge(self, fileList, mergePath):
    if len(fileList) == 0:
      gLogger.error('Can not merge empty file list!')
      return False

    try:
      args = ['hadd', mergePath] + fileList
      p = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
      out, err = p.communicate()
      ret = p.returncode
    except Exception, e:
      gLogger.debug('hadd error:', e)
      gLogger.error('Command "hadd" not found. Can not merge files. Please check your environment!')
      return False

    return ret == 0

  def __getAllFileSize(self, fileList):
    allFileSize = {}
    for fn in fileList:
      allFileSize[fn] = os.path.getsize(fn)
    return allFileSize
