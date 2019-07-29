#!/usr/bin/env python
#mtime:2013/12/09
"""
besdirac-dms-get-files
  This script get a set of files from SE to localdir.

  Usage:
    besdirac-dms-get-files [-n|-f|-m] <Arguments>
    Arguments:
      datasetName: a dataset that contain a set of files.
      DFCDir: The logical dir in DFC. Download files under this dir.
      metequery: a set of query condition and download eligible files. It must be a string like "a=1 b=2"
    Examples:
      script -n name1
      script -r /zhanggang_test 
      script -m "runL>1111 runH<2345 bossVer=6.6.4" 
      script -f hello.txt
"""
__RCSID__ = "$Id$"

import time
from DIRAC import S_OK, S_ERROR, gLogger, exit
from DIRAC.Core.Base import Script

switches = [
    ("n:","datasetName=","a dataset that contain a set of files."),
    ("r:","DFCDir=","The logical dir in DFC."),
    ("m:","metequery=","a set of query condition"),
    ("f:","fileName=","a fileList in this file"),
            ]
for switch in switches:
  Script.registerSwitch(*switch)
Script.setUsageMessage(__doc__)
Script.parseCommandLine(ignoreErrors=True)

#args  = Script.getPositionalArgs()
args = Script.getUnprocessedSwitches()
if not args:
  Script.showHelp()
  exit(-1)
setNameFlag = False
dfcDirFlag = False
queryFlag = False
filenameFlag = False
for switch in args:
  if switch[0].lower() == "n" or switch[0].lower() == "datasetName":
    setNameFlag = True
    setName = switch[1]
  if switch[0].lower() == "r" or switch[0].lower() == "DFCDir":
    dfcDirFlag = True
    dfcDir = switch[1]
  if switch[0].lower() == "m" or switch[0].lower() == "metequery":
    queryFlag = True
    setQuery = switch[1]
  if switch[0].lower() == "f" or switch[0].lower() == "fileName":
    filenameFlag = True
    filename = switch[1]

from IHEPDIRAC.Badger.API.Badger import Badger
from IHEPDIRAC.Badger.API.multiworker import IWorker,MultiWorker
print "start download..."
start = time.time()


class DownloadWorker(IWorker):
  """
  """
  #if file failed download,then append in errorDict
  errorDict = {}

  def __init__(self):
    self.badger = Badger()
    if queryFlag:
      self.m_list = self.badger.getFilesByMetadataQuery(setQuery)
    elif setNameFlag:
      self.m_list = self.badger.getFilesByDatasetName(setName)
    elif dfcDirFlag:
      self.m_list = self.badger.listDir(dfcDir)
    elif filenameFlag:
      with open(filename) as f:
        for line in f:
          self.m_list.append(line.strip())

  def get_file_list(self):
    return self.m_list

  def Do(self, item):
    badger = Badger()
    result = badger.downloadFilesByFilelist([item])#,destDir)
    if not result['OK']:
      #print result['Message'],type(result['Message'])
      errorDict.update(result['Message'])
      print errorDict


dw = DownloadWorker()
#print dw.get_file_list()
mw = MultiWorker(dw,5)
mw.main()
total=time.time()-start
print "Finished,total time is %s"%total

exit(1)
