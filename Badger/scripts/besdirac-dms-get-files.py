#!/usr/bin/env python
#mtime:2013/12/10
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
"""
__RCSID__ = "$Id$"

import os
import anydbm
import time
from DIRAC import S_OK, S_ERROR, gLogger, exit
from DIRAC.Core.Base import Script

switches = [
    ("n:","datasetName=","a dataset that contain a set of files."),
    ("r:","DFCDir=","The logical dir in DFC."),
    ("m:","metequery=","a set of query condition"),
            ]

for switch in switches:
  Script.registerSwitch(*switch)
Script.setUsageMessage(__doc__)
Script.parseCommandLine(ignoreErrors=True)

args = Script.getUnprocessedSwitches()
if not args:
  Script.showHelp()
  exit(1)
setNameFlag = False
dfcDirFlag = False
queryFlag = False
for switch in args:
  if switch[0].lower() == "n" or switch[0].lower() == "datasetName":
    setNameFlag = True
    setName = switch[1]
  elif switch[0].lower() == "r" or switch[0].lower() == "DFCDir":
    dfcDirFlag = True
    dfcDir = switch[1]
  elif switch[0].lower() == "m" or switch[0].lower() == "metequery":
    queryFlag = True
    setQuery = switch[1]

from IHEPDIRAC.Badger.API.Badger import Badger
from IHEPDIRAC.Badger.API.multiworker import IWorker,MultiWorker

def getDB(name,function):
  """return a db instance,the db contain the file list.
  default value is 0,means the file is not transfer yet,if 2,means OK.
  """
  dbname = "db"+name[-4:]
  if not os.path.exists(dbname):
    result = function(name)
    if result['OK']:
      fileList = result['Value']
      db = anydbm.open(dbname,'c')
      for file in fileList:
        db[file] = '0'
        db.sync()
  else:
    db = anydbm.open(dbname,'c')
  return (db,dbname)

print "start download..."
start = time.time()

class DownloadWorker(IWorker):
  """
  """
  #if file failed download,then append in errorDict
  #self.errorDict = {}

  def __init__(self):
    self.badger = Badger()
    if queryFlag:
      self.db,self.dbName = getDB(setQuery,self.badger.getFilesByMetadataQuery)
      #print self.db,self.dbName
    elif setNameFlag:
      self.db,self.dbName = getDB(setName,self.badger.getFilesByDatasetName)
      #print self.db,self.dbName
    elif dfcDirFlag:
      self.db,self.dbName = getDB(dfcDir,self.badger.listDir) 
      #print self.db,self.dbName

  def get_file_list(self):
    #return self.m_list
    for k,v in self.db.iteritems():
      if v=='2':
        continue
      yield k
      #print k

  def Do(self, item):
    badger = Badger()
    #print "world"
    result = badger.downloadFilesByFilelist([item])#,destDir)
    #print "result",result
    if result['OK']:
      self.db[item] = '2'
      self.db.sync()
  def Clear(self):
    transferOK = True
    for k,v in self.db.iteritems():
      if v=='0':
        transferOK = False
        print "Some files failed, you need run this script again"
        break
    self.db.close()
    if transferOK:
      print "All files transfer successful"
      os.remove(self.dbName)

dw = DownloadWorker()
mw = MultiWorker(dw,5)
mw.main()
dw.Clear()
total=time.time()-start
print "Finished,total time is %s"%total

exit(0)
