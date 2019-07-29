#!/usr/bin/env python
# -*- coding: UTF-8 -*-
#mtime:2013/12/10
"""
besdirac-dms-get-files
  This script get a set of files from SE to localdir.

  Usage:
    besdirac-dms-dataset-get datasetName [destdir]
    Arguments:
      datasetName: a dataset that contain a set of files.
    Examples:
      besdirac-dms-dataset-get User_XXX_XXX -D /localdir/subdir/subdir/ 
      besdirac-dms-dataset-get User_XXX_XXX 
"""
__RCSID__ = "$Id$"

import sched
import os
import sys
import anydbm
import time
import datetime
import tempfile
import subprocess
from DIRAC import S_OK, S_ERROR, gLogger, gConfig, exit
from DIRAC.Core.Base import Script


Script.setUsageMessage(__doc__)
Script.registerSwitch("m:", "method=", "Downloading method")
Script.registerSwitch("D:", "dir=",    "Output directory")
Script.registerSwitch("w:", "wait=",   "Waiting interval (s)")
Script.registerSwitch("t:", "thread=", "Simultaneously downloading thread number")

Script.parseCommandLine(ignoreErrors=True)
options = Script.getUnprocessedSwitches()
args = Script.getPositionalArgs()
if not args:
  Script.showHelp()
  exit(1)
setName = args[0]
destDir = '.'    #localdir that file download to
#if len(args)>1:
#  destDir = args[1]
##print destDir

rsync_se = 'IHEPD-USER'
rsync_url = gConfig.getValue('/Resources/Applications/RsyncEndpoints/%s/Url'%rsync_se, 'rsync://localhost/bes-srm')

method = 'rsync'
output_dir = '.'
interval = 300
for option in options:
  (switch, val) = option
  if switch == 'm' or switch == 'method':
    method = val
  if switch == 'D' or switch == 'dir':
    output_dir = val
    destDir = val
  if switch == 'w' or switch == 'wait':
    interval = int(val)

from IHEPDIRAC.Badger.API.Badger import Badger
from IHEPDIRAC.Badger.API.multiworker import IWorker,MultiWorker

sys.stdout = os.fdopen(sys.stdout.fileno(), 'w', 0)

def time_print(message):
  print '[%s UTC] %s' % (datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'), message)

def getDB(name,function):
  """return a db instance,the db contain the file list.
  default value is 0,means the file is not transfer yet,if 2,means OK.
  """
  dbname = "db_"+name[-4:]
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

def getCurrentDirTotalSize(destDir):
  #calculate size of data that has download already
  totalSize = 0
  for path in os.listdir(destDir):
    path = os.path.join(destDir,path)
    if os.path.isfile(path):
      #print path,os.path.getsize(path)
      totalSize += os.path.getsize(path)
  return totalSize

start = 0
datasetTotalSize = 0.0001
originLocalfileSize = 0
def datasetGet():
  badger = Badger()
  badger.updateDataset(setName)
  datasetTotalSize = badger.getDatasetMetadata(setName)['Value']['TotalSize']
  originLocalfileSize = getCurrentDirTotalSize(destDir) #check file size of the destDir before download
  print "start download..."
  start = time.time()

  dw = DownloadWorker()
  mw = MultiWorker(dw,5)
  mw.main()
  dw.Clear()

  total=time.time()-start
  print "Finished,total time is %s"%total


def printInfo():
  #输出平均速度，传输进度等信息 
  spendTime = time.time()-start
  localfileSize = getCurrentDirTotalSize(destDir)#-originLocalfileSize 
  speed = round(float(localfileSize)/1024/spendTime,2)
  downloadRatio = round(float(localfileSize)/datasetTotalSize,6)
  print "The average speed is %s(KB/s)"%(speed)
  print "Has download %s%% data"%(downloadRatio*100)

#s = sched.scheduler(time.time,time.sleep)
#def perform(inc):
#  #可以周期性的执行printInfo函数
#  s.enter(inc,0,perform,(inc,))
#  printInfo()
#
#def mymain(inc=1):
#  s.enter(0,0,perform,(inc,))
#  s.run()
#mymain()
#exit(0)

class DownloadWorker(IWorker):
  """
  """
  #if file failed download,then append in errorDict
  #self.errorDict = {}

  def __init__(self):
    self.badger = Badger()
    self.db,self.dbName = getDB(setName,self.badger.getFilesByDatasetName)
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
    result = badger.downloadFilesByFilelist([item],destDir)
    #print "result",result
    if result['OK']:
      self.db[item] = '2'
      self.db.sync()
      printInfo()
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

class Rsync:
  def __init__(self):
    self.listFile = tempfile.NamedTemporaryFile(mode='w', prefix='tmp_filelist_', delete=False)
    self.dirName = ''

  def __del__(self):
    self.listFile.close()
    os.remove(self.listFile.name)

  def getFileList(self):
    badger = Badger()
    result = badger.getFilesByDatasetName(setName)
    fileList = []
    if result['OK']:
      fileList = result['Value']
      if fileList:
        self.dirName = os.path.dirname(fileList[0])
    for file in fileList:
      print >>self.listFile, os.path.basename(file)
    self.readyNum = len(fileList)
    self.listFile.close()
    time_print('There are %s files ready for download' % self.readyNum)

  def sync(self):
    time_print('='*80)
    time_print('Start downloading...')

    cmd = ["rsync", "-avvvz", "--partial", "--files-from=%s"%self.listFile.name, "%s%s"%(rsync_url, self.dirName), "%s"%output_dir]
    popen = subprocess.Popen(cmd, stdout=subprocess.PIPE)

    self.totalNum = 0
    self.downloadNum = 0
    self.skipNum = 0
    self.speed = 0
    for line in iter(popen.stdout.readline, ""):
      if line.find('recv_file_name') != -1:
        self.totalNum += 1
      if line.find('renaming') != -1:
        self.downloadNum += 1
        time_print('File downloaded: %s' % line.split()[-1])
      if line.find('is uptodate') != -1:
        self.skipNum += 1
      if line.find('bytes/sec') != -1:
        self.speed = float(line.split()[-2]) / 1024 / 1024
    self.status = popen.wait()

    time_print('Finish downloading...')
    time_print('='*80)
    return self.status

  def output(self):
    time_print('Download status: %s, speed: %.2f (MB/s)' % (self.status, self.speed))
    time_print('Total: %s / %s' % (self.totalNum, self.readyNum))
    time_print('Download in this cycle: %s' % self.downloadNum)
    time_print('Skip already downloaded: %s' % self.skipNum)


def datasetRsync():
  rsync_counter = 0
  while True:
    rsync_counter += 1
    time_print('Start cycle %s' % rsync_counter)
    rsync = Rsync()
    rsync.getFileList()
    while True:
      status = rsync.sync()
      rsync.output()
      if status == 0:
        break
    del rsync
    time_print('Waiting %s seconds for next downloading... Press Ctrl+C to exit\n' % interval)
    time.sleep(interval)

if method == 'get':
  datasetGet()
elif method == 'rsync':
  datasetRsync()

exit(0)
