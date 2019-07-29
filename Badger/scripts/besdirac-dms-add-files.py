#!/usr/bin/env python
# author: zhanggang
"""
besdirac-dms-add-files
  Multi-thread upload a set of file to SE and register them in DFC.
  Usage:
    besdirac-dms-add-files <Localdir>
    Argument:
      Localdir: the location of files that you want to upload to SE.
    Example:
      script /afs/ihep.ac.cn/users/z/zhanggang/
"""
__RCSID__ = "$Id$"

import time
import os
import anydbm
from DIRAC import S_OK, S_ERROR, gLogger,exit
from DIRAC.Core.Base import Script

Script.registerSwitch("r","dir","the directory that dataset files located")
Script.setUsageMessage(__doc__)
args = Script.getPositionalArgs()

if len(args) == 0:
  Script.showHelp()
  exit(-1)
ePoint = ''
if len(args)>1:
  energyPoint = args[1]

from IHEPDIRAC.Badger.API.Badger import Badger
from IHEPDIRAC.Badger.API.multiworker import IWorker,MultiWorker

def getDB(name,function):
  """return a db instance,the db contain the file list.
  default value is 0,means the file is not transfer yet,if 2,means OK.
  """
  dbname = "db_"+name[1]+'_'+str(name[-3:])
  if not os.path.exists(dbname):
    fileList = function(name)
    #print fileList
    db = anydbm.open(dbname,'c')
    for file in fileList:
      db[file] = '0'
      db.sync()
  else:
    db = anydbm.open(dbname,'c')
  return (db,dbname)

localdir = args[0]
startTime = time.time()
print "startTime",time.strftime("%Y-%m-%d %H:%m:%S",time.gmtime(startTime))

class UploadWorker(IWorker):
  """ 
  """
  def __init__(self, localdir):
    self.badger = Badger()
    #self.m_list = badger.getFilenamesByLocaldir(localdir)
    self.db,self.dbName = getDB(localdir,self.badger.getFilenamesByLocaldir)
    #print self.db,self.dbName
  def get_file_list(self):
    #print self.m_list
    #return self.m_list
    for k,v in self.db.iteritems():
      if v=='2':
        continue
      yield k

  def Do(self, item):
    badger = Badger()
    result = badger.uploadAndRegisterFiles([item],ePoint=energyPoint)
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

uw = UploadWorker(localdir)
mw = MultiWorker(uw,5)
mw.main()
endTime = time.time()-startTime
uw.Clear()
print "endTime",endTime

exit(0)
