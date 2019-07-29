#!/usr/bin/env python
# -*- coding:utf-8 -*-
# author: zhanggang
'''checksum,compare the size of LFN files and Local files 
   Usage :
    besdirac-dms-check-files <dfcDir> <localDir>
    Example: besdirac-dms-check-files /dir1  /dir2
'''
import os.path
import pprint
import DIRAC
from DIRAC.Core.Base import Script

from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient
Script.setUsageMessage(__doc__)
Script.parseCommandLine(ignoreErrors=True)
from IHEPDIRAC.Badger.API.Badger import Badger

badger = Badger()
dirs = Script.getPositionalArgs()
dfcdir = dirs[0]
localDir = dirs[1]

#get DFC file dict
lfns = badger.listDir(dfcdir)
lfnDict = badger.getSize(lfns)
base_lfnDict = {}
for k,v in lfnDict.items():
  k = os.path.basename(k)
  base_lfnDict[k] = v
#get local files dict
localFiles = badger.getFilenamesByLocaldir(localDir)
base_localDict = {}
for file in localFiles:
  base_localDict[os.path.basename(file)] = os.path.getsize(file)

filesOK = True
omitList = []
partList = []
if len(lfns)>=len(localFiles):
  for item in base_lfnDict.keys():
    if item in base_localDict.keys():
      if base_localDict[item]!=base_lfnDict[item]:
        partList.append(item,base_localDict[item],base_lfnDict[item])
        filesOK = False
      else:
        pass
    else:
      omitList.append(item)
      filesOK = False
  if partList:
    print "these file has not transfer completely"
    pprint.pprint(partList)
  if omitList:
    print "these file has not tranfer yet."
    pprint.pprint(omitList)
  if filesOK:
    print "all are OK"
else:
  for item in base_localDict.keys():
    if item in base_lfnDict.keys():
      if base_lfnDict[item]!=base_localDict[item]:
        partList.append(item,base_localDict[item],base_lfnDict[item])
        filesOK = False
      else:
        pass
    else:
      omitList.append(item)
      filesOK = False
  if partList:
    print "these file has not transfer completely"
    pprint.pprint(partList)
  if omitList:
    print "these file has not tranfer yet."
    pprint.pprint(omitList)
  if filesOK:
    print "all are OK"

DIRAC.exit(0)
