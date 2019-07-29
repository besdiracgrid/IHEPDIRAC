#!/usr/bin/env python
#mtime:2013/12/09
"""

besdirac-dms-dataset-create
  register a new dataset.
  Usage:
    besdirac-dms-dataset-create <datasetName> <path> <conditions>
    Arguments:
      datasetName
      path: the path as the root path
      conditions: meta query conditions
    Example:
      besdirac-dms-dataset-create name /zhanggang_test/ "runL>111 runH<200 resonance=jpsi"
"""
__RCSID__ = "$Id$"
from DIRAC import S_OK, S_ERROR, gLogger, exit
from DIRAC.Core.Base import Script

Script.setUsageMessage(__doc__)

args = Script.getPositionalArgs()
#print len(args)
if len(args)<3:
  Script.showHelp()
  exit(-1)

datasetName = args[0]
path = args[1]
strArg = args[2]

from IHEPDIRAC.Badger.API.Badger import Badger

badger = Badger()
prefix = badger.getDatasetNamePrefix()
datasetName = prefix+datasetName

#print datasetName
result = badger.registerDataset(datasetName,path,strArg)
if result['OK']:
  resVal = badger.getFilesByDatasetName(datasetName)
  if resVal['OK']:
    fileList = resVal['Value']
    badger.reCalcCount(fileList)
#print result

exit(0)


