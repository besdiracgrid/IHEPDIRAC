#!/usr/bin/env python
#mtime:2013/12/09
"""
besdirac-dms-dataset-filelist
  get the filelist of the given dataset
  Usage:
    besdirac-dms-dataset-filelist<datasetname>
"""

__RCSID__ = "$Id$"
import pprint
from DIRAC import S_OK, S_ERROR, gLogger, exit
from DIRAC.Core.Base import Script

Script.setUsageMessage(__doc__)
args = Script.getPositionalArgs()

if len(args)!=1:
  Script.showHelp()
datasetName = args[0]

from IHEPDIRAC.Badger.API.Badger import Badger
badger = Badger()
result = badger.getFilesByDatasetName(datasetName)
if result['OK']:
  fileList = result['Value']
pprint.pprint(fileList)
exit(0)
