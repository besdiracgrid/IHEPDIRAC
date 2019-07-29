#!/usr/bin/env python
#mtime:2013/12/09
"""
besdirac-dms-dataset-release
  release a dataset from freeze to dynamic
  Usage:
    besdirac-dms-dataset-release <datasetname>
"""

__RCSID__ = "$Id$"
from DIRAC import S_OK, S_ERROR, gLogger, exit
from DIRAC.Core.Base import Script

Script.setUsageMessage(__doc__)
args = Script.getPositionalArgs()

if len(args)!=1:
  Script.showHelp()
datasetName = args[0]

from IHEPDIRAC.Badger.API.Badger import Badger
badger = Badger()
badger.releaseDataset(datasetName)
exit(0)
