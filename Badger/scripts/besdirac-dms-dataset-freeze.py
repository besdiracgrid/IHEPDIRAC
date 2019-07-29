#!/usr/bin/env python
#mtime:2013/12/09
"""
besdirac-dms-dataset-freeze
  freeze a dataset
  Usage:
    besdirac-dms-dataset-freeze <datasetname>
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
badger.freezeDataset(datasetName)
exit(0)
