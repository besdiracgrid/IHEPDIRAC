#!/usr/bin/env python
#mtime:2013/12/09 
"""
list dataste names and their metadata
"""
__RCSID__ = "$Id$"

import time
import pprint
import DIRAC
from DIRAC.Core.Base import Script
Script.parseCommandLine(ignoreErrors=True)
Script.setUsageMessage(__doc__)

from IHEPDIRAC.Badger.API.Badger import Badger
badger = Badger()
badger.listDatasets()
exitCode = 0
DIRAC.exit(exitCode)

