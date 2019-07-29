# -*- coding: utf-8 -*-

import DIRAC
from DIRAC import gLogger
from DIRAC.Core.Base import Script

Script.setUsageMessage("""
Show a list of datasets

""")

Script.parseCommandLine( ignoreErrors = True )

from DIRAC.Core.DISET.RPCClient import RPCClient

transferRequest = RPCClient("Transfer/Dataset")

condDict = {}
orderby = []
start = 0
limit = 50

res = transferRequest.showtotal(condDict)

if not res["OK"]:
  gLogger.error(res)
  DIRAC.exit(-1)

print "Total:", res["Value"]

res = transferRequest.show(condDict, orderby, start, limit)

if not res["OK"]:
  gLogger.error(res)
  DIRAC.exit(-1)

for entry in res["Value"]:
  print entry
