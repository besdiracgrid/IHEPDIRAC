# -*- coding: utf-8 -*-

import DIRAC
from DIRAC import gLogger
from DIRAC.Core.Base import Script

Script.parseCommandLine( ignoreErrors = True )
args = Script.getPositionalArgs()

if (len(args)==0):
  gLogger.error("Please give the file id you want to retransfer")

from DIRAC.Core.DISET.RPCClient import RPCClient

transferRequest = RPCClient("Transfer/TransferRequest")

for transid in args:
  condDict = {"id": int(transid)}
  res = transferRequest.retransfer(condDict)
  print res

