# -*- coding: utf-8 -*-

import DIRAC
from DIRAC import gLogger
from DIRAC.Core.Base import Script

Script.parseCommandLine( ignoreErrors = True )
args = Script.getPositionalArgs()

if (len(args)==0):
  gLogger.error("Please give the request id")

from DIRAC.Core.DISET.RPCClient import RPCClient

transferRequest = RPCClient("Transfer/TransferRequest")

for transid in args:
  condDict = {"trans_req_id": int(transid)}
  res = transferRequest.show(condDict)
  if res["OK"]:
    for line in res["Value"]:
      print line

