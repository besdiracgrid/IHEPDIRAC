# -*- coding: utf-8 -*-

import DIRAC
from DIRAC import gLogger
from DIRAC.Core.Base import Script

Script.setUsageMessage("""
delete a file transfer.
NOTE: This will only kill transfer which does not finish.
Usage:
  %s <fileid>
""" % Script.scriptName)

Script.parseCommandLine( ignoreErrors = True )

args = Script.getPositionalArgs()
if (len(args) != 1):
  gLogger.error("Please support ReqID.")
  DIRAC.exit(-1)

from DIRAC.Core.DISET.RPCClient import RPCClient

transferRequest = RPCClient("Transfer/TransferRequest")

fileid = int(args[0])

print transferRequest.delete_files_in_req({"trans_req_id":fileid})
