# -*- coding: utf-8 -*-

import DIRAC
from DIRAC import gLogger
from DIRAC.Core.Base import Script

Script.setUsageMessage("""
Show a list of LFNs in a dataset 

Usage:
  %s <dataset name> 
""" % Script.scriptName)

Script.parseCommandLine( ignoreErrors = True )

args = Script.getPositionalArgs()
if ( len(args) == 0 ):
  gLogger.error("Please give the dataset name")
  DIRAC.exit(-1)

dataset = args[0]

gLogger.info("Dataset is ", dataset)

from DIRAC.Core.DISET.RPCClient import RPCClient

transferRequest = RPCClient("Transfer/Dataset")

res = transferRequest.list(dataset)

if not res["OK"]:
  gLogger.error(res)

for entry in res["Value"]:
  print entry
