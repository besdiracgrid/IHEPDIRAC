# -*- coding: utf-8 -*-

import DIRAC
from DIRAC import gLogger
from DIRAC.Core.Base import Script

Script.setUsageMessage("""
Create a dataset with a list of LFNs

Usage:
  %s <new dataset name> <filelist>
""" % Script.scriptName)

Script.registerSwitch("f:", "file=", "file contains LFN")

Script.parseCommandLine( ignoreErrors = True )

filelist = []

for k,v in Script.getUnprocessedSwitches():
  if k.lower() in ["f", "file"]:
    with open(v) as f:
      for line in f:
        filelist.append(line.strip())

args = Script.getPositionalArgs()
if ( len(args) == 0 or (len(filelist)+len(args) < 2) ):
  gLogger.error("Please support dataset name and LFNs")
  DIRAC.exit(-1)
dataset = args[0]
filelist.extend(args[1:])

gLogger.info("Dataset is ", dataset)
gLogger.info("FileList is ", filelist)

from DIRAC.Core.DISET.RPCClient import RPCClient

transferRequest = RPCClient("Transfer/Dataset")

print transferRequest.create(dataset, filelist)

