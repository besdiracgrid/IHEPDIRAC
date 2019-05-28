#!/usr/bin/env python

import os
import sys

from DIRAC import S_OK, S_ERROR, gLogger, exit
from DIRAC.Core.Base import Script

usageMsg = '''Add all the files under specified directory to SE and DFC

{0} [option|cfgfile] DFCDir LocalDir SE

Example: {0} /juno/user/u/username/data1 /junofs/users/username/data1 IHEP-STORM'''.format(Script.scriptName)
Script.setUsageMessage(usageMsg)
Script.parseCommandLine(ignoreErrors = False)

args = Script.getPositionalArgs()

if len(args) != 3:
    Script.showHelp()
    exit(1)

dfcDir = args[0]
localDir = args[1]
toSE = args[2]

from DIRAC.Interfaces.API.Dirac import Dirac
dirac = Dirac()

lfnList = []
pfnList = []

for root, dirs, files in os.walk(localDir):
    if not root.startswith(localDir):
        gLogger.error('Can not find corrent lfn')
        exit(1)
    relRoot = root[len(localDir):].lstrip('/')
    for f in files:
        fullFn = os.path.join(root, f)
        lfn = os.path.join(dfcDir, relRoot, f)
        lfnList.append(lfn)
        pfnList.append(fullFn)

gLogger.notice('%s files will be added to DFC "%s"' % (len(lfnList), dfcDir))

for lfn, pfn in zip(lfnList, pfnList):
    gLogger.debug('Add file to DFC: %s' % lfn)
    result = dirac.addFile(lfn, pfn, toSE)
    if not result['OK']:
        gLogger.error('Can not add file to DFC "%s": %s' % (lfn, result['Message']))
        exit(1)
    gLogger.debug('File upload successfully: %s' % f)
gLogger.notice('%s files added to DFC' % len(lfnList))
