#!/usr/bin/env python

import os
import sys

from DIRAC import S_OK, S_ERROR, gLogger, exit
from DIRAC.Core.Base import Script

usageMsg = '''Remove specified replica from SE under specified directory

{0} [option|cfgfile] DFCDir SE'''.format(Script.scriptName)
Script.setUsageMessage(usageMsg)
Script.parseCommandLine(ignoreErrors=False)

args = Script.getPositionalArgs()

if len(args) != 2:
    Script.showHelp()
    exit(1)

dfcDir = args[0]
SE = args[1]


from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient
fcc = FileCatalogClient('DataManagement/FileCatalog')

from DIRAC.DataManagementSystem.Client.DataManager import DataManager
dm = DataManager()


counterFile = 0
counterDir = 0


def removeFromDir(d):
    global counterFile
    global counterDir

    result = fcc.listDirectory(d)
    if not result['OK']:
        gLogger.error('Failed to list directory %s: %s' %
                      (d, result['Message']))
        return

    gLogger.notice('Removing replicas from dir: %s' % d)

    if result['Value']['Successful'][d]['Files']:
        files = result['Value']['Successful'][d]['Files']
        fileNumber = len(files)
        gLogger.notice(
            'Removing replicas of {0} files from dir "{1}"'.format(fileNumber, d))
        counterFile += fileNumber
        dm.removeReplica(SE, files)

    if result['Value']['Successful'][d]['SubDirs']:
        for subdir in result['Value']['Successful'][d]['SubDirs']:
            removeFromDir(subdir)

    counterDir += 1


removeFromDir(dfcDir)

gLogger.notice('%s directories and %s files deleted' %
               (counterDir, counterFile))
