#!/usr/bin/env python

import os
import sys

from DIRAC import S_OK, S_ERROR, gLogger, exit
from DIRAC.Core.Base import Script

usageMsg = '''Remove all the files and directories from SE and DFC under specified directory

{0} [option|cfgfile] DFCDir'''.format(Script.scriptName)
Script.setUsageMessage(usageMsg)
Script.parseCommandLine(ignoreErrors = False)

args = Script.getPositionalArgs()

if len(args) != 1:
    Script.showHelp()
    exit(1)

dfcDir = args[0]


from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient
fcc = FileCatalogClient('DataManagement/FileCatalog')

from DIRAC.DataManagementSystem.Client.DataManager import DataManager
dm = DataManager()


counterFile = 0
counterDir = 0

def removeDir(d):
    global counterFile
    global counterDir

    result = fcc.listDirectory(d)
    if not result['OK']:
        gLogger.error('Failed to list directory %s:' % (d, result['Message']))
        return

    if result['Value']['Successful'][d]['Files']:
        for f in result['Value']['Successful'][d]['Files']:
            gLogger.notice('Removing file: %s' % f)
            counterFile += 1
            dm.removeFile(f)

    if result['Value']['Successful'][d]['SubDirs']:
        for subdir in result['Value']['Successful'][d]['SubDirs']:
            removeDir(subdir)

    gLogger.notice('Removing dir: %s' % d)
    counterDir += 1
    fcc.removeDirectory(d)

removeDir(dfcDir)

gLogger.notice('%s directories and %s files deleted' % (counterDir, counterFile))
