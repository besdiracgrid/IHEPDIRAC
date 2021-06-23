#!/usr/bin/env python

import os
import sys
import re
from stat import S_ISREG, S_ISDIR, S_IXUSR, S_IRUSR, S_IWUSR, S_IRWXG, S_IRWXU, S_IRWXO

from DIRAC import S_OK, S_ERROR, gLogger, exit
from DIRAC.Core.Base import Script

Script.setUsageMessage('''Scan dark files or unlinked files (registered in other SEs from DFC but not linked to assigned SE) in assigned SEs.

[option] DfcDir/File|DfcFileList SE

Example: -e /juno/user/file.txt IHEP-STORM'''.format(Script.scriptName))
Script.registerSwitch( 'e', 'delete', 'Delete dark files from SE after scan')
Script.registerSwitch( 'r:', 'register=', 'Register dark files or unlinked files after scan, <value>=[dark|unlinked|both]')
Script.registerSwitch( 'l', 'list', 'Use a local file list as input')
Script.registerSwitch( 'k', 'skipsub', 'Skip sub-directory when scanning')
Script.registerSwitch( 'm', 'srmmode', 'Special registering mode for SRM when more than 2000 files in a directory, only scan unlinked file')
Script.parseCommandLine(ignoreErrors = False)

import gfal2
g=gfal2.creat_context()

from DIRAC.Core.Utilities.File import makeGuid
from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient
fcc = FileCatalogClient('DataManagement/FileCatalog')

# input data
args = Script.getPositionalArgs()
if len(args) != 2:
    Script.showHelp()
    exit(1)

INPUT = args[0]
SITE = args[1]

deletionSwitch = False
registerOption = 'none'
inputSwitch = False
srmSwitch = False
skipSwitch = False
switches = Script.getUnprocessedSwitches()
for switch in switches:
    if switch[0] == 'e' or switch[0] == 'delete':
        deletionSwitch = True
    if switch[0] == 'r' or switch[0] == 'register':
        registerOption = switch[1].lower()
    if switch[0] == 'l' or switch[0] == 'list':
        inputSwitch = True
    if switch[0] == 'm' or switch[0] == 'srmmode':
        srmSwitch = True
    if switch[0] == 'k' or switch[0] == 'skipsub':
        skipSwitch = True

if registerOption not in ['none', 'dark', 'unlinked', 'both']:
    gLogger.error("Error: '-r' option can only be 'dark', 'unlinked' or 'both'")
    exit(1)
if registerOption in ['both', 'dark'] and deletionSwitch:
    gLogger.error("Error: '-r dark/both' and '-e' cannot be used at the same time, please use '-r dark' when deleting")
    exit(1)
if registerOption in ['both', 'dark'] and srmSwitch:
    gLogger.error("Error: '-r dark/both' and '-m' cannot be used at the same time, please use '-r unlinked' when using srmmode")
    exit(1)

from DIRAC.DataManagementSystem.Client.DataManager import DataManager
dm = DataManager()

from DIRAC.Resources.Storage.StorageElement import StorageElement
se = StorageElement(SITE)

# initial list
protocols = ['root', 'http', 'gsiftp', 'srm']
fileList = []
fileDFCList = []
fileDarkList = []
fileUnlinkedList = []

inputFileList = []
if inputSwitch:
    inputFile = open(INPUT, 'r')
    tmpList = inputFile.readlines()
    for i in tmpList:
        inputFileList.append(i.rstrip('\n').rstrip('/'))
else:
    inputFileList.append(INPUT.rstrip('/'))
HEAD0 = se.getURL(inputFileList[0],protocols)['Value']['Successful'][inputFileList[0]][0:-len(inputFileList[0])]
HEAD = re.sub('\?', '\\?', HEAD0)
# print HEAD0

# special for SRM
if HEAD[0:6] == ("srm://"):
    gLogger.notice("Notice: This is a SRM SE. Cannot scan more than 2000 files! Please use srm mode if you need to scan and register more than 2000 files in a directory.\n")

def resolveUrl(dfcUrl):
    pfnUrl = se.getURL(dfcUrl, protocols)['Value']['Successful'][dfcUrl]
    return pfnUrl

# list all files recursively in a PFN directory
def listFile(pfnUrl):
    # skip "." and ".." when list file
    if pfnUrl[-2:] == ("/.") or pfnUrl[-3:] == ("/.."):
        return
    tmpList = g.listdir(pfnUrl)
    for i in tmpList:
        if isDir(pfnUrl + '/' + i):
            if not skipSwitch:
                listFile(pfnUrl + '/' + i)
            else:
                return
        else:
            fileList.append(pfnUrl + '/' + i)
            fileDFCList.append(re.sub(HEAD, '', pfnUrl + '/' + i))
    return

# compare a single file between DFC and PFN
def compareFile(dfcUrl):
    rstDist=dm.getReplicas(dfcUrl)
    if rstDist['OK'] == True:
        if dfcUrl in rstDist['Value']['Successful'].keys():
            if SITE in rstDist['Value']['Successful'][dfcUrl].keys():
                return True
            else:
                fileUnlinkedList.append(dfcUrl)
                return False
        else:
            fileDarkList.append(dfcUrl)
            return False
    else:
        gLogger.error("Dirac API Error!")
        exit(1)
    return False

# listFile func for srm mode
def srmListFile(dfcUrl):
    rstDist = fcc.listDirectory(dfcUrl)['Value']['Successful'][dfcUrl]
    if 'Files' in rstDist:
        tmpFileList = rstDist['Files'].keys()
        for i in tmpFileList:
            fileList.append(HEAD0 + '/' + i)
            fileDFCList.append(i)
    if 'SubDirs' in rstDist and not skipSwitch:
        tmpSubdirList = rstDist['SubDirs'].keys()
        for i in tmpSubdirList:
            srmListFile(i)
    return

# compare a single file between DFC and PFN, only scan unlinked files, cannot scan lost files
def srmCompareFile(dfcUrl):
    rstDist=dm.getReplicas(dfcUrl)
    if rstDist['OK']:
        if dfcUrl in rstDist['Value']['Successful'].keys():
            if SITE in rstDist['Value']['Successful'][dfcUrl].keys():
                return False
            try:
                g.stat(resolveUrl(dfcUrl))
            except gfal2.GError:
                return False
    gLogger.notice("Notice: Unlinked file found, %s" % dfcUrl)
    fileUnlinkedList.append(dfcUrl)
    return True

# judge whether a gfal return is a directory
def isDir(pfnUrl):
    try:
        rst = S_ISDIR(g.stat(pfnUrl).st_mode)
    except gfal2.GError:
        raise
    return S_ISDIR(g.stat(pfnUrl).st_mode)

# execute
for i in inputFileList:
    PFNURL = resolveUrl(i)

    # judge if file/path exist in SE. if not, move to next file/path
    try:
        isDir(PFNURL)
    except gfal2.GError:
        gLogger.notice("Warning: file or directory (%s) not found in assigned SE, skipped!\n" % PFNURL)
        continue

    if not isDir(PFNURL):
        fileList.append(PFNURL)
        fileDFCList.append(re.sub(HEAD, '', PFNURL))
    else:
        if not srmSwitch:
            listFile(PFNURL)
        else:
            srmListFile(i)

# duplicate removal
fileList = list(set(fileList))
fileList.sort()
fileDFCList = list(set(fileDFCList))
fileDFCList.sort()

# output
if not srmSwitch:
    gLogger.notice("----------  Files found in assigned SE:  ----------")
else:
    gLogger.notice("----------  Files found in assigned DFC:  ----------")
for i in fileDFCList:
    gLogger.notice(i)
    if not srmSwitch:
        compareFile(i)
    else:
        srmCompareFile(i)
gLogger.notice("----------  Total: %d found files.  ----------\n" %len(fileDFCList))

if not srmSwitch:
    gLogger.notice("----------  Files unregistered (dark files):  ----------")
    for i in fileDarkList:
        gLogger.notice(i)
    gLogger.notice("----------  Total: %d dark files.  ----------\n" %len(fileDarkList))

gLogger.notice("----------  Files registered but not register as assigned SE replicas:  ----------")
for i in fileUnlinkedList:
    gLogger.notice(i)
gLogger.notice("----------  Total: %d unlinked files.  ----------\n" %len(fileUnlinkedList))

if deletionSwitch:
    for i in fileDarkList:
        g.unlink(HEAD0 + '/' + i)
        gLogger.notice("%s deleted!" %(HEAD0 + '/' + i))
    gLogger.notice("----------  All dark files deleted!  ----------\n")

if registerOption != 'none':
    if registerOption in ['dark', 'both']:
        gLogger.notice("----------  Register dark files:  ----------")
        for i in fileDarkList:
            fileStat = g.stat(HEAD0 + '/' + i)
            fileSize = fileStat.st_size
            fileGuid = makeGuid()
            fileSum = g.checksum(HEAD0 + '/' + i, "ADLER32")
            fileTuple = (i, HEAD0 + '/' + i, fileSize, SITE, fileGuid, fileSum)
            dm.registerFile(fileTuple)
            gLogger.notice(i + ' registered successfully!')
    if registerOption in ['unlinked', 'both']:
        gLogger.notice("----------  Register unlinked files:  ----------")
        for i in fileUnlinkedList:
            fileTuple = (i, HEAD0 + '/' + i, SITE)
            dm.registerReplica(fileTuple)
            gLogger.notice(i + ' registered successfully!')
    gLogger.notice("----------  Register finished  ----------\n")
