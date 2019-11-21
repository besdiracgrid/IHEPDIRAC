#!/usr/bin/env python

""" Upload files to SE, register to DFC and set metadata
"""

__RCSID__ = "$Id$"

# generic imports
import os
import glob
import json

# DIRAC imports
import DIRAC
from DIRAC.Core.Base import Script
Script.parseCommandLine()

from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient
from DIRAC.DataManagementSystem.Client.DataManager import DataManager
from DIRAC.Core.Utilities.SiteSEMapping import getSEsForSite


def getSiteSE(SEname):
    sitename = DIRAC.siteName()
    DIRAC.gLogger.error('Sitename: %s' % (sitename))
    print "sitename", sitename
    res = getSEsForSite(sitename)
    if not res['OK']:
        DIRAC.gLogger.error(res['Message'])
        return SEname
    if res['Value']:
        SEname = res['Value'][0]
    return SEname

####################################################


def addDataFiles(args):
    fcc = FileCatalogClient()
    dm = DataManager(['FileCatalog'])

    outputPath = args[0]
    outputPattern = args[1]
    outputSE = args[2]
    mode = args[3]

    if mode == 'closest':
        outputSE = getSiteSE(outputSE)

    DIRAC.gLogger.error('OutputSE: %s' % (outputSE))
    print "outputSE", outputSE

    # Create path
    res = fcc.createDirectory(outputPath)
    if not res['OK']:
        return res

    # Upload data files
    all_files = glob.glob(outputPattern)

    # Check that data files exist
    if len(all_files) == 0:
        return DIRAC.S_ERROR('No data files found')

    for one_file in all_files:
        lfn = os.path.join(outputPath, one_file)
        msg = 'Try to upload local file: %s \nwith LFN: %s \nto %s' % (
            one_file, lfn, outputSE)
        DIRAC.gLogger.notice(msg)
        res = dm.putAndRegister(lfn, one_file, outputSE)
        # Check if failed
        if not res['OK']:
            DIRAC.gLogger.error('Failed to putAndRegister %s \nto %s \nwith message: %s' % (
                lfn, outputSE, res['Message']))
            return res
        elif res['Value']['Failed'].has_key(lfn):
            DIRAC.gLogger.error(
                'Failed to putAndRegister %s to %s' % (lfn, outputSE))
            return res

    return DIRAC.S_OK()


####################################################
if __name__ == '__main__':
    args = Script.getPositionalArgs()
    try:
        res = addDataFiles(args)
        if not res['OK']:
            DIRAC.gLogger.error(res['Message'])
            DIRAC.exit(-1)
    except Exception:
        DIRAC.gLogger.exception()
        DIRAC.exit(-1)
