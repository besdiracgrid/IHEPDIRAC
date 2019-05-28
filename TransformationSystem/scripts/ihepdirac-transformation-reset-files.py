#!/usr/bin/env python

import sys

from DIRAC import S_OK, S_ERROR, gLogger, exit
from DIRAC.Core.Base import Script

Script.setUsageMessage('''Reset files in transformation with specified status

{0} [option|cfgfile] TransID1 [TransID2 ...]

Example: {0} -t Problematic,Assigned 123 456'''.format(Script.scriptName))
Script.registerSwitch('t:', 'status=', 'Files status to be reset (default to "Problematic")')
Script.parseCommandLine(ignoreErrors = False)

from DIRAC.TransformationSystem.Client.Transformation import Transformation
from DIRAC.TransformationSystem.Client.TransformationClient import TransformationClient

args = Script.getPositionalArgs()

if len(args) == 0:
    Script.showHelp()
    exit(1)


status = ['Problematic']

switches = Script.getUnprocessedSwitches()
for switch in switches:
    if switch[0] == 't' or switch[0] == 'status':
        status = switch[1].split(',')
        status = [s.strip() for s in status]

tc = TransformationClient()

for t in args:
    res = tc.getTransformation(t)
    if not res['OK']:
        gLogger.error('Failed to get transformation information for %s: %s' % (t, res['Message']))
        continue

    selectDict = {'TransformationID': res['Value']['TransformationID']}
    if status:
        selectDict['Status'] = status
    res = tc.getTransformationFiles(condDict=selectDict)
    if not res['OK']:
        gLogger.error('Failed to get transformation files: %s' % res['Message'])
        continue
    if not res['Value']:
        gLogger.debug('No file found for transformation %s' % t)
        continue

    lfns = [f['LFN'] for f in res['Value']]

    gLogger.notice('Reset files for status: %s' % status)
    res = tc.setFileStatusForTransformation(t, 'Unused', lfns)
    if not res['OK']:
        gLogger.error('Failed to reset file status: %s' % res['Message'])
        continue
    if 'Failed' in res['Value']:
        gLogger.warn('Could not reset some files: ')
        for lfn, reason in res['Value']['Failed'].items():
          gLogger.warn('%s: %s' % (lfn, reason))

    gLogger.notice('Updated file statuses to "Unused" for %d file(s)' % len(lfns))

    result = tc.setTransformationParameter(t, 'Status', 'Flush')
    if not result['OK']:
        gLogger.error('Can not flush transformation: %s' % result['Message'])
        continue
