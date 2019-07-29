#!/usr/bin/env python

import json

import DIRAC
from DIRAC import S_OK, S_ERROR

from DIRAC.Core.Base import Script

Script.setUsageMessage( """
Get task progress of events

Usage:
   %s TaskID1 TaskID2 ...
""" % Script.scriptName )

Script.parseCommandLine( ignoreErrors = False )
options = Script.getUnprocessedSwitches()
args = Script.getPositionalArgs()

from IHEPDIRAC.WorkloadManagementSystem.Client.TaskClient   import TaskClient
taskClient = TaskClient()

from DIRAC.Core.DISET.RPCClient           import RPCClient
jobMonitor = RPCClient('WorkloadManagement/JobMonitoring')

def getTaskEventProgress(taskID):
    progress = {}

    result = taskClient.getTaskJobs(taskID)
    if not result['OK']:
        print result['Message']
        return progress
    jobIDs = result['Value']

    result = jobMonitor.getJobsStatus(jobIDs)
    if not result['OK']:
        print result['Message']
        return progress
    jobsStatus = result['Value']

    outFields = ['JobID', 'Info']
    result = taskClient.getJobs(jobIDs, outFields)
    if not result['OK']:
        print result['Message']
        return progress
    jobsInfo = {info[0] : json.loads(info[1]) for info in result['Value']}

    progress['Total'] = {'JobNum': 0, 'EventNum': 0}
    for jobID in jobIDs:
        if jobID in jobsStatus:
            status = jobsStatus[jobID]['Status']
        else:
            status = 'Deleted'

        if status not in progress:
            progress[status] = {'JobNum': 0, 'EventNum': 0}
        progress[status]['JobNum'] += 1
        progress['Total']['JobNum'] += 1

        if jobID in jobsInfo:
            evtNum = jobsInfo[jobID]['EventNum'] if 'EventNum' in jobsInfo[jobID] else 0
            progress[status]['EventNum'] += evtNum
            progress['Total']['EventNum'] += evtNum

    return progress

def printALine(progress, status):
    if status in progress:
        print '%-12s %12s %12s' % (status, progress[status]['JobNum'], progress[status]['EventNum'])

def main():
    if len(args) < 1:
        Script.showHelp()
        return

    fixedStatuses = ['Total', 'Done', 'Failed', 'Running', 'Waiting', 'Deleted']

    for taskID in args:
        taskID = int(taskID)
        progress = getTaskEventProgress(taskID)

        if not progress:
            continue

        print '='*80
        print 'Task %s progress:' % taskID
        print '-'*12 + ' ' + '-'*12 + ' ' + '-'*12
        print '%-12s %-12s %-12s' % ('Status', 'Job Number', 'Event Number')
        print '-'*12 + ' ' + '-'*12 + ' ' + '-'*12
        printALine(progress, 'Total')
        print '-'*12 + ' ' + '-'*12 + ' ' + '-'*12
        for fixedStatus in fixedStatuses:
            if fixedStatus != 'Total':
                printALine(progress, fixedStatus)
        for p in progress:
            if p not in fixedStatuses:
                printALine(progress, p)
        print '-'*12 + ' ' + '-'*12 + ' ' + '-'*12

        print ''

if __name__ == '__main__':
    main()
