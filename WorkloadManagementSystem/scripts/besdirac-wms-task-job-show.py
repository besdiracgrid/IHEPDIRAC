#!/usr/bin/env python

import json

import DIRAC
from DIRAC import S_OK, S_ERROR

from DIRAC.Core.Base import Script

Script.setUsageMessage( """
Show job information

Usage:
   %s [option] ... [JobID] ...
""" % Script.scriptName )

Script.parseCommandLine( ignoreErrors = False )
options = Script.getUnprocessedSwitches()
args = Script.getPositionalArgs()

from IHEPDIRAC.WorkloadManagementSystem.Client.TaskClient   import TaskClient
taskClient = TaskClient()

def showPairs(pairs):
  width = 0
  for pair in pairs:
    width = max(width, len(pair[0]))
  format = '- %%-%ds : %%s' % width
  for k,v in pairs:
    print format % (k, v)

def showJobs(jobIDs):
  outFields = ['TaskID', 'JobID', 'Info']
  result = taskClient.getJobs(jobIDs, outFields)
  if not result['OK']:
    print 'Show jobs error: %s' % result['Message']
    return

  if not result['Value']:
    print 'No task found for jobs: %s' % jobIDs
    return

  for v in result['Value']:
    taskID = v[0]
    jobID = v[1]
    jobInfo = json.loads(v[2])

    pairs = []
    pairs.append(['TaskID', str(taskID)])
    for k,v in sorted(jobInfo.iteritems(), key=lambda d:d[0]):
      if type(v) == type([]):
        v = ', '.join(v)
      pairs.append([k, v])

    print 'JobID : %s' % jobID
    showPairs(pairs)

    print ''

def main():
  if len(args) < 1:
    Script.showHelp()
    return

  jobIDs = []
  for jobID in args:
    jobIDs.append(int(jobID))

  showJobs(jobIDs)

  print 'Totally %s job(s) displayed' % len(jobIDs)

if __name__ == '__main__':
  main()
