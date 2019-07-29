#!/usr/bin/env python

import DIRAC
from DIRAC import S_OK, S_ERROR

from DIRAC.Core.Base import Script

Script.setUsageMessage( """
Show task detailed information

Usage:
   %s [option] ... [TaskID] ...
""" % Script.scriptName )

Script.registerSwitch( "j",  "job",             "Show job numbers" )

Script.parseCommandLine( ignoreErrors = False )
options = Script.getUnprocessedSwitches()
args = Script.getPositionalArgs()

from IHEPDIRAC.WorkloadManagementSystem.Client.TaskClient   import TaskClient
taskClient = TaskClient()

from DIRAC.Core.DISET.RPCClient                      import RPCClient
jobmonClient = RPCClient('WorkloadManagement/JobMonitoring')

def showPairs(pairs):
  width = 0
  for pair in pairs:
    width = max(width, len(pair[0]))
  format = '%%-%ds : %%s' % width
  for k,v in pairs:
    print format % (k, v)

def showTask(taskID):
  outFields = ['TaskID','TaskName','Status','Owner','OwnerDN','OwnerGroup','CreationTime','UpdateTime','JobGroup','Site','Progress','Info']
  result = taskClient.getTask(taskID)
  if not result['OK']:
    print 'Get task error: %s' % result['Message']
    return False
  task = result['Value']

  pairsDict = {'Task':[], 'Progress':[], 'Info':[]}
  for k in outFields:
    if k in ['Progress', 'Info']:
      for kp,vp in sorted(task[k].iteritems(), key=lambda d:d[0]):
        if type(vp) == type([]):
          vp = ', '.join(vp)
        pairsDict[k].append([kp, vp])
    else:
      pairsDict['Task'].append([k, task[k]])

  showPairs(pairsDict['Task'])
  print '\n== Progress =='
  showPairs(pairsDict['Progress'])
  print '\n== Information =='
  showPairs(pairsDict['Info'])

def showTaskJobs(taskID):
  result = taskClient.getTaskJobs(taskID)
  if not result['OK']:
    print 'Get task jobs error: %s' % result['Message']
    return
  jobIDs = result['Value']
  print '== Jobs =='
  if not jobIDs:
    print 'No jobs found'
    return

  for jobID in jobIDs:
    print jobID,
  print ''

  result = jobmonClient.getJobs( { 'JobID': jobIDs, 'Status': ['Failed'] } )
  if not result['OK']:
    print 'Get task failed jobs error: %s' % result['Message']
    return
  print '\n== Jobs (Failed) =='
  for jobID in result['Value']:
    print jobID,

def showTaskHistories(taskID):
  result = taskClient.getTaskHistories(taskID)
  if not result['OK']:
    print 'Get task histories error: %s' % result['Message']
    return

  print '\n== Job Histories =='
  for status, statusTime, description in result['Value']:
    print '%-16s %-24s %s' % (status, statusTime, description)

def main():
  if len(args) < 1:
    Script.showHelp()
    return

  showJobNumber = False
  for option in options:
    (switch, val) = option
    if switch == 'j' or switch == 'job':
      showJobNumber = True

  for taskID in args:
    print '='*80
    taskID = int(taskID)

    showTask(taskID)
    print ''

    showTaskHistories(taskID)
    print ''

    if showJobNumber:
      showTaskJobs(taskID)
      print ''

if __name__ == '__main__':
  main()
