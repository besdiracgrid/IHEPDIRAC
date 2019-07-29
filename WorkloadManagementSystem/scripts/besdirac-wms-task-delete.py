#!/usr/bin/env python

import json

import DIRAC
from DIRAC import S_OK, S_ERROR

from DIRAC.Core.Base import Script

Script.setUsageMessage( """
Delete task and all jobs in the task

Usage:
   %s [option] ... [TaskID] ...
""" % Script.scriptName )

Script.parseCommandLine( ignoreErrors = False )
options = Script.getUnprocessedSwitches()
args = Script.getPositionalArgs()

from IHEPDIRAC.WorkloadManagementSystem.Client.TaskClient   import TaskClient
taskClient = TaskClient()

def deleteTask(taskID):
  result = taskClient.deleteTask(taskID)
  if not result['OK']:
    print 'Delete task error: %s' % result['Message']
    return
  print 'Task %s deleted' % taskID

def main():
  if len(args) < 1:
    Script.showHelp()
    return

  for taskID in args:
    taskID = int(taskID)
    deleteTask(taskID)
    print ''

if __name__ == '__main__':
  main()
