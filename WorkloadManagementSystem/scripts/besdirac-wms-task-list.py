#!/usr/bin/env python

import DIRAC
from DIRAC import S_OK, S_ERROR

from DIRAC.Core.Base import Script

Script.setUsageMessage( """
List all the tasks

Usage:
   %s [option]
""" % Script.scriptName )

Script.registerSwitch( "f:", "field=",          "Fields to list, seperated by comma. (Owner,OwnerDN,OwnerGroup,CreationTime,Progress,JobGroup,Site)" )
Script.registerSwitch( "w",  "whole",           "List all the fields" )
Script.registerSwitch( "l:", "limit=",          "Task number limit" )
Script.registerSwitch( "e",  "expired",         "Include expired tasks" )
Script.registerSwitch( "r",  "removed",         "Include removed tasks (only for admin)" )
Script.registerSwitch( "u",  "users",           "Include other users' tasks (only for admin)" )
Script.registerSwitch( "a",  "all",             "Show all tasks" )

Script.parseCommandLine( ignoreErrors = False )
options = Script.getUnprocessedSwitches()
args = Script.getPositionalArgs()

from DIRAC.Core.Security.ProxyInfo                         import getProxyInfo
from IHEPDIRAC.WorkloadManagementSystem.Client.TaskClient   import TaskClient
taskClient = TaskClient()

fieldTitle = {
  'TaskID'      : 'TaskID',
  'TaskName'    : 'TaskName',
  'Status'      : 'Status',
  'Owner'       : 'Owner',
  'OwnerDN'     : 'OwnerDN',
  'OwnerGroup'  : 'OwnerGroup',
  'CreationTime': 'CreationTime',
  'UpdateTime'  : 'UpdateTime',
  'Progress'    : 'Progress(T/(D|F|R|W|O))',
  'Site'        : 'Site',
  'JobGroup'    : 'JobGroup',
}

def showTable(title, content):
  widths = []
  column = 0
  for t in title:
    width = len(t)
    for c in content:
      width = max(width, len(c[column]))
    widths.append(width+1)
    column += 1

  showTitle(title, widths)
  showLine(widths)
  showContent(content, widths)

def showTitle(title, widths):
  for t,w in zip(title, widths):
    format = '%%-%ds' % w
    print format % t,
  print ''

def showLine(widths):
  for w in widths:
    print '-'*w,
  print ''

def showContent(content, widths):
  for con in content:
    for c,w in zip(con, widths):
      format = '%%-%ds' % w
      print format % c,
    print ''

def getTitle(fields):
  title = []
  for field in fields:
    title.append(fieldTitle[field])
  return title

def getTasks(fields, tasks):
  content = []
  for task in tasks:
    line = []
    for field in fields:
      if field == 'Progress':
        prog = task[field]
        progStr = '%s/(%s|%s|%s|%s|%s)' \
          % (prog.get('Total', 0), prog.get('Done', 0), prog.get('Failed', 0),
             prog.get('Running', 0), prog.get('Waiting', 0), prog.get('Deleted', 0))
        line.append(progStr)
      else:
        line.append(str(task[field]))
    content.append(line)
  return content

def main():
  basicFields = ['TaskID','TaskName','Status']

  fields = ['Owner','OwnerGroup','CreationTime','Progress']
  limit = -1
  showExpired = False
  showOtherUser = False
  showRemoved = False
  for option in options:
    (switch, val) = option
    if switch == 'f' or switch == 'field':
      fields = val.split(',')
    if switch == 'w' or switch == 'whole':
      fields = ['Owner','OwnerGroup','CreationTime','UpdateTime','Progress','JobGroup','Site']
    if switch == 'l' or switch == 'limit':
      limit = int(val)
    if switch == 'e' or switch == 'expired':
      showExpired = True
    if switch == 'r' or switch == 'removed':
      showRemoved = True
    if switch == 'u' or switch == 'user':
      showOtherUser = True
    if switch == 'a' or switch == 'all':
      showExpired = True
      showOtherUser = True
      showRemoved = True

  fields = basicFields + fields;

  condDict = {}
  condDict['Status'] = ['Init', 'Ready', 'Processing', 'Finished']
  if showExpired:
    condDict['Status'].append('Expired')
  if showRemoved:
    condDict['Status'].append('Removed')

  if not showOtherUser:
    result = getProxyInfo()
    if result['OK']:
      condDict['OwnerDN'] = result['Value']['identity']

  orderAttribute = 'TaskID:DESC'

  result = taskClient.listTask(condDict, limit, 0, orderAttribute)
  if not result['OK']:
    print 'Get task list error: %s' % result['Message']
    return

  # Task value
  taskList = list(result['Value'])
  taskList.reverse()

  title = getTitle(fields)
  content = getTasks(fields, taskList)
  showTable(title, content)

if __name__ == '__main__':
  main()
