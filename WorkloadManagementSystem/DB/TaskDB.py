#

import json

from DIRAC                import gConfig, S_OK, S_ERROR
from DIRAC.Core.Utilities import Time
from DIRAC.Core.Base.DB   import DB


__RCSID__ = "$Id: TaskDB.py 1 2015-01-11 11:39:29 zhaoxh@ihep.ac.cn $"

class TaskDB( DB ):

  def __init__( self, maxQueueSize = 10 ):
    DB.__init__( self, 'TaskDB', 'WorkloadManagement/TaskDB', maxQueueSize )

    result = self.__initializeDB()
    if not result[ 'OK' ]:
      raise Exception( "Can't create tables: %s" % result[ 'Message' ] )

  def __initializeDB( self ):
    """
    Create the tables
    """
    result = self._query( "show tables" )
    if not result[ 'OK' ]:
      return result

    tablesInDB = [ t[0] for t in result[ 'Value' ] ]
    tablesToCreate = {}
    self.__tablesDesc = {}

    self.__tablesDesc[ 'Task' ] = { 'Fields' : { 'TaskID'       : 'BIGINT UNSIGNED AUTO_INCREMENT NOT NULL',
                                                 'TaskName'     : 'VARCHAR(128) NOT NULL DEFAULT "unknown"',
                                                 'CreationTime' : 'DATETIME NOT NULL',
                                                 'UpdateTime'   : 'DATETIME NOT NULL',
                                                 'Status'       : 'VARCHAR(64) NOT NULL DEFAULT "Unknown"',
                                                 'Owner'        : 'VARCHAR(64) NOT NULL DEFAULT "unknown"',
                                                 'OwnerDN'      : 'VARCHAR(255) NOT NULL DEFAULT "unknown"',
                                                 'OwnerGroup'   : 'VARCHAR(128) NOT NULL DEFAULT "unknown"',
                                                 'Site'         : 'VARCHAR(512) NOT NULL DEFAULT ""',
                                                 'JobGroup'     : 'VARCHAR(512) NOT NULL DEFAULT ""',
                                                 'Progress'     : 'VARCHAR(128) NOT NULL DEFAULT "{}"',
                                                 'Info'         : 'TEXT NOT NULL',
                                               },
                                    'PrimaryKey' : 'TaskID',
                                    'Indexes': { 'TaskIDIndex'       : [ 'TaskID' ],
                                                 'CreationTimeIndex' : [ 'CreationTime' ],
                                                 'UpdateTimeIndex'   : [ 'UpdateTime' ],
                                                 'StatusIndex'       : [ 'Status' ],
                                                 'OwnerIndex'        : [ 'Owner' ],
                                                 'OwnerDNIndex'      : [ 'OwnerDN' ],
                                                 'OwnerGroupIndex'   : [ 'OwnerGroup' ],
                                               }
                                  }

    self.__tablesDesc[ 'TaskHistory' ] = { 'Fields' : { 'TaskHistoryID' : 'BIGINT UNSIGNED AUTO_INCREMENT NOT NULL',
                                                        'TaskID'        : 'BIGINT UNSIGNED NOT NULL DEFAULT 0',
                                                        'Status'        : 'VARCHAR(64) NOT NULL DEFAULT "Unknown"',
                                                        'StatusTime'    : 'DATETIME NOT NULL',
                                                        'Description'   : 'VARCHAR(128) NOT NULL DEFAULT ""',
                                                      },
                                           'PrimaryKey' : 'TaskHistoryID',
                                           'Indexes': { 'TaskHistoryIDIndex' : [ 'TaskHistoryID' ],
                                                        'TaskIDIndex'        : [ 'TaskID' ],
                                                      }
                                         }

    self.__tablesDesc[ 'TaskJob' ] = { 'Fields' : { 'TaskJobID'    : 'BIGINT UNSIGNED AUTO_INCREMENT NOT NULL',
                                                    'TaskID'       : 'BIGINT UNSIGNED NOT NULL DEFAULT 0',
                                                    'JobID'        : 'BIGINT UNSIGNED UNIQUE NOT NULL DEFAULT 0',
                                                    'Info'         : 'TEXT NOT NULL',
                                                  },
                                       'PrimaryKey' : 'TaskJobID',
                                       'Indexes': { 'TaskJobIDIndex' : [ 'TaskJobID' ],
                                                    'TaskIDIndex'    : [ 'TaskID' ],
                                                    'JobIDIndex'     : [ 'JobID' ],
                                                  }
                                     }

    for tableName in self.__tablesDesc:
      if not tableName in tablesInDB:
        tablesToCreate[ tableName ] = self.__tablesDesc[ tableName ]

    return self._createTables( tablesToCreate )

################################################################################

  def createTask( self, taskName, status, owner, ownerDN, ownerGroup, taskInfo = {} ):
    taskAttrNames = ['TaskName', 'CreationTime', 'UpdateTime', 'Status', 'Owner', 'OwnerDN', 'OwnerGroup', 'Info']
    taskAttrValues = [taskName, Time.dateTime(), Time.dateTime(), status, owner, ownerDN, ownerGroup, json.dumps(taskInfo, separators=(',',':'))]

    result = self.insertFields( 'Task', taskAttrNames, taskAttrValues )
    if not result['OK']:
      self.log.error( 'Can not create new task', result['Message'] )
      return result

    if 'lastRowId' not in result:
      return S_ERROR( 'Failed to retrieve a new ID for task' )

    taskID = int( result['lastRowId'] )

    self.log.info( 'TaskDB: New TaskID served "%s"' % taskID )

    return S_OK( taskID )

  def addTaskJob( self, taskID, jobID, jobInfo ):
    taskJobAttrNames = ['TaskID', 'JobID', 'Info']
    taskJobAttrValues = [taskID, jobID, json.dumps(jobInfo, separators=(',',':'))]

    result = self.insertFields( 'TaskJob', taskJobAttrNames, taskJobAttrValues )
    if not result['OK']:
      self.log.error( 'Can not add job to task %s' % taskID, result['Message'] )

    return result

  def insertTaskHistory( self, taskID, status, description = '' ):
    taskHistoryAttrNames = ['TaskID', 'Status', 'StatusTime', 'Description']
    taskHistoryAttrValues = [taskID, status, Time.dateTime(), description]

    result = self.insertFields( 'TaskHistory', taskHistoryAttrNames, taskHistoryAttrValues )
    if not result['OK']:
      self.log.error( 'Can not insert task history to task %s' % taskID, result['Message'] )

    return result

  def updateTask( self, taskID, taskAttrNames, taskAttrValues ):
    condDict = { 'TaskID': taskID }

    if 'UpdateTime' not in taskAttrNames:
      taskAttrNames.append( 'UpdateTime' )
      taskAttrValues.append( Time.dateTime() )

    result = self.updateFields( 'Task', taskAttrNames, taskAttrValues, condDict )
    if not result['OK']:
      self.log.error( 'Can not update task %s' % taskID, result['Message'] )

    return result

  def updateTaskStatus( self, taskID, status, description ):
    result = self.updateTask( taskID, ['Status'], [status] )
    if not result['OK']:
      return result
    result = self.insertTaskHistory( taskID, status, description )
    if not result['OK']:
      return result

    return S_OK( status )

  def updateTaskProgress( self, taskID, progress ):
    condDict = { 'TaskID': taskID }
    taskAttrNames = ['Progress']
    taskAttrValues = [json.dumps(progress, separators=(',',':'))]

    result = self.updateFields( 'Task', taskAttrNames, taskAttrValues, condDict )
    if not result['OK']:
      self.log.error( 'Can not update task progress for task %s' % taskID, result['Message'] )

    return result

  def updateTaskInfo( self, taskID, taskInfo ):
    condDict = { 'TaskID': taskID }
    taskAttrNames = ['Info']
    taskAttrValues = [json.dumps(taskInfo, separators=(',',':'))]

    result = self.updateFields( 'Task', taskAttrNames, taskAttrValues, condDict )
    if not result['OK']:
      self.log.error( 'Can not update task info for task %s' % taskID, result['Message'] )

    return result

  def renameTask( self, taskID, newName ):
    condDict = { 'TaskID': taskID }
    taskAttrNames = ['TaskName']
    taskAttrValues = [newName]

    result = self.updateFields( 'Task', taskAttrNames, taskAttrValues, condDict )
    if not result['OK']:
      self.log.error( 'Can not rename task %s' % taskID, result['Message'] )

    return result

################################################################################

  def getDistinctTaskAttributes( self, attribute, condDict = None, older = None,
                                newer = None, timeStamp = 'UpdateTime' ):
    """ Get distinct values of the task attribute under specified conditions
    """
    return self.getDistinctAttributeValues( 'Task', attribute, condDict = condDict,
                                              older = older, newer = newer, timeStamp = timeStamp )

  def getTaskCount( self, condDict ):
    newer = None
    if 'FromDate' in condDict:
      newer = condDict['FromDate']
      del condDict['FromDate']
    older = None
    if 'ToDate' in condDict:
      older = condDict['ToDate']
      del condDict['ToDate']

    result = self.getCounters( 'Task', ['Status'], condDict, newer = newer, older = older, timeStamp = 'UpdateTime' )
    if not result['OK']:
      self.log.error( 'Can not get task count', result['Message'] )
      return result

    nTasks = 0
    for stDict, count in result['Value']:
      nTasks += count

    return S_OK( nTasks )

  def getTasks( self, outFields, condDict, limit = None, offset = None, orderAttribute = None ):
    newer = None
    if 'FromDate' in condDict:
      newer = condDict['FromDate']
      del condDict['FromDate']
    older = None
    if 'ToDate' in condDict:
      older = condDict['ToDate']
      del condDict['ToDate']

    result = self.getFields( 'Task', outFields, condDict, newer = newer, older = older, timeStamp = 'UpdateTime', limit = (limit, offset), orderAttribute = orderAttribute )
    if not result['OK']:
      self.log.error( 'Can not get task list', result['Message'] )
      return result

    return S_OK( result['Value'] )

  def getAttributesForTaskList( self, taskIDs, attrList = None ):
    """ Get attributes for the jobs in the the jobIDList.
        Returns an S_OK structure with a dictionary of dictionaries as its Value:
        ValueDict[jobID][attribute_name] = attribute_value
    """
    if not taskIDs:
      return S_OK( {} )
    if attrList:
      attrNames = ',' + ','.join( [ str( x ) for x in attrList ] )
      attr_tmp_list = attrList
    else:
      attrNames = ''
      attr_tmp_list = []
    taskList = ','.join( [str( x ) for x in taskIDs] )

    # FIXME: need to check if the attributes are in the list of task Attributes

    cmd = 'SELECT TaskID%s FROM Task WHERE TaskID in ( %s )' % ( attrNames, taskList )
    res = self._query( cmd )
    if not res['OK']:
      return res
    try:
      retDict = {}
      for retValues in res['Value']:
        taskID = retValues[0]
        taskDict = {}
        taskDict[ 'TaskID' ] = taskID
        attrValues = retValues[1:]
        for i in range( len( attr_tmp_list ) ):
          try:
            taskDict[attr_tmp_list[i]] = attrValues[i].tostring()
          except Exception:
            taskDict[attr_tmp_list[i]] = str( attrValues[i] )
        retDict[int( taskID )] = taskDict
      return S_OK( retDict )
    except Exception, x:
      return S_ERROR( 'TaskDB.getAttributesForTaskList: Failed\n%s' % str( x ) )

  def getTask( self, taskID, outFields ):
    condDict = { 'TaskID': taskID }
    result = self.getFields( 'Task', outFields, condDict )
    if not result['OK']:
      self.log.error( 'Can not get task %s' % taskID, result['Message'] )
      return result

    if not result['Value']:
      self.log.error( 'Task ID %d not found' % taskID )
      return S_ERROR( 'Task ID %d not found' % taskID )

    return S_OK( result['Value'][0] )

  def getTaskStatus( self, taskID ):
    outFields = ( 'Status', )
    result = self.getTask( taskID, outFields )
    if not result['OK']:
      self.log.error( 'Can not get task status for task %s' % taskID, result['Message'] )
      return result

    return S_OK( result['Value'][0] )

  def getTaskInfo( self, taskID ):
    outFields = ( 'Info', )
    result = self.getTask( taskID, outFields )
    if not result['OK']:
      self.log.error( 'Can not get task info for task %s' % taskID, result['Message'] )
      return result

    return S_OK( json.loads( result['Value'][0] ) )

  def getTaskHistories( self, taskID ):
    condDict = { 'TaskID': taskID }
    outFields = ( 'Status', 'StatusTime', 'Description' )
    result = self.getFields( 'TaskHistory', outFields, condDict )
    if not result['OK']:
      self.log.error( 'Can not get task histories for task %s' % taskID, result['Message'] )
      return result

    return S_OK( result['Value'] )

  def getTaskJobs( self, taskID ):
    condDict = { 'TaskID': taskID }
    outFields = ( 'JobID', )
    result = self.getFields( 'TaskJob', outFields, condDict, orderAttribute = 'JobID:ASC' )
    if not result['OK']:
      self.log.error( 'Can not get task jobs for task %s' % taskID, result['Message'] )
      return result

    return S_OK( [ i[0] for i in  result['Value'] ] )

  def getJobs( self, jobIDs, outFields ):
    if not jobIDs:
      return S_OK( [] )

    condDict = { 'JobID': jobIDs }
    result = self.getFields( 'TaskJob', outFields, condDict )
    if not result['OK']:
      self.log.error( 'Can not get task ID for job ID %s' % jobIDs, result['Message'] )
      return result

    return S_OK( result['Value'] )

  def getTaskIDFromJob( self, jobID ):
    condDict = { 'JobID': jobID }
    outFields = ( 'TaskID', )
    result = self.getFields( 'TaskJob', outFields, condDict )
    if not result['OK']:
      self.log.error( 'Can not get task ID for job %s' % jobID, result['Message'] )
      return result

    return S_OK( [ i[0] for i in  result['Value'] ] )

  def getJobInfo( self, jobID ):
    condDict = { 'JobID': jobID }
    outFields = ( 'Info', )
    result = self.getFields( 'TaskJob', outFields, condDict )
    if not result['OK']:
      self.log.error( 'Can not get job info for job %s' % jobID, result['Message'] )
      return result

    if not result['Value']:
      self.log.error( 'Job info %d not found' % jobID )
      return S_ERROR( 'Job info %d not found' % jobID )

    return S_OK( json.loads( result['Value'][0][0] ) )
