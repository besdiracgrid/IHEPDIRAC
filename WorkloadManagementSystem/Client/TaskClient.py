
from DIRAC                                import S_OK, S_ERROR
from DIRAC.Core.DISET.RPCClient           import RPCClient

class TaskClient( object ):
  """
      The task manager client
  """

  def __init__( self ):
    self.__taskManager = RPCClient('WorkloadManagement/TaskManager')
    self.__jobManager = RPCClient('WorkloadManagement/JobManager')
    self.__jobMonitor = RPCClient('WorkloadManagement/JobMonitoring')


  def createTask( self, taskName, taskInfo, jobInfos ):
    return self.__taskManager.createTask( taskName, taskInfo, jobInfos )


  def getTaskCount( self, condDict ):
    return self.__taskManager.getTaskCount( condDict )

  def listTask( self, condDict, limit, offset, orderAttribute ):
    return self.__taskManager.getTasks( condDict, limit, offset, orderAttribute, 1 )


  def renameTask( self, taskID, newName ):
    return self.__taskManager.renameTask( taskID, newName )

  def getTaskOwners( self ):
    return self.__taskManager.getTaskOwners()

  def getTaskOwnerGroups( self ):
    return self.__taskManager.getTaskOwnerGroups()

  def getTask( self, taskID ):
    return self.__taskManager.getTask( taskID, 1 )

  def getTaskProgress( self, taskID ):
    return self.__taskManager.getTaskProgress( taskID )

  def getTaskInfo( self, taskID ):
    return self.__taskManager.getTaskInfo( taskID )

  def getTaskJobs( self, taskID ):
    return self.__taskManager.getTaskJobs( taskID )

  def getTaskJobsStatistics( self, taskID, statusType ):
    return self.__taskManager.getTaskJobsStatistics( taskID, statusType )

  def getTaskHistories( self, taskID ):
    return self.__taskManager.getTaskHistories( taskID )


  def getJobs( self, jobIDs, outFields ):
    return self.__taskManager.getJobs( jobIDs, outFields )


  def rescheduleTask( self, taskID, jobStatus=[] ):
    return self.__manageTask( taskID, self.__jobManager.rescheduleJob, jobStatus )


  def deleteTask( self, taskID, jobStatus=[] ):
    result = self.__manageTask( taskID, self.__jobManager.deleteJob, jobStatus )
    if not result['OK']:
      return result
    jobIDs = result['Value']['JobID']

    result = self.__taskManager.removeTask( taskID )
    if not result['OK']:
      return S_ERROR( 'Remove task error: %s' % result['Message'] )

    return S_OK( {'TaskID': taskID, 'JobID': jobIDs} )


  def __manageTask( self, taskID, action, jobStatus=[] ):
    result = self.__taskManager.getTaskJobs( taskID )
    if not result['OK']:
      return S_ERROR( 'Get task jobs error: %s' % result['Message'] )
    jobIDs = result['Value']

    if jobStatus:
      result = self.__jobMonitor.getJobs( { 'JobID': jobIDs, 'Status': jobStatus } )
      if not result['OK']:
        return S_ERROR( 'Get jobs of status %s error: %s' % (status, result['Message']) )
      jobIDs = result['Value']

    if jobIDs:
      result = action( jobIDs )
      if not result['OK']:
        return S_ERROR( 'Manage jobs error (%s): %s' % (action.__name__, result['Message']) )

    return S_OK( {'TaskID': taskID, 'JobID': jobIDs} )
