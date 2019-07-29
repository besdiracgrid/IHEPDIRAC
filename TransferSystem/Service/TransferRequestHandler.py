# -*- coding: utf-8 -*-
__RCSID__ = "$Id: $"

import datetime

from DIRAC import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.Core.DISET.RequestHandler import RequestHandler

from IHEPDIRAC.TransferSystem.DB.TransferDB import TransRequestEntry

tmpGlobalStore = {}

global gTransferDB 

def initializeTransferRequestHandler(serviceInfo):
  """ initialize handler """

  gLogger.info("Initialize TransferRequestHandler.")

  from IHEPDIRAC.TransferSystem.DB.TransferDB import TransferDB

  global gTransferDB

  gTransferDB = TransferDB()

  return S_OK()

class TransferRequestHandler(RequestHandler):
  """
  This is for:
    * create a request to transfer data
  """

  def initialize(self):
    credDict = self.getRemoteCredentials()
    gLogger.info(credDict)
    self.user = credDict["username"]

    tmpGlobalStore.setdefault( self.user, 
                              {"endpoint":{}
                              } )

  types_serverIsOK = []
  def export_serverIsOK(self):
    return S_OK()

  types_getEndPoint = []
  def export_getEndPoint(self):
    ep = tmpGlobalStore[self.user]["endpoint"]

    return S_OK( ep )

  types_setEndPoint = [str, str]
  def export_setEndPoint(self, name, url):
    gLogger.info(name)
    gLogger.info(url)

    tmpGlobalStore[self.user]["endpoint"][name] = url

    return S_OK()

  def check_create_permission(self):
    credDict = self.getRemoteCredentials()
    if credDict["group"] in ["data_transfer"]:
      return True
    return False
    
  types_create = [ str, str, str, str ]
  def export_create(self, dataset, ep_from, ep_to, protocol):
    # check whether the user can create a request

    if not self.check_create_permission():
      return S_ERROR("The user can't create transfer request")
    entry = TransRequestEntry(username = self.user, 
                              dataset = dataset,
                              srcSE = ep_from,
                              dstSE = ep_to,
                              protocol = protocol,
                              status = "new",
                              submit_time = datetime.datetime.utcnow())
    gLogger.info("create an Entry:", entry)
    res = gTransferDB.insert_TransferRequest(entry)
    return res

  types_status = [ dict ]
  def export_status(self, condDict=None):
    """ This is give the status of the request db
    """
    res = gTransferDB.get_TransferRequest(condDict)
    return res

  types_statustotal = [ dict ]
  def export_statustotal(self, condDict=None):
    """ This is give the status of the request db
    """
    res = gTransferDB.get_TransferRequestTotal(condDict)
    return res

  types_statuslimit = [ dict, [list, str], [int, long], [int, long] ]
  def export_statuslimit(self, condDict=None, orderby=None, 
                               offset=None, limit=None):
    """ This is give the status of the request db
    """
    res = gTransferDB.get_TransferRequestWithLimit(condDict, orderby, offset, limit)
    return res

  types_show = [ dict ]
  def export_show(self, condDict):
    """ This is for query the file list.
    """
    res = gTransferDB.get_TransferFileList(condDict)
    return res

  types_delete = [ dict ]
  def export_delete(self, condDict):
    """ This will make the status to kill
         (new, transfer) --> kill
    """ 
    res = gTransferDB.delete_TransferFileList(condDict)
    return res

  types_delete_files_in_req = [ dict ]
  def export_delete_files_in_req(self, condDict):
    res = gTransferDB.delete_TransferFileListByReq(condDict)
    return res

  types_retransfer = [ dict ]
  def export_retransfer(self, condDict):
    """ This will make the status to new
         (kill, finish) --> new
    """ 
    res = gTransferDB.retransfer_TransferFileList(condDict)
    return res
