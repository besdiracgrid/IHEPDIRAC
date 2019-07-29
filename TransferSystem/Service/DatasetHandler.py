# -*- coding: utf-8 -*-

__RCSID__ = "$Id: $"

import datetime

from DIRAC import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.Core.DISET.RequestHandler import RequestHandler

global gTransferDB

def initializeDatasetHandler(serviceInfo):
  """ initialize handler """
  gLogger.info("Initialize Dataset Handler.")

  from IHEPDIRAC.TransferSystem.DB.TransferDB import TransferDB

  global gTransferDB

  gTransferDB = TransferDB()

  return S_OK()

class DatasetHandler(RequestHandler):
  """
  This is for:
    * create a dataset
  """

  def initialize(self):
    credDict = self.getRemoteCredentials()
    gLogger.info(credDict)
    self.user = credDict["username"]

    return S_OK()

  types_create = [str, list]
  def export_create(self, dataset, filelist):
    gLogger.info("Username: ", self.user)
    gLogger.info("Dataset: ", dataset)
    gLogger.info("Filelist: ", filelist)
    res = gTransferDB.insert_Dataset( dataset, self.user, filelist)

    if not res["OK"]:
      return res

    return S_OK()

  types_list = [ str ]
  def export_list(self, dataset):
    gLogger.info("Dataset: ", dataset)
    condDict = {'name': dataset}

    res = gTransferDB.get_Dataset(condDict)
    return res

  types_show = [ dict, [list, str], [int, long], [int, long] ]
  def export_show(self, condDict=None, orderby=None,
                        start=None, limit=None):
    """ show the datasets """
    res = gTransferDB.show_Datasets(condDict, orderby, start, limit)
    return res

  types_showtotal = [ dict ]
  def export_showtotal(self, condDict):
    res = gTransferDB.showtotal_Datasets(condDict)
    return res
