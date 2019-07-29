# -*- coding: utf-8 -*-
__RCSID__ = "$Id: $"

from DIRAC import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.Core.Base.AgentModule import AgentModule

global gTransferDB

class TransferAgent(AgentModule):

  def initialize(self):
    self.count = 0

    self.MAX_TRANSFER = self.am_getOption("MAX_TRANSFER", 2)
    self.MAX_RETRY = self.am_getOption("MAX_RETRY", 3)
    gLogger.info("MAX_TRANSFER: ", self.MAX_TRANSFER)

    self.transfer_worker = []

    global gTransferDB
    from IHEPDIRAC.TransferSystem.DB.TransferDB import TransferDB
    from IHEPDIRAC.TransferSystem.Agent.helper import helper_TransferAgent
    gTransferDB = TransferDB()

    self.helper = helper_TransferAgent(self, gTransferDB)

    return S_OK()

  def execute(self):

    gLogger.info("execute: ", self.count)
    self.count += 1

    # Handle the existed transfer worker
    for worker in self.transfer_worker[:]:
      # check worker status
      self.helper.check_worker_status(worker)
      # worker is TransferWorker
      retcode = worker.get_retcode()
      if retcode is not None:
        # Make sure the failed job can retransfer
        worker.info["retransfer"]+=1
        # Handle retcode
        worker.handle_waiting()
        result = worker.handle_exit(retcode)
        
        if result:
          gLogger.error("There is some errors!")
          gLogger.error(result)
          # if we can retransfer, try to retransfer
          if worker.info["retransfer"] < self.MAX_RETRY:
            # retransfer
            gLogger.info("Try to Retransfer (%d) "% worker.info["retransfer"])
            worker.create_popen(worker.info)
            continue
          self.helper.helper_error_report(worker, result)
          worker.info["error"] = result

        self.transfer_worker.remove(worker)
        self.helper.helper_remove_transfer(worker)
      else:
        # the job is not OK
        worker.handle_waiting()
      pass
    else:
      # the transfer work are all working
      pass

    # Create new transfer worker
    idle_worker = self.MAX_TRANSFER - len(self.transfer_worker)
    if idle_worker:
      for i in xrange(idle_worker):
        # append a new worker
        # if can't add, just break
        if not self.add_new_transfer():
          break
        gLogger.info("Add a new Transfer")

    return S_OK()

  def add_new_transfer(self):
    """
      if add a new, return true,
      else return false
    """
    # << Get New File >>
    result = self.helper.helper_get_new_request()
    gLogger.info(result)
    if not result:
      return False

    # << Add New Transfer >>
    return self.helper.helper_add_transfer(result)

