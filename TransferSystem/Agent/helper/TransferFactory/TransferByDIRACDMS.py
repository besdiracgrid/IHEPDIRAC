# -*- coding: utf-8 -*-

from IHEPDIRAC.TransferSystem.Agent.helper.TransferFactory.ITransferWorker import *

class DIRACDMSTransferWorker(ITransferWorker):

  def build_cmd(self, info):
    LFN = info["LFN"]
    srcSE = info["srcSE"]
    dstSE = info["dstSE"]

    cmd_list = [#"dirac-dms-fts-submit", 
                  "dirac-dms-replicate-lfn",
                  LFN,
                  dstSE,
                  srcSE,
                  "-c",
                  "y"]
    return cmd_list

  def handle_exit(self, returncode):
    if returncode is None:
      return
    if returncode != 0:
      return  self._buffer 

  def handle_line(self, line):
    return line

if __name__ == "__main__":

  import DIRAC
  from DIRAC.Core.Base import Script
  Script.parseCommandLine( ignoreErrors = True )

  dtw = DIRACDMSTransferWorker()
  info = {"LFN":"/users/l/lintao/README_IHEPD",
          "srcSE": "IHEP-USER",
          "dstSE": "JINR-USER"}
  dtw.create_popen(info)

  returncode = dtw.get_retcode()
  while returncode is None:
    time.sleep(0.2)
    dtw.handle_waiting()
    returncode = dtw.get_retcode()
  else:
    dtw.handle_waiting()
    print dtw.handle_exit(returncode)

  print "Work Done."
