#!/usr/bin/env python
# -*- coding:utf-8 -*-
# author: lintao

import subprocess
import time
import select
import sys

import DIRAC
from DIRAC import gLogger

class ITransferWorker(object):
  """
  This is an Interface.
  """

  def __init__(self):
    self._proc = None
    self._buffer = ""

  @property
  def proc(self):
    return self._proc

  @property
  def info(self):
    return self._info

  def get_retcode(self):
    return self.proc.poll()

  def create_popen(self, info):
    cmd = self.build_cmd(info)
    gLogger.info("The info to create a command:")
    gLogger.info(info)
    gLogger.info("build command:")
    gLogger.info(cmd)

    self._info = info
    self._proc = subprocess.Popen(cmd, 
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
        )
    # Make sure that the stdout is to PIPE.

  def build_cmd(self, info):
    raise NotImplementedError

  def handle_exit(self, returncode):
    raise NotImplementedError

  def handle_stream(self, stream):
    buffer_line = ""
    try:
      for line in stream:
        buffer_line += self.handle_line(line)
    except:
      pass
    finally:
      return buffer_line

  def handle_line(self, line):
    raise NotImplementedError

  def handle_waiting(self):
    if not self._proc:
      return

    r,w,x = select.select( [self._proc.stdout, self._proc.stderr], 
                           [], 
                           [self._proc.stdout, self._proc.stderr]
                           , 1 )
    for out in r:
      # ready for reading
      self._buffer += self.handle_stream( out )
    for out in x:
      # exceptional condition
      self._buffer += self.handle_stream( out )

class DemoTransferWorker(ITransferWorker):

  # Interface

  def build_cmd(self, info):
    return info

  def handle_exit(self, returncode):
    if returncode is None:
      return
    if returncode != 0:
      print "some error happens."
      print "work done"

  def handle_line(self, line):
    return line


if __name__ == "__main__":

  dtw = DemoTransferWorker()

  dtw.create_popen(["sleep", "5"])

  returncode = dtw.get_retcode()
  while returncode is None:
    time.sleep(0.2)
    dtw.handle_waiting()
    returncode = dtw.get_retcode()
  else:
    dtw.handle_exit(returncode)

  print "Work Done."


