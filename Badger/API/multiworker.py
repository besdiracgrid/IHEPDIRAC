#!/usr/bin/env python
# -*- coding:utf-8 -*-
# author: zhanggang

import threading
import time
import random

class LockedTierator(object):
  def __init__(self, it):
    self.lock = threading.Lock()
    self.it = it.__iter__()

  def __iter__(self): return self

  def next(self):
    self.lock.acquire()
    try:
      return self.it.next()
    finally:
      self.lock.release()

class IWorker(object):

  def get_file_list(self):
    raise NotImplementedError

  def Do(self, item):
    raise NotImplementedError

class MultiWorker(object):

  def __init__(self, worker, pool_size=5):
    self.max_pool_size = pool_size
    self.pool = []
    self.worker = worker
    self.worker.get_file_list = LockedTierator(self.worker.get_file_list())
    self.it = iter(self.worker.get_file_list)

  def check_pool(self):
    return self.max_pool_size - len(self.pool)

  def clear_pool(self):
    for t in self.pool[:]:
      if not t.isAlive():
        self.pool.remove(t)

  def get_file(self):
    try:
      return self.it.next()
    except StopIteration:
      return None
    pass

  def main(self):
    while True:
      self.clear_pool()
      residual = self.check_pool()
      if not residual:
        continue
      for i in range(residual):
        f = self.get_file()
        if f is None and len(self.pool) == 0:
          return

        t = threading.Thread( target=self.worker.Do, args=(f,) )
        self.pool.append(t)
        #t.setDaemon(True)
        t.start()

      #break


if __name__ == "__main__":

  worker = UploadWorker()
  mw = MultiWorker(worker)
  mw.main()
