#!/usr/bin/env python

import hashlib

class Md5CheckSum():
  @staticmethod
  def checksum(filename, blocksize=1048576):
    hash = hashlib.md5()
    with open(filename, "rb") as f:
      for block in iter(lambda: f.read(blocksize), ""):
        hash.update(block)
    return hash.hexdigest()
