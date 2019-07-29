#!/usr/bin/env python

import zlib

class Adler32CheckSum():
  @staticmethod
  def checksum(filename, blocksize=1048576):
    value = 1
    with open(filename, "rb") as f:
      for block in iter(lambda: f.read(blocksize), ""):
        value = zlib.adler32(block, value)
    return hex(value & 0xffffffff).lower().replace('l','').replace('x','0')[-8:]
