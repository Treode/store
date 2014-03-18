package com.treode.buffer

import com.google.common.hash.{HashCode, HashFunction}

trait InputBuffer extends Input {

  def readPos: Int
  def readPos_= (pos: Int)
  def readableBytes: Int

  def hash (start: Int, length: Int, hashf: HashFunction): HashCode
}
