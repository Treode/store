package com.treode.buffer

trait InputBuffer extends Input {

  def readPos: Int
  def readPos_= (pos: Int)
  def readableBytes: Int
}
