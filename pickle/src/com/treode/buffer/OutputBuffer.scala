package com.treode.buffer

trait OutputBuffer extends Output {

  def capacity: Int
  def capacity (length: Int)
  def writePos: Int
  def writePos_= (pos: Int)
  def writeableBytes: Int
}
