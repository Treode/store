package com.treode.pickle

private class SizingPickleContext extends PickleContext {

  private [this] var size = 0

  def result: Int = size

  def writeBytes (v: Array [Byte], offset: Int, length: Int) = size += length
  def writeByte (v: Byte) = size += 1
  def writeInt (v: Int) = size += 4
  def writeVarInt (v: Int) = size += 5
  def writeVarUInt (v: Int) = size += 5
  def writeShort (v: Short) = size += 2
  def writeLong (v: Long) = size += 8
  def writeVarLong (v: Long) = size += 9
  def writeVarULong (v: Long) = size += 9
  def writeFloat (v: Float) = size += 4
  def writeDouble (v: Double) = size += 8
  def writeString (v: String) = size += (5 + 2 * v.length)
}
