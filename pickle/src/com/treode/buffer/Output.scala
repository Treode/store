package com.treode.buffer

trait Output {

  def writeBytes (data: Array [Byte], offset: Int, length: Int)
  def writeByte (v: Byte)
  def writeShort (v: Short)
  def writeInt (v: Int)
  def writeVarUInt (v: Int)
  def writeVarInt (v: Int)
  def writeLong (v: Long)
  def writeVarULong (v: Long)
  def writeVarLong (v: Long)
  def writeFloat (v: Float)
  def writeDouble (v: Double)
  def writeString (v: String)
}
