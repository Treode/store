package com.treode.buffer

trait Input {

  def readBytes (data: Array [Byte], offset: Int, length: Int)
  def readByte(): Byte
  def readShort(): Short
  def readInt(): Int
  def readVarUInt(): Int
  def readVarInt(): Int
  def readLong(): Long
  def readVarULong(): Long
  def readVarLong(): Long
  def readFloat(): Float
  def readDouble(): Double
  def readString(): String
}
