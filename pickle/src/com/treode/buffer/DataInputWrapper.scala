package com.treode.buffer

import java.io.DataInput

private class DataInputWrapper (in: Input) extends DataInput {

  def readBoolean(): Boolean = in.readByte() != 0
  def readByte(): Byte = in.readByte()
  def readChar(): Char = in.readShort().toChar
  def readDouble(): Double = in.readDouble()
  def readFloat(): Float = in.readFloat()
  def readInt(): Int = in.readInt()
  def readLong(): Long = in.readLong()
  def readShort(): Short = in.readShort()
  def readUnsignedByte(): Int = in.readByte().toInt & 0xFF
  def readUnsignedShort(): Int = in.readShort().toInt & 0xFFFF

  def readFully (data: Array [Byte], offset: Int, length: Int): Unit =
    in.readBytes (data, offset, length)

  def readFully (data: Array [Byte]): Unit =
    in.readBytes (data, 0, data.length)

  def skipBytes (length: Int): Int = ???
  def readLine(): String = ???
  def readUTF(): String = ???
}
