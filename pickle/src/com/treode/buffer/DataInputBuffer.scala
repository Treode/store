package com.treode.buffer

import java.io.DataInput

class DataInputBuffer (buffer: InputBuffer) extends DataInput {

  def readBoolean(): Boolean = buffer.readByte() != 0
  def readByte(): Byte = buffer.readByte()
  def readChar(): Char = buffer.readShort().toChar
  def readDouble(): Double = buffer.readDouble()
  def readFloat(): Float = buffer.readFloat()
  def readInt(): Int = buffer.readInt()
  def readLong(): Long = buffer.readLong()
  def readShort(): Short = buffer.readShort()
  def readUnsignedByte(): Int = buffer.readByte().toInt & 0xFF
  def readUnsignedShort(): Int = buffer.readShort().toInt & 0xFFFF

  def readFully (data: Array [Byte], offset: Int, length: Int): Unit =
    buffer.readBytes (data, offset, length)

  def readFully (data: Array [Byte]): Unit =
    buffer.readBytes (data, 0, data.length)

  def skipBytes (length: Int): Int = {
    val n = math.min (length, buffer.readableBytes)
    buffer.readPos += n
    n
  }

  def readLine(): String = ???
  def readUTF(): String = ???
}
