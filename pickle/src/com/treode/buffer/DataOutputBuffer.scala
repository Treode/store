package com.treode.buffer

import java.io.DataOutput

class DataOutputBuffer (buffer: OutputBuffer) extends DataOutput {

  def write (v: Int): Unit = buffer.writeByte (v.toByte)
  def writeBoolean (v: Boolean): Unit = buffer.writeByte (if (v) 1 else 0)
  def writeByte (v: Int): Unit = buffer.writeByte (v.toByte)
  def writeChar (v: Int): Unit = buffer.writeShort (v.toShort)
  def writeDouble (v: Double): Unit = buffer.writeDouble (v)
  def writeFloat (v: Float): Unit = buffer.writeFloat (v)
  def writeInt (v: Int): Unit = buffer.writeInt (v)
  def writeLong (v: Long): Unit = buffer.writeLong (v)
  def writeShort (v: Int): Unit = buffer.writeShort (v.toShort)

  def write (data: Array [Byte], offset: Int, length: Int): Unit =
    buffer.writeBytes (data, offset, length)

  def write (data:  Array [Byte]): Unit =
    buffer.writeBytes (data, 0, data.length)

  def writeBytes (v: String): Unit = ???
  def writeChars (v: String): Unit = ???
  def writeUTF (v: String): Unit = ???
}
