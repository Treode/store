package com.treode.buffer

import java.io.DataOutput

private class DataOutputWrapper (out: Output) extends DataOutput {

  def write (v: Int): Unit = out.writeByte (v.toByte)
  def writeBoolean (v: Boolean): Unit = out.writeByte (if (v) 1 else 0)
  def writeByte (v: Int): Unit = out.writeByte (v.toByte)
  def writeChar (v: Int): Unit = out.writeShort (v.toShort)
  def writeDouble (v: Double): Unit = out.writeDouble (v)
  def writeFloat (v: Float): Unit = out.writeFloat (v)
  def writeInt (v: Int): Unit = out.writeInt (v)
  def writeLong (v: Long): Unit = out.writeLong (v)
  def writeShort (v: Int): Unit = out.writeShort (v.toShort)

  def write (data: Array [Byte], offset: Int, length: Int): Unit =
    out.writeBytes (data, offset, length)

  def write (data:  Array [Byte]): Unit =
    out.writeBytes (data, 0, data.length)

  def writeBytes (v: String): Unit = ???
  def writeChars (v: String): Unit = ???
  def writeUTF (v: String): Unit = ???
}
