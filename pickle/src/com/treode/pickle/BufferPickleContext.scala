package com.treode.pickle

import java.io.DataOutput
import com.treode.buffer.{DataOutputBuffer, OutputBuffer}

private class BufferPickleContext (buffer: OutputBuffer) extends PickleContext {

  def writeBytes (v: Array [Byte], offset: Int, length: Int) =
    buffer.writeBytes (v, offset, length)

  def writeByte (v: Byte) = buffer.writeByte (v)
  def writeInt (v: Int) = buffer.writeInt (v)
  def writeVarInt (v: Int) = buffer.writeVarInt (v)
  def writeVarUInt (v: Int) = buffer.writeVarUInt (v)
  def writeShort (v: Short) = buffer.writeShort (v)
  def writeLong (v: Long) = buffer.writeLong (v)
  def writeVarLong (v: Long) = buffer.writeVarLong (v)
  def writeVarULong (v: Long) = buffer.writeVarULong (v)
  def writeFloat (v: Float) = buffer.writeFloat (v)
  def writeDouble (v: Double) = buffer.writeDouble (v)
  def writeString (v: String) = buffer.writeString (v)

  def toDataOutput: DataOutput =
    new DataOutputBuffer (buffer)
}
