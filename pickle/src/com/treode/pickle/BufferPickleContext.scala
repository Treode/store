package com.treode.pickle

import java.io.DataOutput
import com.treode.buffer.Output

private class BufferPickleContext (out: Output) extends PickleContext {

  def writeBytes (v: Array [Byte], offset: Int, length: Int) =
    out.writeBytes (v, offset, length)

  def writeByte (v: Byte) = out.writeByte (v)
  def writeInt (v: Int) = out.writeInt (v)
  def writeVarInt (v: Int) = out.writeVarInt (v)
  def writeVarUInt (v: Int) = out.writeVarUInt (v)
  def writeShort (v: Short) = out.writeShort (v)
  def writeLong (v: Long) = out.writeLong (v)
  def writeVarLong (v: Long) = out.writeVarLong (v)
  def writeVarULong (v: Long) = out.writeVarULong (v)
  def writeFloat (v: Float) = out.writeFloat (v)
  def writeDouble (v: Double) = out.writeDouble (v)
  def writeString (v: String) = out.writeString (v)

  def toDataOutput = Output.asDataOutput (out)
}
