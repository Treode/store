package com.treode.pickle

import com.esotericsoftware.kryo.io.Output

private class KryoPickleContext (output: Output) extends PickleContext {

  def writeBytes (v: Array [Byte], offset: Int, length: Int) =
    output.writeBytes (v, offset, length)

  def writeByte (v: Byte) = output.writeByte (v)
  def writeInt (v: Int) = output.writeInt (v)
  def writeVarInt (v: Int) = output.writeVarInt (v, false)
  def writeVarUInt (v: Int) = output.writeVarInt (v, true)
  def writeShort (v: Short) = output.writeShort (v)
  def writeLong (v: Long) = output.writeLong (v)
  def writeVarLong (v: Long) = output.writeLong (v, false)
  def writeVarULong (v: Long) = output.writeLong (v, true)
  def writeFloat (v: Float) = output.writeFloat (v)
  def writeDouble (v: Double) = output.writeDouble (v)
  def writeString (v: String) = output.writeString (v)
}
