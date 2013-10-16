package com.treode.pickle

import com.esotericsoftware.kryo.io.Input

private class KryoUnpickleContext (buffer: Input) extends UnpickleContext {

  def readBytes (data: Array [Byte], offset: Int, length: Int) =
    buffer.readBytes (data, offset, length)

  def readByte() = buffer.readByte()
  def readShort() = buffer.readShort()
  def readInt() = buffer.readInt()
  def readVarInt() = buffer.readInt (false)
  def readVarUInt() = buffer.readInt (true)
  def readLong() = buffer.readLong()
  def readVarLong() = buffer.readLong (false)
  def readVarULong() = buffer.readLong (true)
  def readFloat() = buffer.readFloat()
  def readDouble() = buffer.readDouble()
  def readString() = buffer.readString()
}
