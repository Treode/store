package com.treode.pickle

import com.treode.buffer.Input

private class BufferUnpickleContext (buffer: Input) extends UnpickleContext {

  def readBytes (data: Array [Byte], offset: Int, length: Int) =
    buffer.readBytes (data, offset, length)

  def readByte() = buffer.readByte()
  def readShort() = buffer.readShort()
  def readInt() = buffer.readInt()
  def readVarInt() = buffer.readVarInt()
  def readVarUInt() = buffer.readVarUInt()
  def readLong() = buffer.readLong()
  def readVarLong() = buffer.readVarLong()
  def readVarULong() = buffer.readVarULong()
  def readFloat() = buffer.readFloat()
  def readDouble() = buffer.readDouble()
  def readString() = buffer.readString()
}
