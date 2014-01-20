package com.treode.pickle

import java.io.DataInput
import com.treode.buffer.Input

private class BufferUnpickleContext (in: Input) extends UnpickleContext {

  def readBytes (data: Array [Byte], offset: Int, length: Int) =
    in.readBytes (data, offset, length)

  def readByte() = in.readByte()
  def readShort() = in.readShort()
  def readInt() = in.readInt()
  def readVarInt() = in.readVarInt()
  def readVarUInt() = in.readVarUInt()
  def readLong() = in.readLong()
  def readVarLong() = in.readVarLong()
  def readVarULong() = in.readVarULong()
  def readFloat() = in.readFloat()
  def readDouble() = in.readDouble()
  def readString() = in.readString()

  def toDataInput = Input.asDataInput (in)
}
