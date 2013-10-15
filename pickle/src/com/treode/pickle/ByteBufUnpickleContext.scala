package com.treode.pickle

import io.netty.buffer.ByteBuf

private class ByteBufUnpickleContext (buffer: ByteBuf) extends UnpickleContext {

  def readByte() = buffer.readByte()
  def readShort() = buffer.readShort()
  def readInt() = buffer.readInt()
  def readLong() = buffer.readLong()
  def readFloat() = buffer.readFloat()
  def readDouble() = buffer.readDouble()
  def readBytes (data: Array [Byte], offset: Int, length: Int) = buffer.readBytes (data, offset, length)
}
