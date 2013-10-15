package com.treode.pickle

import io.netty.buffer.ByteBuf

private class ByteBufPickleContext (buffer: ByteBuf) extends PickleContext {

  def writeByte (v: Byte) = buffer.writeByte (v)
  def writeInt (v: Int) = buffer.writeInt (v)
  def writeShort (v: Short) = buffer.writeShort (v)
  def writeLong (v: Long) = buffer.writeLong (v)
  def writeFloat (v: Float) = buffer.writeFloat (v)
  def writeDouble (v: Double) = buffer.writeDouble (v)
  def writeBytes (v: Array [Byte], offset: Int, length: Int) = buffer.writeBytes (v, offset, length)
}
