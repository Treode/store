package com.treode.pickle

import java.io.DataOutput
import java.nio.charset.StandardCharsets.UTF_8
import com.google.common.hash.Hasher

private class HashingPickleContext (hash: Hasher) extends PickleContext with DataOutput {

  // PickleContext

  def writeBytes (v: Array [Byte], offset: Int, length: Int) =
    hash.putBytes (v, offset, length)

  def writeByte (v: Byte) = hash.putByte (v)
  def writeInt (v: Int) = hash.putInt (v)
  def writeVarInt (v: Int) = hash.putInt (v)
  def writeVarUInt (v: Int) = hash.putInt (v)
  def writeShort (v: Short) = hash.putShort (v)
  def writeLong (v: Long) = hash.putLong (v)
  def writeVarLong (v: Long) = hash.putLong (v)
  def writeVarULong (v: Long) = hash.putLong (v)
  def writeFloat (v: Float) = hash.putFloat (v)
  def writeDouble (v: Double) = hash.putDouble (v)
  def writeString (v: String) = hash.putString (v, UTF_8)

  // DataOuput - PickleContext

  def write (data: Array [Byte], offset: Int, length: Int) =
    hash.putBytes (data, offset, length)

  def write (v: Int) = hash.putInt (v)
  def writeBoolean (v: Boolean) = hash.putBoolean (v)
  def writeByte (v: Int) = hash.putByte (v.toByte)
  def writeChar (v: Int) = hash.putChar (v.toChar)
  def writeShort (v: Int) = hash.putShort (v.toShort)
  def write (data:  Array [Byte]) = hash.putBytes (data)
  def writeBytes (v: String): Unit = ???
  def writeChars (v: String): Unit = ???
  def writeUTF (v: String): Unit = ???

  def toDataOutput: DataOutput = this
}
