package com.treode.pickle

import java.nio.charset.StandardCharsets.UTF_8
import scala.collection.mutable

abstract class PickleContext private [pickle] {

  private [this] val m = mutable.Map [Any, Int]()

  private [pickle] def contains (v: Any) = m contains v

  private [pickle] def get (v: Any) = m (v)

  private [pickle] def put (v: Any) = m.put (v, m.size)

  def writeVarUInt (v: Int) {
    var u = v
    while ((u & 0xFFFFFF80) != 0) {
      writeByte ((u & 0x7F | 0x80).toByte)
      u = u >>> 7
    }
    writeByte ((u & 0x7F).toByte)
  }

  def writeVarInt (v: Int): Unit =
    writeVarUInt ((v << 1) ^ (v >> 31))

  def writeVarULong (v: Long) {
    var u = v
    while ((u & 0xFFFFFF80) != 0) {
      writeByte ((u & 0x7F | 0x80).toByte)
      u = u >>> 7
    }
    writeByte ((u & 0x7F).toByte)
  }

  def writeVarLong (v: Long): Unit =
    writeVarULong ((v << 1) ^ (v >> 63))

  def writeString (v: String) {
    val b = UTF_8.encode (v)
    writeVarUInt (b.limit)
    writeBytes (b.array, 0, b.limit)
  }

  def writeByte (v: Byte)
  def writeShort (v: Short)
  def writeInt (v: Int)
  def writeLong (v: Long)
  def writeFloat (v: Float)
  def writeDouble (v: Double)
  def writeBytes (data: Array [Byte], offset: Int, length: Int)
}
