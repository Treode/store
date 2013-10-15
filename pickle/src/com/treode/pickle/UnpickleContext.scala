package com.treode.pickle

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets.UTF_8
import scala.collection.mutable

abstract class UnpickleContext private [pickle] {

  private [this] val m = mutable.Map [Int, Any]()

  private [pickle] def get [A] (idx: Int) = m (idx) .asInstanceOf [A]

  private [pickle] def put [A] (v: A) = m.put (m.size, v)

  def readVarInt(): Int = {
    val b1 = readByte().toInt
    var b = b1
    var v = b & 0x7F
    var shift = 7
    while ((b & 0x80) != 0) {
      b = readByte().toInt
      v = v | ((b & 0x7F) << shift)
      shift += 7
    }
    (if ((b1 & 1) == 0) 0 else -1) ^ (v >>> 1)
  }

  def readVarUInt(): Int = {
    val b1 = readByte().toInt
    var b = b1
    var v = b & 0x7F
    var shift = 7
    while ((b & 0x80) != 0) {
      b = readByte().toInt
      v = v | ((b & 0x7F) << shift)
      shift += 7
    }
    v
  }

  def readVarLong(): Long = {
    val b1 = readByte().toLong
    var b = b1
    var v = b & 0x7F
    var shift = 7
    while ((b & 0x80) != 0) {
      b = readByte().toLong
      v = v | ((b & 0x7F) << shift)
      shift += 7
    }
    (if ((b1 & 1) == 0) 0 else -1) ^ (v >>> 1)
  }

  def readVarULong(): Long = {
    val b1 = readByte().toLong
    var b = b1
    var v = b & 0x7F
    var shift = 7
    while ((b & 0x80) != 0) {
      b = readByte().toLong
      v = v | ((b & 0x7F) << shift)
      shift += 7
    }
    v
  }

  def readString(): String = {
    val l = readVarUInt()
    val b = ByteBuffer.allocate (l)
    readBytes (b.array, 0, l)
    UTF_8.decode (b).toString
  }

  def readByte(): Byte
  def readShort(): Short
  def readInt(): Int
  def readLong(): Long
  def readFloat(): Float
  def readDouble(): Double
  def readBytes (data: Array [Byte], offset: Int, length: Int)
}
