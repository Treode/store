package com.treode.pickle

import scala.collection.mutable

abstract class UnpickleContext private [pickle] {

  private [this] val m = mutable.Map [Int, Any]()

  private [pickle] def get [A] (idx: Int) = m (idx) .asInstanceOf [A]

  private [pickle] def put [A] (v: A) = m.put (m.size, v)

  def readVariableLengthInt(): Int = {
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

  def readVariableLengthUnsignedInt(): Int = {
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

  def readVariableLengthLong(): Long = {
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

  def readVariableLengthUnsignedLong(): Long = {
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

  def readByte(): Byte
  def readShort(): Short
  def readInt(): Int
  def readLong(): Long
  def readFloat(): Float
  def readDouble(): Double
  def readBytes (data: Array [Byte], offset: Int, length: Int)
}
