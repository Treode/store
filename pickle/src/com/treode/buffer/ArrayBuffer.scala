package com.treode.buffer

import java.nio.ByteBuffer
import java.util.Arrays

class ArrayBuffer private (data: Array [Byte]) extends InputBuffer {

  private [this] var pos = 0

  def readPos: Int = pos

  def readPos_= (pos: Int) {
    if (pos > data.length)
      throw new BufferUnderflowException (this.pos - pos, data.length - pos)
    this.pos = pos
  }

  def readableBytes: Int = data.length - pos

  private [this] def requireReadable (length: Int) {
    val available = data.length - pos
    if (available < length)
      throw new BufferUnderflowException (length, available)
  }

  def readBytes (data: Array [Byte], offset: Int, length: Int) {
    requireReadable (length)
    System.arraycopy (data, pos, data, offset, length)
    pos += length
  }

  def readByte(): Byte = {
    requireReadable (1)
    val v = data (pos)
    pos += 1
    v
  }

  def readShort(): Short = {
    requireReadable (2)
    val v =
      ((data (pos) & 0xFF) << 8) |
      (data (pos+1) & 0xFF)
    pos += 2
    v.toShort
  }

  def readInt(): Int = {
    requireReadable (4)
    val v =
      ((data (pos) & 0xFF).toInt << 24) |
      ((data (pos+1) & 0xFF).toInt << 16) |
      ((data (pos+2) & 0xFF).toInt << 8) |
      (data (pos+3) & 0xFF).toInt
    pos += 4
    v
  }

  def readVarUInt(): Int = {
    requireReadable (1)
    var b = data (pos) .toInt
      pos += 1
    var v = b & 0x7F
    if ((b & 0x80) == 0)
      return v
    requireReadable (1)
    b = data (pos) .toInt
    pos += 1
    v |= (b & 0x7F) << 7
    if ((b & 0x80) == 0)
      return v
    requireReadable (1)
    b = data (pos) .toInt
    pos += 1
    v |= (b & 0x7F) << 14
    if ((b & 0x80) == 0)
      return v
    requireReadable (1)
    b = data (pos) .toInt
    pos += 1
    v |= (b & 0x7F) << 21
    if ((b & 0x80) == 0)
      return v
    requireReadable (1)
    b = data (pos) .toInt
    pos += 1
    v |= (b & 0x7F) << 28
    return v
  }

  def readVarInt(): Int = {
    val v = readVarUInt()
    ((v >>> 1) ^ -(v & 1))
  }

  def readLong(): Long = {
    requireReadable (8)
    val v =
      ((data (pos) & 0xFF).toLong << 56) |
      ((data (pos+1) & 0xFF).toLong << 48) |
      ((data (pos+2) & 0xFF).toLong << 40) |
      ((data (pos+3) & 0xFF).toLong << 32) |
      ((data (pos+4) & 0xFF).toLong << 24) |
      ((data (pos+5) & 0xFF).toLong << 16) |
      ((data (pos+6) & 0xFF).toLong << 8) |
      (data (pos+7) & 0xFF).toLong
    pos += 8
    v
  }

  def readVarULong(): Long = {
    requireReadable (1)
    var b = data (pos) .toLong
    pos += 1
    var v = b & 0x7F
    if ((b & 0x80) == 0)
      return v
    requireReadable (1)
    b = data (pos) .toLong
    pos += 1
    v |= (b & 0x7F) << 7
    if ((b & 0x80) == 0)
      return v
    requireReadable (1)
    b = data (pos) .toLong
    pos += 1
    v |= (b & 0x7F) << 14
    if ((b & 0x80) == 0)
      return v
    requireReadable (1)
    b = data (pos) .toLong
    pos += 1
    v |= (b & 0x7F) << 21
    if ((b & 0x80) == 0)
      return v
    requireReadable (1)
    b = data (pos) .toLong
    pos += 1
    v |= (b & 0x7F) << 28
    if ((b & 0x80) == 0)
      return v
    requireReadable (1)
    b = data (pos) .toLong
    pos += 1
    v |= (b & 0x7F) << 35
    if ((b & 0x80) == 0)
      return v
    requireReadable (1)
    b = data (pos) .toLong
    pos += 1
    v |= (b & 0x7F) << 42
    if ((b & 0x80) == 0)
      return v
    requireReadable (1)
    b = data (pos) .toLong
    pos += 1
    v |= (b & 0x7F) << 49
    if ((b & 0x80) == 0)
      return v
    requireReadable (1)
    b = data (pos) .toLong
    pos += 1
    v |= b << 56
    return v
  }

  def readVarLong(): Long = {
    val v = readVarULong()
    ((v >>> 1) ^ -(v & 1L))
  }

  def readFloat(): Float =
    java.lang.Float.intBitsToFloat (readInt())

  def readDouble(): Double =
    java.lang.Double.longBitsToDouble (readLong())

  private [this] def isAscii (v: String): Boolean = {
    var i = 0
    while (i < v.length) {
      if (v.charAt(i) > 127)
        return false
      i += 1
    }
    return true
  }

  private [this] def readUtf8Char(): Char = {
    requireReadable (1)
    val b = data (pos) & 0xFF
    val x = b >> 4
    if (x < 8) {
      pos += 1
      b.toChar
    } else if (x < 14) {
      requireReadable (1)
      val b1 = (b & 0x1F) << 6
      val b2 = data (pos+1) & 0x3F
      pos += 2
      (b1 | b2).toChar
    } else {
      requireReadable (2)
      val b1 = (b & 0x0F) << 12
      val b2 = (data (pos+1) & 0x3F) << 6
      val b3 = data (pos+2) & 0x3F
      pos += 3
      (b1 | b2 | b3).toChar
    }}

  def readString(): String = {
    val len = readVarUInt()
    val chars = new Array [Char] (len)
    var i = 0
    var c = if (i < len) (data (pos) & 0xFF) else -1
    while (i < len && pos+1 < data.length && c < 128) {
      // Super fast case for ASCII strings within a page.
      chars (i) = c.toChar
      i += 1
      pos += 1
      c = if (i < len) (data (pos) & 0xFF) else -1
    }
    while (i < len) {
      chars (i) = readUtf8Char()
      i += 1
    }
    new String (chars, 0, len)
  }

  override def toString = "ArrayBuffer" + (pos, data.length)
}

object ArrayBuffer {

  def apply (data: Array [Byte]): ArrayBuffer =
    new ArrayBuffer (data)
}
