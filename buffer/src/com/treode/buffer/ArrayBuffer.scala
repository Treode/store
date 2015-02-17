/*
 * Copyright 2014 Treode, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.treode.buffer

import java.nio.ByteBuffer
import java.util.Arrays

import com.google.common.hash.{HashCode, HashFunction}

/** A Buffer that has a fixed size. */
class ArrayBuffer private (data: Array [Byte]) extends Buffer {

  private [this] var wpos = 0
  private [this] var rpos = 0

  def capacity = data.length

  def hash (start: Int, length: Int, hashf: HashFunction): HashCode = {
    val available = data.length - start
    if (length > available)
      throw new BufferUnderflowException (length, available)
    hashf.hashBytes (data, start, length)
  }

  def writePos: Int = wpos

  def writePos_= (pos: Int) {
    if (pos > data.length)
      throw new BufferOverflowException (this.wpos - pos, data.length - pos)
    this.wpos = pos
  }

  def writeableBytes: Int = data.length - wpos

  def readPos: Int = rpos

  def readPos_= (pos: Int) {
    if (pos > data.length)
      throw new BufferUnderflowException (this.rpos - pos, data.length - pos)
    this.rpos = pos
  }

  def readableBytes: Int = wpos - rpos

  private [this] def requireWritable (length: Int) {
    val available = data.length - wpos
    if (available < length)
      throw new BufferOverflowException (length, available)
  }

  private [this] def requireReadable (length: Int) {
    val available = wpos - rpos
    if (available < length)
      throw new BufferUnderflowException (length, available)
  }

  def writeBytes (data: Array [Byte], offset: Int, length: Int) {
    requireWritable (length)
    System.arraycopy (data, offset, this.data, wpos, length)
    wpos += length
  }

  def readBytes (data: Array [Byte], offset: Int, length: Int) {
    requireReadable (length)
    System.arraycopy (this.data, rpos, data, offset, length)
    rpos += length
  }

  def writeByte (v: Byte) {
    requireWritable (1)
    data (wpos) = v
    wpos += 1
  }

  def readByte(): Byte = {
    requireReadable (1)
    val v = data (rpos)
    rpos += 1
    v
  }

  def writeShort (v: Short) {
    requireWritable (2)
    data (wpos) = (v >> 8).toByte
    data (wpos+1) = v.toByte
    wpos += 2
  }

  def readShort(): Short = {
    requireReadable (2)
    val v =
      ((data (rpos) & 0xFF) << 8) |
      (data (rpos+1) & 0xFF)
    rpos += 2
    v.toShort
  }

  def writeInt (v: Int) {
    requireWritable (4)
    data (wpos) = (v >> 24).toByte
    data (wpos+1) = (v >> 16).toByte
    data (wpos+2) = (v >> 8).toByte
    data (wpos+3) = v.toByte
    wpos += 4
  }

  def readInt(): Int = {
    requireReadable (4)
    val v =
      ((data (rpos) & 0xFF).toInt << 24) |
      ((data (rpos+1) & 0xFF).toInt << 16) |
      ((data (rpos+2) & 0xFF).toInt << 8) |
      (data (rpos+3) & 0xFF).toInt
    rpos += 4
    v
  }

  def writeVarUInt (v: Int) {
    if (v >>> 7 == 0) {
      requireWritable (1)
      data (wpos) = v.toByte
      wpos += 1
    } else if (v >>> 14 == 0) {
      requireWritable (2)
      data (wpos) = ((v & 0x7F) | 0x80).toByte
      data (wpos+1) = (v >>> 7).toByte
      wpos += 2
    } else if (v >>> 21 == 0) {
      requireWritable (3)
      data (wpos) = ((v & 0x7F) | 0x80).toByte
      data (wpos+1) = (v >>> 7 | 0x80).toByte
      data (wpos+2) = (v >>> 14).toByte
      wpos += 3
    } else if (v >>> 28 == 0) {
      requireWritable (4)
      data (wpos) = ((v & 0x7F) | 0x80).toByte
      data (wpos+1) = (v >>> 7 | 0x80).toByte
      data (wpos+2) = (v >>> 14 | 0x80).toByte
      data (wpos+3) = (v >>> 21).toByte
      wpos += 4
    } else {
      requireWritable (5)
      data (wpos) = ((v & 0x7F) | 0x80).toByte
      data (wpos+1) = (v >>> 7 | 0x80).toByte
      data (wpos+2) = (v >>> 14 | 0x80).toByte
      data (wpos+3) = (v >>> 21 | 0x80).toByte
      data (wpos+4) = (v >>> 28).toByte
      wpos += 5
    }}

  def readVarUInt(): Int = {
    requireReadable (1)
    var b = data (rpos) .toInt
      rpos += 1
    var v = b & 0x7F
    if ((b & 0x80) == 0)
      return v
    requireReadable (1)
    b = data (rpos) .toInt
    rpos += 1
    v |= (b & 0x7F) << 7
    if ((b & 0x80) == 0)
      return v
    requireReadable (1)
    b = data (rpos) .toInt
    rpos += 1
    v |= (b & 0x7F) << 14
    if ((b & 0x80) == 0)
      return v
    requireReadable (1)
    b = data (rpos) .toInt
    rpos += 1
    v |= (b & 0x7F) << 21
    if ((b & 0x80) == 0)
      return v
    requireReadable (1)
    b = data (rpos) .toInt
    rpos += 1
    v |= (b & 0x7F) << 28
    return v
  }

  def writeVarInt (v: Int): Unit =
    writeVarUInt ((v << 1) ^ (v >> 31))

  def readVarInt(): Int = {
    val v = readVarUInt()
    ((v >>> 1) ^ -(v & 1))
  }

  def writeLong (v: Long) {
    requireWritable (8)
    data (wpos) = (v >> 56).toByte
    data (wpos+1) = (v >> 48).toByte
    data (wpos+2) = (v >> 40).toByte
    data (wpos+3) = (v >> 32).toByte
    data (wpos+4) = (v >> 24).toByte
    data (wpos+5) = (v >> 16).toByte
    data (wpos+6) = (v >> 8).toByte
    data (wpos+7) = v.toByte
    wpos += 8
  }

  def readLong(): Long = {
    requireReadable (8)
    val v =
      ((data (rpos) & 0xFF).toLong << 56) |
      ((data (rpos+1) & 0xFF).toLong << 48) |
      ((data (rpos+2) & 0xFF).toLong << 40) |
      ((data (rpos+3) & 0xFF).toLong << 32) |
      ((data (rpos+4) & 0xFF).toLong << 24) |
      ((data (rpos+5) & 0xFF).toLong << 16) |
      ((data (rpos+6) & 0xFF).toLong << 8) |
      (data (rpos+7) & 0xFF).toLong
    rpos += 8
    v
  }

  def writeVarULong (v: Long) {
    if (v >>> 7 == 0) {
      requireWritable (1)
      data (wpos) = v.toByte
      wpos += 1
    } else if (v >>> 14 == 0) {
      requireWritable (2)
      data (wpos) = ((v & 0x7F) | 0x80).toByte
      data (wpos+1) = (v >>> 7).toByte
      wpos += 2
    } else if (v >>> 21 == 0) {
      requireWritable (3)
      data (wpos) = ((v & 0x7F) | 0x80).toByte
      data (wpos+1) = (v >>> 7 | 0x80).toByte
      data (wpos+2) = (v >>> 14).toByte
      wpos += 3
    } else if (v >>> 28 == 0) {
      requireWritable (4)
      data (wpos) = ((v & 0x7F) | 0x80).toByte
      data (wpos+1) = (v >>> 7 | 0x80).toByte
      data (wpos+2) = (v >>> 14 | 0x80).toByte
      data (wpos+3) = (v >>> 21).toByte
      wpos += 4
    } else if (v >>> 35 == 0) {
      requireWritable (5)
      data (wpos) = ((v & 0x7F) | 0x80).toByte
      data (wpos+1) = (v >>> 7 | 0x80).toByte
      data (wpos+2) = (v >>> 14 | 0x80).toByte
      data (wpos+3) = (v >>> 21 | 0x80).toByte
      data (wpos+4) = (v >>> 28).toByte
      wpos += 5
    } else if (v >>> 42 == 0) {
      requireWritable (6)
      data (wpos) = ((v & 0x7F) | 0x80).toByte
      data (wpos+1) = (v >>> 7 | 0x80).toByte
      data (wpos+2) = (v >>> 14 | 0x80).toByte
      data (wpos+3) = (v >>> 21 | 0x80).toByte
      data (wpos+4) = (v >>> 28 | 0x80).toByte
      data (wpos+5) = (v >>> 35).toByte
      wpos += 6
    } else if (v >>> 49 == 0) {
      requireWritable (7)
      data (wpos) = ((v & 0x7F) | 0x80).toByte
      data (wpos+1) = (v >>> 7 | 0x80).toByte
      data (wpos+2) = (v >>> 14 | 0x80).toByte
      data (wpos+3) = (v >>> 21 | 0x80).toByte
      data (wpos+4) = (v >>> 28 | 0x80).toByte
      data (wpos+5) = (v >>> 35 | 0x80).toByte
      data (wpos+6) = (v >>> 42).toByte
      wpos += 7
    } else if (v >>> 56 == 0) {
      requireWritable (8)
      data (wpos) = ((v & 0x7F) | 0x80).toByte
      data (wpos+1) = (v >>> 7 | 0x80).toByte
      data (wpos+2) = (v >>> 14 | 0x80).toByte
      data (wpos+3) = (v >>> 21 | 0x80).toByte
      data (wpos+4) = (v >>> 28 | 0x80).toByte
      data (wpos+5) = (v >>> 35 | 0x80).toByte
      data (wpos+6) = (v >>> 42 | 0x80).toByte
      data (wpos+7) = (v >>> 49).toByte
      wpos += 8
    } else {
      requireWritable (9)
      data (wpos) = ((v & 0x7F) | 0x80).toByte
      data (wpos+1) = (v >>> 7 | 0x80).toByte
      data (wpos+2) = (v >>> 14 | 0x80).toByte
      data (wpos+3) = (v >>> 21 | 0x80).toByte
      data (wpos+4) = (v >>> 28 | 0x80).toByte
      data (wpos+5) = (v >>> 35 | 0x80).toByte
      data (wpos+6) = (v >>> 42 | 0x80).toByte
      data (wpos+7) = (v >>> 49 | 0x80).toByte
      data (wpos+8) = (v >>> 56).toByte
      wpos += 9
    }}

  def readVarULong(): Long = {
    requireReadable (1)
    var b = data (rpos) .toLong
    rpos += 1
    var v = b & 0x7F
    if ((b & 0x80) == 0)
      return v
    requireReadable (1)
    b = data (rpos) .toLong
    rpos += 1
    v |= (b & 0x7F) << 7
    if ((b & 0x80) == 0)
      return v
    requireReadable (1)
    b = data (rpos) .toLong
    rpos += 1
    v |= (b & 0x7F) << 14
    if ((b & 0x80) == 0)
      return v
    requireReadable (1)
    b = data (rpos) .toLong
    rpos += 1
    v |= (b & 0x7F) << 21
    if ((b & 0x80) == 0)
      return v
    requireReadable (1)
    b = data (rpos) .toLong
    rpos += 1
    v |= (b & 0x7F) << 28
    if ((b & 0x80) == 0)
      return v
    requireReadable (1)
    b = data (rpos) .toLong
    rpos += 1
    v |= (b & 0x7F) << 35
    if ((b & 0x80) == 0)
      return v
    requireReadable (1)
    b = data (rpos) .toLong
    rpos += 1
    v |= (b & 0x7F) << 42
    if ((b & 0x80) == 0)
      return v
    requireReadable (1)
    b = data (rpos) .toLong
    rpos += 1
    v |= (b & 0x7F) << 49
    if ((b & 0x80) == 0)
      return v
    requireReadable (1)
    b = data (rpos) .toLong
    rpos += 1
    v |= b << 56
    return v
  }

  def writeVarLong (v: Long): Unit =
    writeVarULong ((v << 1) ^ (v >> 63))

  def readVarLong(): Long = {
    val v = readVarULong()
    ((v >>> 1) ^ -(v & 1L))
  }

  def writeFloat (v: Float): Unit =
    writeInt (java.lang.Float.floatToIntBits (v))

  def readFloat(): Float =
    java.lang.Float.intBitsToFloat (readInt())

  def writeDouble (v: Double): Unit =
    writeLong (java.lang.Double.doubleToLongBits (v))

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

  private [this] def writeUtf8Char (c: Char) {
    requireWritable (3)
    if (c <= 0x007F) {
      data (wpos) = c.toByte
      wpos += 1
    } else if (c <= 0x07FF) {
      data (wpos) = (0xC0 | c >> 6 & 0x1F).toByte
      data (wpos+1) = (0x80 | c & 0x3F).toByte
      wpos += 2
    } else {
      data (wpos) = (0xE0 | c >> 12 & 0x0F).toByte
      data (wpos+1) = (0x80 | c >> 6 & 0x3F).toByte
      data (wpos+2) = (0x80 | c & 0x3F).toByte
      wpos += 3
    }}

  def writeString (v: String) {
    writeVarUInt (v.length)
    if (v.length < 64 && v.length <= data.length - wpos && isAscii (v)) {
      // Super fast case for short ASCII strings within a page.
      DeprecationCorral.getBytes (v, 0, v.length, data, wpos)
      wpos += v.length
    } else {
      var i = 0
      while (i < v.length) {
        writeUtf8Char (v.charAt (i))
        i += 1
      }}}

  private [this] def readUtf8Char(): Char = {
    requireReadable (1)
    val b = data (rpos) & 0xFF
    val x = b >> 4
    if (x < 8) {
      rpos += 1
      b.toChar
    } else if (x < 14) {
      requireReadable (1)
      val b1 = (b & 0x1F) << 6
      val b2 = data (rpos+1) & 0x3F
      rpos += 2
      (b1 | b2).toChar
    } else {
      requireReadable (2)
      val b1 = (b & 0x0F) << 12
      val b2 = (data (rpos+1) & 0x3F) << 6
      val b3 = data (rpos+2) & 0x3F
      rpos += 3
      (b1 | b2 | b3).toChar
    }}

  def readString(): String = {
    val len = readVarUInt()
    val chars = new Array [Char] (len)
    var i = 0
    var c = if (i < len) (data (rpos) & 0xFF) else -1
    while (i < len && rpos+1 < data.length && c < 128) {
      // Super fast case for ASCII strings within a page.
      chars (i) = c.toChar
      i += 1
      rpos += 1
      c = if (i < len) (data (rpos) & 0xFF) else -1
    }
    while (i < len) {
      chars (i) = readUtf8Char()
      i += 1
    }
    new String (chars, 0, len)
  }

  override def toString = "ArrayBuffer" + (rpos, data.length)
}

object ArrayBuffer {

  /** Create an ArrayBuffer setup for reading data.
    * @param data The byte array to wrap.
    * @return An ArrayBuffer that wraps `data` for reading, with `readPos` set at the beginning,
    * and with `writePos` set at the end.
    */
  def readable (data: Array [Byte]): ArrayBuffer = {
    val buffer = new ArrayBuffer (data)
    buffer.writePos = data.length
    buffer
  }

  /** Create an ArrayBuffer setup for writing data.
    * @param data The byte array to wrap.
    * @return An ArrayBuffer that wraps `data` for writing, with `readPos` and `writePos` set at
    * the beginning.
    */
  def writable (data: Array [Byte]): ArrayBuffer =
    new ArrayBuffer (data)

  /** Create an ArrayBuffer setup for writing data.
    * @param length The size of the byte array to allocate and then wrap.
    * @return An ArrayBuffer for writing, with `readPos` and `writePos` set at the beginning.
    */
  def writable (length: Int): ArrayBuffer =
    new ArrayBuffer (new Array (length))
}
