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

/** A Buffer that can resize by adding and dropping pages of bytes. */
class PagedBuffer private (val pageBits: Int) extends Buffer {

  private [this] val InitPages = 8

  val pageSize = 1 << pageBits
  val pageMask = pageSize - 1

  private [buffer] var pages = new Array [Array [Byte]] (InitPages)
  pages (0) = new Array [Byte] (pageSize)

  private [this] var limit = 1

  private [this] var wpage = pages (0)
  private [this] var woff = 0
  private [this] var wpos = 0

  private [this] var rpage = pages (0)
  private [this] var roff = 0
  private [this] var rpos = 0

  def clear() {
    val tmp = pages (0)
    if (pages.length != InitPages)
      pages = new Array [Array [Byte]] (InitPages)
    pages (0) = tmp
    limit = 1
    wpage = pages (0)
    woff = 0
    wpos = 0
    rpage = pages (0)
    roff = 0
    rpos = 0
  }

  def discard (length: Int): Int = {
    require (length <= readPos)
    if (length == writePos) {
      clear()
      return length;
    } else {
      val ndrop = length >> pageBits
      val nkeep = limit - ndrop
      System.arraycopy (pages, ndrop, pages, 0, nkeep)
      var i = nkeep
      while (i < pages.length) {
        pages (i) = null
        i += 1
      }
      val discarded = ndrop * pageSize
      limit = limit - ndrop
      woff = woff - discarded
      roff = roff - discarded
      return discarded
    }}

  def capacity: Int = limit << pageBits

  def capacity (length: Int) {
    val npages = (length + pageMask) >> pageBits
    if (npages <= limit)
      return
    if (npages > pages.length)
      pages = Arrays.copyOf (pages, twopow (npages))
    var i = limit
    while (i < npages) {
      pages (i) = new Array [Byte] (pageSize)
      i += 1
    }
    limit = npages
  }

  def buffer (start: Int, length: Int): ByteBuffer = {
    val pos = start & pageMask
    val end = math.min (pageSize - pos, length)
    ByteBuffer.wrap (pages (start >> pageBits), pos, end)
  }

  def buffers (start: Int, length: Int): Array [ByteBuffer] = {
    val end = start + length
    require (0 <= start)
    require (end <= capacity)
    val spage = start >> pageBits
    val epage = (end - 1) >> pageBits
    val nbufs = epage - spage + 1
    val bufs = new Array [ByteBuffer] (nbufs)
    var i = 0
    while (i < nbufs) {
      bufs (i) = ByteBuffer.wrap (pages (i + spage))
      i += 1
    }
    if (nbufs > 0) {
      bufs (0) .position (start & pageMask)
      val remaining = end & pageMask
      if (remaining > 0)
        bufs (nbufs - 1) .limit (remaining)
    }
    bufs
  }

  def hash (start: Int, length: Int, hashf: HashFunction): HashCode = {
    val end = start + length
    require (0 <= start)
    require (end <= capacity)
    var index = start >> pageBits
    var page = pages (index)
    val segment = pageSize - (start & pageMask)
    if (segment < length) {
      val hasher = hashf.newHasher()
      hasher.putBytes (page, start & pageMask, segment)
      var remaining = length - segment
      while (remaining > pageSize) {
        index += 1
        page = pages (index)
        hasher.putBytes (page, 0, pageSize)
        remaining -= pageSize
      }
      if (remaining > 0) {
        index += 1
        page = pages (index)
        hasher.putBytes (page, 0, remaining)
      }
      hasher.hash()
    } else {
      hashf.hashBytes (page, start & pageMask, length)
    }}

  private [this] def requireWritable (length: Int): Unit =
    capacity (woff + wpos + length)

  private [this] def wfix() {
    if (wpos == wpage.length) {
      wpage = pages ((woff >> pageBits) + 1)
      woff = woff + pageSize
      wpos = 0
    }}

  private [this] def write (v: Int) {
    wfix()
    wpage (wpos) = v.toByte
    wpos += 1
  }

  private [this] def write (v: Long) {
    wfix()
    wpage (wpos) = v.toByte
    wpos += 1
  }

  private [this] def crossesBoundry (length: Int): Boolean = {
    val limit = if (roff == woff) wpos else pageSize
    limit - rpos < length
  }

  private [this] def requireReadable (length: Int) {
    val available = writePos - readPos
    if (available < length)
      throw new BufferUnderflowException (length, available)
  }

  private [this] def rfix() {
    if (rpos == rpage.length) {
      rpage = pages ((roff >> pageBits) + 1)
      roff = roff + pageSize
      rpos = 0
    }}

  private [this] def read(): Byte = {
    rfix()
    val v = rpage (rpos)
    rpos += 1
    v
  }

  def writePos: Int =
    woff + wpos

  def writePos_= (pos: Int) {
    capacity (pos+1)
    wpage = pages (pos >> pageBits)
    woff = pos & ~pageMask
    wpos = pos & pageMask
  }

  def writeableBytes: Int =
    capacity - writePos

  def readPos: Int =
    roff + rpos

  def readPos_= (pos: Int) {
    val writePos = this.writePos
    val readPos = this.readPos
    if (pos > writePos)
      throw new BufferUnderflowException (pos - readPos, writePos - readPos)
    rpage = pages (pos >> pageBits)
    roff = pos & ~pageMask
    rpos = pos & pageMask
  }

  def readableBytes: Int = writePos - readPos

  def writeBytes (data: Array [Byte], offset: Int, length: Int) {
    val segment = pageSize - wpos
    if (segment < length) {
      requireWritable (length+1)
      System.arraycopy (data, offset, wpage, wpos, segment)
      var position = offset + segment
      var remaining = length - segment
      var windex = (woff >> pageBits) + 1
      while (remaining > pageSize) {
        wpage = pages (windex)
        System.arraycopy (data, position, wpage, 0, pageSize)
        position += pageSize
        remaining -= pageSize
        windex += 1
      }
      wpage = pages (windex)
      System.arraycopy (data, position, wpage, 0, remaining)
      woff = windex << pageBits
      wpos = remaining
    } else {
      System.arraycopy (data, offset, wpage, wpos, length)
      wpos += length
    }}

  def readBytes (data: Array [Byte], offset: Int, length: Int) {
    requireReadable (length)
    val segment = pageSize - rpos
    if (segment < length) {
      System.arraycopy (rpage, rpos, data, offset, segment)
      var position = offset + segment
      var remaining = length - segment
      var rindex = (roff >> pageBits) + 1
      while (remaining > pageSize) {
        rpage = pages (rindex)
        System.arraycopy (rpage, 0, data, position, pageSize)
        position += pageSize
        remaining -= pageSize
        rindex += 1
      }
      rpage = pages (rindex)
      System.arraycopy (rpage, 0, data, position, remaining)
      roff = rindex << pageBits
      rpos = remaining
    } else {
      System.arraycopy (rpage, rpos, data, offset, length)
      rpos += length
    }}

  def writeByte (v: Byte) {
    if (pageSize - wpos < 1) {
      requireWritable (1)
      write (v)
    } else {
      wpage (wpos) = v
      wpos += 1
    }}

  def readByte(): Byte = {
    if (crossesBoundry (1)) {
      requireReadable (1)
      read()
    } else {
      val v = rpage (rpos)
      rpos += 1
      v
    }}

  def writeShort (v: Short) {
    if (pageSize - wpos < 2) {
      requireWritable (2)
      write (v >> 8)
      write (v)
    } else {
      wpage (wpos) = (v >> 8).toByte
      wpage (wpos+1) = v.toByte
      wpos += 2
    }}

  def readShort(): Short = {
    if (crossesBoundry (2)) {
      requireReadable (2)
      val v =
          (read() & 0xFF) << 8 |
          (read() & 0xFF)
      v.toShort
    } else {
      val v =
        ((rpage (rpos) & 0xFF) << 8) |
        (rpage (rpos+1) & 0xFF)
      rpos += 2
      v.toShort
    }}

  def writeInt (v: Int) {
    if (pageSize - wpos < 4) {
      requireWritable (4)
      write (v >> 24)
      write (v >> 16)
      write (v >> 8)
      write (v)
    } else {
      wpage (wpos) = (v >> 24).toByte
      wpage (wpos+1) = (v >> 16).toByte
      wpage (wpos+2) = (v >> 8).toByte
      wpage (wpos+3) = v.toByte
      wpos += 4
    }}

  def readInt(): Int = {
    if (crossesBoundry (4)) {
      requireReadable (4)
      val v =
          (read() & 0xFF).toInt << 24 |
          (read() & 0xFF).toInt << 16 |
          (read() & 0xFF).toInt << 8 |
          (read() & 0xFF).toInt
      v
    } else {
      val v =
        ((rpage (rpos) & 0xFF).toInt << 24) |
        ((rpage (rpos+1) & 0xFF).toInt << 16) |
        ((rpage (rpos+2) & 0xFF).toInt << 8) |
        (rpage (rpos+3) & 0xFF).toInt
      rpos += 4
      v
    }}

  def writeVarUInt (v: Int) {
    if (pageSize - wpos < 5) {
      if (v >>> 7 == 0) {
        requireWritable (1)
        write (v)
      } else if (v >>> 14 == 0) {
        requireWritable (2)
        write ((v & 0x7F) | 0x80)
        write (v >>> 7)
      } else if (v >>> 21 == 0) {
        requireWritable (3)
        write ((v & 0x7F) | 0x80)
        write (v >>> 7 | 0x80)
        write (v >>> 14)
      } else if (v >>> 28 == 0) {
        requireWritable (4)
        write ((v & 0x7F) | 0x80)
        write (v >>> 7 | 0x80)
        write (v >>> 14 | 0x80)
        write (v >>> 21)
      } else {
        requireWritable (5)
        write ((v & 0x7F) | 0x80)
        write (v >>> 7 | 0x80)
        write (v >>> 14 | 0x80)
        write (v >>> 21 | 0x80)
        write (v >>> 28)
      }
    } else {
      if (v >>> 7 == 0) {
        wpage (wpos) = v.toByte
        wpos += 1
      } else if (v >>> 14 == 0) {
        wpage (wpos) = ((v & 0x7F) | 0x80).toByte
        wpage (wpos+1) = (v >>> 7).toByte
        wpos += 2
      } else if (v >>> 21 == 0) {
        wpage (wpos) = ((v & 0x7F) | 0x80).toByte
        wpage (wpos+1) = (v >>> 7 | 0x80).toByte
        wpage (wpos+2) = (v >>> 14).toByte
        wpos += 3
      } else if (v >>> 28 == 0) {
        wpage (wpos) = ((v & 0x7F) | 0x80).toByte
        wpage (wpos+1) = (v >>> 7 | 0x80).toByte
        wpage (wpos+2) = (v >>> 14 | 0x80).toByte
        wpage (wpos+3) = (v >>> 21).toByte
        wpos += 4
      } else {
        wpage (wpos) = ((v & 0x7F) | 0x80).toByte
        wpage (wpos+1) = (v >>> 7 | 0x80).toByte
        wpage (wpos+2) = (v >>> 14 | 0x80).toByte
        wpage (wpos+3) = (v >>> 21 | 0x80).toByte
        wpage (wpos+4) = (v >>> 28).toByte
        wpos += 5
    }}}

  def readVarUInt(): Int = {
    if (crossesBoundry (5)) {
      requireReadable (1)
      var b = read().toInt
      var v = b & 0x7F
      if ((b & 0x80) == 0)
        return v
      requireReadable (1)
      b = read().toInt
      v |= (b & 0x7F) << 7
      if ((b & 0x80) == 0)
        return v
      requireReadable (1)
      b = read().toInt
      v |= (b & 0x7F) << 14
      if ((b & 0x80) == 0)
        return v
      requireReadable (1)
      b = read().toInt
      v |= (b & 0x7F) << 21
      if ((b & 0x80) == 0)
        return v
      requireReadable (1)
      b = read().toInt
      v |= (b & 0x7F) << 28
      return v
    } else {
      var b = rpage (rpos) .toInt
      rpos += 1
      var v = b & 0x7F
      if ((b & 0x80) == 0)
        return v
      b = rpage (rpos) .toInt
      rpos += 1
      v |= (b & 0x7F) << 7
      if ((b & 0x80) == 0)
        return v
      b = rpage (rpos) .toInt
      rpos += 1
      v |= (b & 0x7F) << 14
      if ((b & 0x80) == 0)
        return v
      b = rpage (rpos) .toInt
      rpos += 1
      v |= (b & 0x7F) << 21
      if ((b & 0x80) == 0)
        return v
      b = rpage (rpos) .toInt
      rpos += 1
      v |= (b & 0x7F) << 28
      return v
    }}

  def writeVarInt (v: Int): Unit =
    writeVarUInt ((v << 1) ^ (v >> 31))

  def readVarInt(): Int = {
    val v = readVarUInt()
    ((v >>> 1) ^ -(v & 1))
  }

  def writeLong (v: Long) {
    if (pageSize - wpos < 8) {
      requireWritable (8)
      write (v >> 56)
      write (v >> 48)
      write (v >> 40)
      write (v >> 32)
      write (v >> 24)
      write (v >> 16)
      write (v >> 8)
      write (v)
    } else {
      wpage (wpos) = (v >> 56).toByte
      wpage (wpos+1) = (v >> 48).toByte
      wpage (wpos+2) = (v >> 40).toByte
      wpage (wpos+3) = (v >> 32).toByte
      wpage (wpos+4) = (v >> 24).toByte
      wpage (wpos+5) = (v >> 16).toByte
      wpage (wpos+6) = (v >> 8).toByte
      wpage (wpos+7) = v.toByte
      wpos += 8
    }}

  def readLong(): Long = {
    if (crossesBoundry (8)) {
      requireReadable (8)
      val v =
          ((read() & 0xFF).toLong << 56) |
          (read() & 0xFF).toLong << 48 |
          (read() & 0xFF).toLong << 40 |
          (read() & 0xFF).toLong << 32 |
          (read() & 0xFF).toLong << 24 |
          (read() & 0xFF).toLong << 16 |
          (read() & 0xFF).toLong << 8 |
          (read() & 0xFF).toLong
      v
    } else {
      val v =
        ((rpage (rpos) & 0xFF).toLong << 56) |
        ((rpage (rpos+1) & 0xFF).toLong << 48) |
        ((rpage (rpos+2) & 0xFF).toLong << 40) |
        ((rpage (rpos+3) & 0xFF).toLong << 32) |
        ((rpage (rpos+4) & 0xFF).toLong << 24) |
        ((rpage (rpos+5) & 0xFF).toLong << 16) |
        ((rpage (rpos+6) & 0xFF).toLong << 8) |
        (rpage (rpos+7) & 0xFF).toLong
      rpos += 8
      v
    }}

  def writeVarULong (v: Long) {
    if (pageSize - wpos < 9) {
      if (v >>> 7 == 0) {
        requireWritable (1)
        write (v)
      } else if (v >>> 14 == 0) {
        requireWritable (2)
        write ((v & 0x7F) | 0x80)
        write (v >>> 7)
      } else if (v >>> 21 == 0) {
        requireWritable (3)
        write ((v & 0x7F) | 0x80)
        write (v >>> 7 | 0x80)
        write (v >>> 14)
      } else if (v >>> 28 == 0) {
        requireWritable (4)
        write ((v & 0x7F) | 0x80)
        write (v >>> 7 | 0x80)
        write (v >>> 14 | 0x80)
        write (v >>> 21)
      } else if (v >>> 35 == 0) {
        requireWritable (5)
        write ((v & 0x7F) | 0x80)
        write (v >>> 7 | 0x80)
        write (v >>> 14 | 0x80)
        write (v >>> 21 | 0x80)
        write (v >>> 28)
      } else if (v >>> 42 == 0) {
        requireWritable (6)
        write ((v & 0x7F) | 0x80)
        write (v >>> 7 | 0x80)
        write (v >>> 14 | 0x80)
        write (v >>> 21 | 0x80)
        write (v >>> 28 | 0x80)
        write (v >>> 35)
      } else if (v >>> 49 == 0) {
        requireWritable (7)
        write ((v & 0x7F) | 0x80)
        write (v >>> 7 | 0x80)
        write (v >>> 14 | 0x80)
        write (v >>> 21 | 0x80)
        write (v >>> 28 | 0x80)
        write (v >>> 35 | 0x80)
        write (v >>> 42)
      } else if (v >>> 56 == 0) {
        requireWritable (8)
        write ((v & 0x7F) | 0x80)
        write (v >>> 7 | 0x80)
        write (v >>> 14 | 0x80)
        write (v >>> 21 | 0x80)
        write (v >>> 28 | 0x80)
        write (v >>> 35 | 0x80)
        write (v >>> 42 | 0x80)
        write (v >>> 49)
      } else {
        requireWritable (9)
        write ((v & 0x7F) | 0x80)
        write (v >>> 7 | 0x80)
        write (v >>> 14 | 0x80)
        write (v >>> 21 | 0x80)
        write (v >>> 28 | 0x80)
        write (v >>> 35 | 0x80)
        write (v >>> 42 | 0x80)
        write (v >>> 49 | 0x80)
        write (v >>> 56)
      }
    } else {
      if (v >>> 7 == 0) {
        wpage (wpos) = v.toByte
        wpos += 1
      } else if (v >>> 14 == 0) {
        wpage (wpos) = ((v & 0x7F) | 0x80).toByte
        wpage (wpos+1) = (v >>> 7).toByte
        wpos += 2
      } else if (v >>> 21 == 0) {
        wpage (wpos) = ((v & 0x7F) | 0x80).toByte
        wpage (wpos+1) = (v >>> 7 | 0x80).toByte
        wpage (wpos+2) = (v >>> 14).toByte
        wpos += 3
      } else if (v >>> 28 == 0) {
        wpage (wpos) = ((v & 0x7F) | 0x80).toByte
        wpage (wpos+1) = (v >>> 7 | 0x80).toByte
        wpage (wpos+2) = (v >>> 14 | 0x80).toByte
        wpage (wpos+3) = (v >>> 21).toByte
        wpos += 4
      } else if (v >>> 35 == 0) {
        wpage (wpos) = ((v & 0x7F) | 0x80).toByte
        wpage (wpos+1) = (v >>> 7 | 0x80).toByte
        wpage (wpos+2) = (v >>> 14 | 0x80).toByte
        wpage (wpos+3) = (v >>> 21 | 0x80).toByte
        wpage (wpos+4) = (v >>> 28).toByte
        wpos += 5
      } else if (v >>> 42 == 0) {
        wpage (wpos) = ((v & 0x7F) | 0x80).toByte
        wpage (wpos+1) = (v >>> 7 | 0x80).toByte
        wpage (wpos+2) = (v >>> 14 | 0x80).toByte
        wpage (wpos+3) = (v >>> 21 | 0x80).toByte
        wpage (wpos+4) = (v >>> 28 | 0x80).toByte
        wpage (wpos+5) = (v >>> 35).toByte
        wpos += 6
      } else if (v >>> 49 == 0) {
        wpage (wpos) = ((v & 0x7F) | 0x80).toByte
        wpage (wpos+1) = (v >>> 7 | 0x80).toByte
        wpage (wpos+2) = (v >>> 14 | 0x80).toByte
        wpage (wpos+3) = (v >>> 21 | 0x80).toByte
        wpage (wpos+4) = (v >>> 28 | 0x80).toByte
        wpage (wpos+5) = (v >>> 35 | 0x80).toByte
        wpage (wpos+6) = (v >>> 42).toByte
        wpos += 7
      } else if (v >>> 56 == 0) {
        wpage (wpos) = ((v & 0x7F) | 0x80).toByte
        wpage (wpos+1) = (v >>> 7 | 0x80).toByte
        wpage (wpos+2) = (v >>> 14 | 0x80).toByte
        wpage (wpos+3) = (v >>> 21 | 0x80).toByte
        wpage (wpos+4) = (v >>> 28 | 0x80).toByte
        wpage (wpos+5) = (v >>> 35 | 0x80).toByte
        wpage (wpos+6) = (v >>> 42 | 0x80).toByte
        wpage (wpos+7) = (v >>> 49).toByte
        wpos += 8
      } else {
        wpage (wpos) = ((v & 0x7F) | 0x80).toByte
        wpage (wpos+1) = (v >>> 7 | 0x80).toByte
        wpage (wpos+2) = (v >>> 14 | 0x80).toByte
        wpage (wpos+3) = (v >>> 21 | 0x80).toByte
        wpage (wpos+4) = (v >>> 28 | 0x80).toByte
        wpage (wpos+5) = (v >>> 35 | 0x80).toByte
        wpage (wpos+6) = (v >>> 42 | 0x80).toByte
        wpage (wpos+7) = (v >>> 49 | 0x80).toByte
        wpage (wpos+8) = (v >>> 56).toByte
        wpos += 9
      }}}

  def readVarULong(): Long = {
    if (crossesBoundry (9)) {
      requireReadable (1)
      var b = read().toLong
      var v = b & 0x7F
      if ((b & 0x80) == 0)
        return v
      requireReadable (1)
      b = read().toLong
      v |= (b & 0x7F) << 7
      if ((b & 0x80) == 0)
        return v
      requireReadable (1)
      b = read().toLong
      v |= (b & 0x7F) << 14
      if ((b & 0x80) == 0)
        return v
      requireReadable (1)
      b = read().toLong
      v |= (b & 0x7F) << 21
      if ((b & 0x80) == 0)
        return v
      requireReadable (1)
      b = read().toLong
      v |= (b & 0x7F) << 28
      if ((b & 0x80) == 0)
        return v
      requireReadable (1)
      b = read().toLong
      v |= (b & 0x7F) << 35
      if ((b & 0x80) == 0)
        return v
      requireReadable (1)
      b = read().toLong
      v |= (b & 0x7F) << 42
      if ((b & 0x80) == 0)
        return v
      requireReadable (1)
      b = read().toLong
      v |= (b & 0x7F) << 49
      if ((b & 0x80) == 0)
        return v
      requireReadable (1)
      b = read().toLong
      v |= b << 56
      return v
    } else {
      var b = rpage (rpos) .toLong
      rpos += 1
      var v = b & 0x7F
      if ((b & 0x80) == 0)
        return v
      b = rpage (rpos) .toLong
      rpos += 1
      v |= (b & 0x7F) << 7
      if ((b & 0x80) == 0)
        return v
      b = rpage (rpos) .toLong
      rpos += 1
      v |= (b & 0x7F) << 14
      if ((b & 0x80) == 0)
        return v
      b = rpage (rpos) .toLong
      rpos += 1
      v |= (b & 0x7F) << 21
      if ((b & 0x80) == 0)
        return v
      b = rpage (rpos) .toLong
      rpos += 1
      v |= (b & 0x7F) << 28
      if ((b & 0x80) == 0)
        return v
      b = rpage (rpos) .toLong
      rpos += 1
      v |= (b & 0x7F) << 35
      if ((b & 0x80) == 0)
        return v
      b = rpage (rpos) .toLong
      rpos += 1
      v |= (b & 0x7F) << 42
      if ((b & 0x80) == 0)
        return v
      b = rpage (rpos) .toLong
      rpos += 1
      v |= (b & 0x7F) << 49
      if ((b & 0x80) == 0)
        return v
      b = rpage (rpos) .toLong
      rpos += 1
      v |= b << 56
      return v
    }}

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

  private [this] def writeUtf8Char (c: Char) {
    if (pageSize - wpos < 3) {
      if (c <= 0x007F) {
        requireWritable (1)
        write (c)
      } else if (c <= 0x07FF) {
        requireWritable (2)
        write (0xC0 | c >> 6 & 0x1F)
        write (0x80 | c & 0x3F)
      } else {
        requireWritable (3)
        write  (0xE0 | c >> 12 & 0x0F)
        write  (0x80 | c >> 6 & 0x3F)
        write  (0x80 | c & 0x3F)
      }
    } else {
      if (c <= 0x007F) {
        wpage (wpos) = c.toByte
        wpos += 1
      } else if (c <= 0x07FF) {
        wpage (wpos) = (0xC0 | c >> 6 & 0x1F).toByte
        wpage (wpos+1) = (0x80 | c & 0x3F).toByte
        wpos += 2
      } else {
        wpage (wpos) = (0xE0 | c >> 12 & 0x0F).toByte
        wpage (wpos+1) = (0x80 | c >> 6 & 0x3F).toByte
        wpage (wpos+2) = (0x80 | c & 0x3F).toByte
        wpos += 3
      }}}

  def writeString (v: String) {
    writeVarUInt (v.length)
    if (v.length < 64 && v.length <= pageSize - wpos && isAscii (v)) {
      // Super fast case for short ASCII strings within a page.
      DeprecationCorral.getBytes (v, 0, v.length, wpage, wpos)
      wpos += v.length
    } else {
      var i = 0
      while (i < v.length) {
        writeUtf8Char (v.charAt (i))
        i += 1
      }}}

  private [this] def readUtf8Char(): Char = {
    if (crossesBoundry (3)) {
      requireReadable (1)
      val b = read() & 0xFF
      val x = b >> 4
      if (x < 8) {
        b.toChar
      } else if (x < 14) {
        requireReadable (1)
        val b1 = (b & 0x1F) << 6
        val b2 = read() & 0x3F
        (b1 | b2).toChar
      } else {
        requireReadable (2)
        val b1 = (b & 0x0F) << 12
        val b2 = (read() & 0x3f) << 6
        val b3 = read() & 0x3F
        (b1 | b2 | b3).toChar
      }
    } else {
      val b = rpage (rpos) & 0xFF
      val x = b >> 4
      if (x < 8) {
        rpos += 1
        b.toChar
      } else if (x < 14) {
        val b1 = (b & 0x1F) << 6
        val b2 = rpage (rpos+1) & 0x3F
        rpos += 2
        (b1 | b2).toChar
      } else {
        val b1 = (b & 0x0F) << 12
        val b2 = (rpage (rpos+1) & 0x3F) << 6
        val b3 = rpage (rpos+2) & 0x3F
        rpos += 3
        (b1 | b2 | b3).toChar
      }}}

  def readString(): String = {
    val len = readVarUInt()
    val chars = new Array [Char] (len)
    var i = 0
    rfix()
    var c = rpage (rpos) & 0xFF
    while (i < len && rpos+1 < pageSize && c < 128) {
      // Super fast case for ASCII strings within a page.
      chars (i) = c.toChar
      i += 1
      rpos += 1
      c = rpage (rpos) & 0xFF
    }
    while (i < len) {
      chars (i) = readUtf8Char()
      i += 1
    }
    new String (chars, 0, len)
  }

  override def toString = "PagedBuffer" + (readPos, writePos, capacity)
}

object PagedBuffer {

  def apply (bits: Int): PagedBuffer =
    new PagedBuffer (bits)
}
