package com.treode.io.buffer

import java.nio.ByteBuffer

private final class PagedBuffer private[buffer](
  private[this] var pages: Array[PagedBuffer.Page],
  private[this] var offset: Int,
  private[this] var _readAt: Int,
  private[this] var _writeAt: Int
) extends Buffer {

  import PagedBuffer._

  private[this] def copy[A] (src: Array[A], srcoff: Int, dst: Array[A], dstoff: Int, len: Int) =
    System.arraycopy (src, srcoff, dst, dstoff, len)

  private[this] def fill[A] (dst: Array[A], dstoff: Int, len: Int, f: => A) {
    var i = dstoff
    while (i < dstoff + len) {
      dst (i) = f
      i += 1
    }
  }

  def readAt = _readAt - offset

  def readAt_= (index: Int) = {
    val adj = index + offset
    if (index < 0 || adj > _writeAt) throw new IndexOutOfBoundsException
    _readAt = adj
  }

  def writeAt = _writeAt - offset

  def writeAt_= (index: Int) = {
    val adj = index + offset
    if (adj < _readAt || index > capacity) throw new IndexOutOfBoundsException
    _writeAt = adj
  }

  def capacity = pages.length * pageSize - offset

  def readableBytes = _writeAt - _readAt

  def writableBytes = pages.length * pageSize - _writeAt

  /** Grow if necessary to accommodate at least `min` bytes. */
  def capacity (min: Int) {
    val n = pageOf (math.max (1, min) + offset - 1) + 1
    if (n > pages.length) {
      val newPages = new Array[Page](n)
      copy (pages, 0, newPages, 0, pages.length)
      fill (newPages, pages.length, n - pages.length, newPage)
      pages = newPages
    }
  }

  def clear () {
    pages = Array (PagedBuffer.newPage)
    offset = 0
    _writeAt = 0
    _readAt = 0
  }

  def discard (length: Int) {
    require (length + offset <= _readAt, "Discard position must preceed read position.")
    val n = pageOf (length + offset)
    if (n > 0) {
      val newPages = new Array[Page](pages.length - n)
      copy (pages, n, newPages, 0, newPages.length)
      pages = newPages
      offset = offset + length - pageSize * n
      _readAt = _readAt - pageSize * n
      _writeAt = _writeAt - pageSize * n
    } else {
      offset = offset + length
    }
  }

  def slice (index: Int, length: Int): Buffer = {
    require (0 <= index, "Slice index must be non-negative.")
    require (index + length + offset <= _writeAt, "Cannot slice past write position.")
    val n = pageOf (length) + 1
    val newPages = new Array[Page](n)
    copy (pages, pageOf (index + offset), newPages, 0, n)
    val i = pageIndexOf (index + offset)
    new PagedBuffer (pages, i, i, length + i)
  }

  def readableByteBuffers: Array[ByteBuffer] = {
    if (_writeAt == _readAt) {
      val m = pageOf (_readAt)
      if (m >= pages.length) {
        new Array[ByteBuffer](0)
      } else {
        val bs = new Array[ByteBuffer](1)
        bs (0) = ByteBuffer.wrap (pages (m))
        bs (0).position (pageIndexOf (_readAt))
        bs (0).limit (pageIndexOf (_readAt))
        bs
      }
    } else {
      val m = pageOf (_readAt)
      val n = pageOf (_writeAt - 1)
      val bs = new Array[ByteBuffer](n - m + 1)
      for (i <- m to n)
        bs (i - m) = ByteBuffer.wrap (pages (i))
      bs (0).position (pageIndexOf (_readAt))
      bs (n - m).limit (pageIndexOf (_writeAt - 1) + 1)
      bs
    }
  }

  def writableByteBuffers: Array[ByteBuffer] = {
    val m = pageOf (_writeAt)
    if (m >= pages.length) {
      new Array[ByteBuffer](0)
    } else {
      val n = pages.length - 1
      val bs = new Array[ByteBuffer](n - m + 1)
      for (i <- m to n)
        bs (i - m) = ByteBuffer.wrap (pages (i))
      bs (0).position (pageIndexOf (_writeAt))
      bs
    }
  }

  def getByte (index: Int): Byte = {
    if (index < 0) throw new IndexOutOfBoundsException
    val adj = index + offset
    if (adj > _writeAt) throw new IndexOutOfBoundsException
    pages (pageOf (adj))(pageIndexOf (adj))
  }

  def getBytes (srcoff: Int, dst: Array[Byte], dstoff: Int, len: Int) {
    val index = pageIndexOf (srcoff + offset)
    val rest = pageSize - index
    val dstEnd = dstoff + len
    var srcPage = pageOf (srcoff + offset)
    var dstOffset = dstoff
    copy (pages (srcPage), index, dst, dstOffset, math.min (rest, len))
    srcPage += 1
    dstOffset += math.min (rest, len)
    while (dstOffset + pageSize < dstEnd) {
      copy (pages (srcPage), 0, dst, dstOffset, pageSize)
      srcPage += 1
      dstOffset += pageSize
    }
    if (srcPage < pages.length)
      copy (pages (srcPage), 0, dst, dstOffset, dstEnd - dstOffset)
  }

  def readByte (): Byte = {
    if (_readAt >= _writeAt) throw new IndexOutOfBoundsException
    val byte = pages (pageOf (_readAt))(pageIndexOf (_readAt))
    _readAt += 1
    byte
  }

  def readBytes (bytes: Array[Byte], offset: Int, length: Int) {
    getBytes (readAt, bytes, offset, length)
    _readAt += length
  }

  def setByte (index: Int, byte: Byte) = {
    if (index < 0) throw new IndexOutOfBoundsException
    val adj = index + offset
    pages (pageOf (adj))(pageIndexOf (adj)) = byte
  }

  def writeByte (byte: Byte) {
    capacity (_writeAt - offset + 1)
    pages (pageOf (_writeAt))(pageIndexOf (_writeAt)) = byte
    _writeAt += 1
  }

  def setBytes (dstoff: Int, src: Array[Byte], srcoff: Int, len: Int) {
    capacity (dstoff + len - offset)
    val index = pageIndexOf (dstoff + offset)
    val rest = pageSize - index
    val srcEnd = srcoff + len
    var dstPage = pageOf (dstoff + offset)
    var srcOffset = srcoff
    copy (src, srcOffset, pages (dstPage), index, math.min (rest, len))
    dstPage += 1
    srcOffset += math.min (rest, len)
    while (srcOffset + pageSize < srcEnd) {
      copy (src, srcOffset, pages (dstPage), 0, pageSize)
      dstPage += 1
      srcOffset += pageSize
    }
    if (dstPage < pages.length)
      copy (src, srcOffset, pages (dstPage), 0, srcEnd - srcOffset)
  }

  def writeBytes (bytes: Array[Byte], offset: Int, length: Int) {
    setBytes (_writeAt, bytes, offset, length)
    _writeAt += length
  }

  override def toString = {
    val lineBits = 5
    val lineSize = 1 << lineBits
    val blockMask = 0x7
    val rline = _readAt >> lineBits
    val wline = _writeAt >> lineBits
    val cline = pages.length * pageSize >> lineBits

    def toChar (b: Byte) =
      b.toChar match {
        case _ if b >= 33 && b <= 126 => b.toChar + "   "
        case _ if b >= 127 => "%03d " format b
        case '\n' => "\\n  "
        case '\b' => "\\b  "
        case '\t' => "\\t  "
        case '\f' => "\\f  "
        case '\r' => "\\r  "
        case ' ' => "spc "
        case _ if b <= 31 => "%03d " format b
      }

    def getCharString (index: Int) = {
      val adj = index + offset
      val b = pages (pageOf (adj))(pageIndexOf (adj))
      toChar (b)
    }

    /** Collect pieces of the final string. */
    val bldr = new StringBuilder

    /** Append the given line */
    def doLine (line: Int) {
      val r = rline == line
      val w = wline == line
      val c = cline == line
      if (r) bldr ++= "readAt: " + readAt
      if (r && w) bldr += ' '
      if (w) bldr ++= "writeAt: " + writeAt
      if (w && c) bldr += ' '
      if (c) bldr ++= "capacity: " + capacity
      if (r || w || c) bldr ++= "\n"

      val pos = line * lineSize - offset
      if (pos < capacity) {
        bldr ++= "%5d " format pos
        for (i <- pos until math.min (pos + lineSize, capacity)) {
          if ((i & blockMask) == 0) bldr ++= " :: "
          bldr ++= getCharString (i)
        }
        bldr += '\n'
      }
    }

    bldr ++= "PagedBuffer {\n"
    doLine (0)
    var line = 1

    /** Ensure the line before, of and after target has been appended. */
    def doBlock (target: Int) {
      if (target - 1 >= line) {
        doLine (target - 1)
        line = target
      }
      if (target >= line) {
        doLine (target)
        line = target + 1
      }
      if (target + 1 >= line) {
        doLine (target + 1)
        line = target + 2
      }
    }

    doBlock (rline)
    doBlock (wline)
    doBlock (cline)

    bldr += '}'
    bldr.result ()
  }
}

private object PagedBuffer {

  private[buffer] type Page = Array[Byte]

  private val pageExponent = 13
  private val pageSize = 1 << pageExponent
  private val pageMask = pageSize - 1

  private def newPage = new Array[Byte](pageSize)

  private def pageOf (index: Int) = index >> pageExponent

  private def pageIndexOf (index: Int) = index & pageMask

  def apply () = new PagedBuffer (Array (PagedBuffer.newPage), 0, 0, 0)
}
