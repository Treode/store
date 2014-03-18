package com.treode.buffer

import com.google.common.hash.Hashing
import org.scalatest.prop.PropertyChecks
import org.scalatest.{FlatSpec, PropSpec, Suites}

class PagedBufferSpec extends Suites (PagedBufferBehaviors, PagedBufferProperties)

private object PagedBufferBehaviors extends FlatSpec {

  val pageBits = 5
  val pageSize = 32

  def capacity (nbytes: Int, npages: Int) {
    it should (s"add the right pages for nbytes=$nbytes") in {
      val buffer = PagedBuffer (pageBits)
      buffer.capacity (nbytes)
      assertResult (npages << pageBits) (buffer.capacity)
      for (i <- 0 until npages)
        assert (buffer.pages (i) != null)
      for (i <- npages until buffer.pages.length)
        assert (buffer.pages (i) == null)
    }}

  behavior of "PagedBuffer.capacity"
  capacity (0, 1)
  capacity (1, 1)
  capacity (31, 1)
  capacity (32, 1)
  capacity (33, 2)
  capacity (63, 2)
  capacity (64, 2)
  capacity (65, 3)

  def discard (nbytes: Int, npages: Int) {
    it should (s"discard the right pages for nbytes=$nbytes") in {
      val buffer = PagedBuffer (pageBits)
      buffer.writePos  = (4 << pageBits) - 1
      buffer.readPos = nbytes
      val before = (Seq (buffer.pages: _*), buffer.writePos, buffer.readPos)
      buffer.discard (nbytes)
      assertResult ((4 - npages) << pageBits) (buffer.capacity)
      val after = (Seq (buffer.pages: _*), buffer.writePos, buffer.readPos)
      for (i <- 0 until before._1.length - npages)
        assert (before._1 (i + npages) == after._1 (i))
      for (i <- before._1.length - npages until after._1.length)
        assert (after._1 (i) == null)
      assertResult (before._2 - pageSize * npages) (after._2)
      assertResult (before._3 - pageSize * npages) (after._3)
    }}

  behavior of "PagedBuffer.discard"
  discard (0, 0)
  discard (7, 0)
  discard (31, 0)
  discard (32, 1)
  discard (39, 1)
  discard (63, 1)
  discard (64, 2)
  discard (71, 2)
  discard (96, 3)

  def buffer (sbyte: Int, nbytes: Int, page: Int, first: Int, last: Int) {
    it should (s"yield the right range for sbyte=$sbyte, nbytes=$nbytes") in {
      val buffer = PagedBuffer (pageBits)
      buffer.writePos = 128
      val bytebuf = buffer.buffer (sbyte, nbytes)
      assertResult (first) (bytebuf.position)
      assertResult (last) (bytebuf.limit)
      assert (buffer.pages (page) == bytebuf.array)
    }}

  behavior of "PagedBuffer.buffer"
  buffer (0, 0, 0, 0, 0)
  buffer (0, 1, 0, 0, 1)
  buffer (0, 31, 0, 0, 31)
  buffer (0, 32, 0, 0, 32)
  buffer (7, 24, 0, 7, 31)
  buffer (7, 25, 0, 7, 32)
  buffer (32, 0, 1, 0, 0)
  buffer (32, 1, 1, 0, 1)
  buffer (32, 31, 1, 0, 31)
  buffer (32, 32, 1, 0, 32)
  buffer (39, 24, 1, 7, 31)
  buffer (39, 25, 1, 7, 32)
  buffer (0, 57, 0, 0, 32)
  buffer (7, 50, 0, 7, 32)
  buffer (32, 57, 1, 0, 32)
  buffer (39, 50, 1, 7, 32)

  def buffers (sbyte: Int, nbytes: Int, spage: Int, nbufs: Int, first: Int, last: Int) {
    it should (s"yield the right range for sbyte=$sbyte, nbytes=$nbytes") in {
      val buffer = PagedBuffer (pageBits)
      buffer.writePos = 128
      val bytebufs = buffer.buffers (sbyte, nbytes)
      assertResult (nbufs) (bytebufs.length)
      if (nbufs > 0) {
        assertResult (first) (bytebufs (0) .position)
        assertResult (last) (bytebufs (nbufs - 1) .limit)
      }
      for (i <- 0 until nbufs)
        assert (buffer.pages (i + spage) == bytebufs (i) .array)
    }}

  behavior of "PagedBuffer.buffers"
  buffers (0, 0, 0, 0, 0, 0)
  buffers (0, 1, 0, 1, 0, 1)
  buffers (0, 31, 0, 1, 0, 31)
  buffers (0, 32, 0, 1, 0, 32)
  buffers (7, 24, 0, 1, 7, 31)
  buffers (7, 25, 0, 1, 7, 32)
  buffers (32, 0, 0, 0, 0, 0)
  buffers (32, 1, 1, 1, 0, 1)
  buffers (32, 31, 1, 1, 0, 31)
  buffers (32, 32, 1, 1, 0, 32)
  buffers (39, 24, 1, 1, 7, 31)
  buffers (39, 25, 1, 1, 7, 32)
  buffers (0, 57, 0, 2, 0, 25)
  buffers (0, 63, 0, 2, 0, 31)
  buffers (0, 64, 0, 2, 0, 32)
  buffers (7, 50, 0, 2, 7, 25)
  buffers (7, 56, 0, 2, 7, 31)
  buffers (7, 57, 0, 2, 7, 32)
  buffers (32, 57, 1, 2, 0, 25)
  buffers (32, 63, 1, 2, 0, 31)
  buffers (32, 64, 1, 2, 0, 32)
  buffers (39, 50, 1, 2, 7, 25)
  buffers (39, 56, 1, 2, 7, 31)
  buffers (39, 57, 1, 2, 7, 32)

  def zeroAlign (offset: Int, bits: Int, length: Int) {
    it should (s"zero align offset=$offset, bits=$bits") in {
      val buffer = PagedBuffer (pageBits)
      buffer.writePos = offset
      buffer.writeZeroToAlign (bits)
      assertResult (offset + length) (buffer.writePos)
      buffer.readPos = offset
      for (i <- 0 until length)
        assertResult (0) (buffer.readByte())
    }}

  behavior of "PagedBuffer.zeroAlign"
  zeroAlign (0, 5, 0)
  zeroAlign (1, 5, 31)
  zeroAlign (31, 5, 1)
  zeroAlign (32, 5, 0)
  zeroAlign (33, 5, 31)
  zeroAlign (0, 3, 0)
  zeroAlign (1, 3, 7)
  zeroAlign (7, 3, 1)
  zeroAlign (8, 3, 0)
  zeroAlign (9, 3, 7)
  zeroAlign (0, 7, 0)
  zeroAlign (1, 7, 127)
  zeroAlign (127, 7, 1)
  zeroAlign (128, 7, 0)
  zeroAlign (129, 7, 127)

  def writeAndReadBytes (size: Int, srcOff: Int, dstOff: Int, len: Int) {
    it should (s"write and read bytes size=$size, srcOff=$srcOff, dstOff=$dstOff, len=$len") in {
      var bytes = Array.tabulate (size) (i => (i + 1).toByte)
      val buffer = PagedBuffer (pageBits)
      buffer.writePos = dstOff
      buffer.writeBytes (bytes, srcOff, len)
      assertResult (dstOff + len) (buffer.writePos)
      buffer.writeInt (0xDEADBEEF)
      bytes = Array.fill (size) (0)
      buffer.readPos = dstOff
      buffer.readBytes (bytes, 0, len)
      assertResult (dstOff + len) (buffer.readPos)
      for (i <- 0 until len)
        assertResult (srcOff+i+1, s"at pos=$i") (bytes (i))
      assertResult (0xDEADBEEF) (buffer.readInt())
    }}

  behavior of "A PagedBuffer"
  writeAndReadBytes (0, 0, 0, 0)
  writeAndReadBytes (1, 0, 0, 1)
  writeAndReadBytes (31, 0, 0, 31)
  writeAndReadBytes (32, 0, 0, 32)
  writeAndReadBytes (33, 0, 0, 32)
  writeAndReadBytes (63, 0, 0, 63)
  writeAndReadBytes (64, 0, 0, 64)
  writeAndReadBytes (65, 0, 0, 65)
  writeAndReadBytes (1, 0, 21, 1)
  writeAndReadBytes (10, 0, 21, 10)
  writeAndReadBytes (11, 0, 21, 11)
  writeAndReadBytes (12, 0, 21, 12)
  writeAndReadBytes (42, 0, 21, 42)
  writeAndReadBytes (43, 0, 21, 43)
  writeAndReadBytes (44, 0, 21, 44)
  writeAndReadBytes (32, 7, 21, 1)
  writeAndReadBytes (32, 7, 21, 10)
  writeAndReadBytes (32, 7, 21, 11)
  writeAndReadBytes (32, 7, 21, 12)
  writeAndReadBytes (64, 7, 21, 42)
  writeAndReadBytes (64, 7, 21, 43)
  writeAndReadBytes (64, 7, 21, 44)

  private def writeAndHashBytes (off: Int, len: Int) {
    it should (s"write and hash bytes off=$off, len=$len") in {
      val hashf = Hashing.murmur3_32()
      var bytes = Array.tabulate (len) (i => (i + 1).toByte)
      val buffer = PagedBuffer (pageBits)
      buffer.writePos = off
      buffer.writeBytes (bytes, 0, len)
      assertResult (off + len) (buffer.writePos)
      assertResult (hashf.hashBytes (bytes)) (buffer.hash (off, len, hashf))
    }}

  writeAndHashBytes (0, 0)
  writeAndHashBytes (0, 1)
  writeAndHashBytes (0, 31)
  writeAndHashBytes (0, 32)
  writeAndHashBytes (0, 63)
  writeAndHashBytes (0, 64)
  writeAndHashBytes (0, 65)
  writeAndHashBytes (21, 1)
  writeAndHashBytes (21, 10)
  writeAndHashBytes (21, 11)
  writeAndHashBytes (21, 12)
  writeAndHashBytes (21, 42)
  writeAndHashBytes (21, 43)
  writeAndHashBytes (21, 44)

  "An empty PagedBuffer" should "fail to read bytes" in {
    val buffer = PagedBuffer (pageBits)
    val bytes = Array [Byte] (15)
    intercept [BufferUnderflowException] (buffer.readBytes (bytes, 0, 16))
  }

  it should "fail to read a byte" in {
    val buffer = PagedBuffer (pageBits)
    intercept [BufferUnderflowException] (buffer.readByte())
  }

  it should "fail to read a short" in {
    val buffer = PagedBuffer (pageBits)
    intercept [BufferUnderflowException] (buffer.readShort())
  }

  it should "fail to read an int" in {
    val buffer = PagedBuffer (pageBits)
    intercept [BufferUnderflowException] (buffer.readInt())
  }

  it should "fail to read a var int" in {
    val buffer = PagedBuffer (pageBits)
    intercept [BufferUnderflowException] (buffer.readVarInt())
  }

  it should "fail to read an unsigned var int" in {
    val buffer = PagedBuffer (pageBits)
    intercept [BufferUnderflowException] (buffer.readVarUInt())
  }

  it should "fail to read a long" in {
    val buffer = PagedBuffer (pageBits)
    intercept [BufferUnderflowException] (buffer.readLong())
  }

  it should "fail to read a var long" in {
    val buffer = PagedBuffer (pageBits)
    intercept [BufferUnderflowException] (buffer.readVarLong())
  }

  it should "fail to read an unsigned var long" in {
    val buffer = PagedBuffer (pageBits)
    intercept [BufferUnderflowException] (buffer.readVarULong())
  }

  it should "fail to read a float" in {
    val buffer = PagedBuffer (pageBits)
    intercept [BufferUnderflowException] (buffer.readFloat())
  }

  it should "fail to read a double" in {
    val buffer = PagedBuffer (pageBits)
    intercept [BufferUnderflowException] (buffer.readDouble())
  }}

private object PagedBufferProperties extends PropSpec with PropertyChecks {

  property ("A PagedBuffer reads and writes shorts within a page") {
    forAll ("x") { x: Short =>
      val buffer = PagedBuffer (5)
      buffer.writeShort (x)
      assertResult (x) (buffer.readShort())
    }}

  property ("A PagedBuffer reads and writes shorts across a page boundry") {
    forAll ("x") { x: Short =>
      val buffer = PagedBuffer (1)
      buffer.writeShort (x)
      assertResult (x) (buffer.readShort())
    }}

  property ("A PagedBuffer reads and writes ints within a page") {
    forAll ("x") { x: Int =>
      val buffer = PagedBuffer (5)
      buffer.writeInt (x)
      assertResult (x) (buffer.readInt())
    }}

  property ("A PagedBuffer reads and writes ints across a page boundry") {
    forAll ("x") { x: Int =>
      val buffer = PagedBuffer (1)
      buffer.writeInt (x)
      assertResult (x) (buffer.readInt())
    }}

  property ("A PagedBuffer reads and writes var ints within a page") {
    forAll ("x") { x: Int =>
      val buffer = PagedBuffer (5)
      buffer.writeVarInt (x)
      assertResult (x) (buffer.readVarInt())
    }}

  property ("A PagedBuffer reads and writes var ints across a page boundry") {
    forAll ("x") { x: Int =>
      val buffer = PagedBuffer (1)
      buffer.writeVarInt (x)
      assertResult (x) (buffer.readVarInt())
    }}

  property ("A PagedBuffer reads and writes unsigned var ints within a page") {
    forAll ("x") { x: Int =>
      val buffer = PagedBuffer (5)
      buffer.writeVarUInt (x)
      assertResult (x) (buffer.readVarUInt())
    }}

  property ("A PagedBuffer reads and writes unsigned var ints across a page boundry") {
    forAll ("x") { x: Int =>
      val buffer = PagedBuffer (1)
      buffer.writeVarUInt (x)
      assertResult (x) (buffer.readVarUInt())
    }}

  property ("A PagedBuffer reads and writes longs within a page") {
    forAll ("x") { x: Long =>
      val buffer = PagedBuffer (5)
      buffer.writeLong (x)
      assertResult (x) (buffer.readLong())
    }}

  property ("A PagedBuffer reads and writes longs across a page boundry") {
    forAll ("x") { x: Long =>
      val buffer = PagedBuffer (1)
      buffer.writeLong (x)
      assertResult (x) (buffer.readLong())
    }}

  property ("A PagedBuffer reads and writes var longs within a page") {
    forAll ("x") { x: Byte =>
      val buffer = PagedBuffer (5)
      buffer.writeVarLong (-1L)
      assertResult (-1L) (buffer.readVarLong())
    }}

  property ("A PagedBuffer reads and writes var longs across a page boundry") {
    forAll ("x") { x: Long =>
      val buffer = PagedBuffer (1)
      buffer.writeVarLong (x)
      assertResult (x) (buffer.readVarLong())
    }}

  property ("A PagedBuffer reads and writes unsigned var longs within a page") {
    forAll ("x") { x: Long =>
      val buffer = PagedBuffer (5)
      buffer.writeVarULong (x)
      assertResult (x) (buffer.readVarULong())
    }}

  property ("A PagedBuffer reads and writes unsigned var longs across a page boundry") {
    forAll ("x") { x: Long =>
      val buffer = PagedBuffer (1)
      buffer.writeVarULong (x)
      assertResult (x) (buffer.readVarULong())
    }}

  property ("A PagedBuffer reads and writes floats within a page") {
    forAll ("x") { x: Float =>
      val buffer = PagedBuffer (5)
      buffer.writeFloat (x)
      assertResult (x) (buffer.readFloat())
    }}

  property ("A PagedBuffer reads and writes floats across a page boundry") {
    forAll ("x") { x: Float =>
      val buffer = PagedBuffer (1)
      buffer.writeFloat (x)
      assertResult (x) (buffer.readFloat())
    }}

  property ("A PagedBuffer reads and writes doubles within a page") {
    forAll ("x") { x: Double =>
      val buffer = PagedBuffer (5)
      buffer.writeDouble (x)
      assertResult (x) (buffer.readDouble())
    }}

  property ("A PagedBuffer reads and writes doubles across a page boundry") {
    forAll ("x") { x: Double =>
      val buffer = PagedBuffer (1)
      buffer.writeDouble (x)
      assertResult (x) (buffer.readDouble())
    }}

  property ("A PagedBuffer reads and writes strings within a page") {
    forAll ("x") { x: String =>
      val buffer = PagedBuffer (9)
      buffer.writeString (x)
      assertResult (x) (buffer.readString())
    }}

  property ("A PagedBuffer reads and writes strings across a page boundry") {
    forAll ("x") { x: String =>
      val buffer = PagedBuffer (3)
      buffer.writeString (x)
      assertResult (x) (buffer.readString())
    }}}
