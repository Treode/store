package com.treode.pickle

import org.scalatest.prop.PropertyChecks
import org.scalatest.{FlatSpec, PropSpec, Specs}

class BufferSpec extends Specs (BufferBehaviors, BufferProperties)

private object BufferBehaviors extends FlatSpec {

  val pageBits = 5
  val pageSize = 32

  def twopow (in: Int, out: Int) {
    it should (s"yield $out for $in") in {
      expectResult (out) (Buffer.twopow (in))
    }}

  behavior of "Buffer.twopow"
  twopow (0, 1)
  twopow (1, 2)
  twopow (2, 4)
  twopow (3, 4)
  twopow (4, 8)
  twopow (5, 8)
  twopow (7, 8)
  twopow (8, 16)
  twopow (9, 16)
  twopow (15, 16)
  twopow (16, 32)
  twopow (17, 32)
  twopow (31, 32)
  twopow (32, 64)

  def capacity (nbytes: Int, npages: Int) {
    it should (s"add the right pages for nbytes=$nbytes") in {
      val buffer = Buffer (pageBits)
      buffer.capacity (nbytes)
      expectResult (npages << pageBits) (buffer.capacity)
      for (i <- 0 until npages)
        assert (buffer.pages (i) != null)
      for (i <- npages until buffer.pages.length)
        assert (buffer.pages (i) == null)
    }}

  behavior of "Buffer.capacity"
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
      val buffer = Buffer (pageBits)
      buffer.writePos  = (4 << pageBits) - 1
      buffer.readPos = nbytes
      val before = (Seq (buffer.pages: _*), buffer.writePos, buffer.readPos)
      buffer.discard (nbytes)
      expectResult ((4 - npages) << pageBits) (buffer.capacity)
      val after = (Seq (buffer.pages: _*), buffer.writePos, buffer.readPos)
      for (i <- 0 until before._1.length - npages)
        assert (before._1 (i + npages) == after._1 (i))
      for (i <- before._1.length - npages until after._1.length)
        assert (after._1 (i) == null)
      expectResult (before._2 - pageSize * npages) (after._2)
      expectResult (before._3 - pageSize * npages) (after._3)
    }}

  behavior of "Buffer.discard"
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
      val buffer = Buffer (pageBits)
      buffer.writePos = 128
      val bytebuf = buffer.buffer (sbyte, nbytes)
      expectResult (first) (bytebuf.position)
      expectResult (last) (bytebuf.limit)
      assert (buffer.pages (page) == bytebuf.array)
    }}

  behavior of "Buffer.buffer"
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
      val buffer = Buffer (pageBits)
      buffer.writePos = 128
      val bytebufs = buffer.buffers (sbyte, nbytes)
      expectResult (nbufs) (bytebufs.length)
      if (nbufs > 0) {
        expectResult (first) (bytebufs (0) .position)
        expectResult (last) (bytebufs (nbufs - 1) .limit)
      }
      for (i <- 0 until nbufs)
        assert (buffer.pages (i + spage) == bytebufs (i) .array)
    }}

  behavior of "Buffer.buffers"
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

  def readWriteBytes (size: Int, srcOff: Int, dstOff: Int, len: Int) {
    it should (s"read and write bytes size=$size, srcOff=$srcOff, dstOff=$dstOff, len=$len") in {
      var bytes = Array.tabulate (size) (i => (i + 1).toByte)
      val buffer = new Buffer (5)
      buffer.writePos = dstOff
      buffer.writeBytes (bytes, srcOff, len)
      expectResult (dstOff + len) (buffer.writePos)
      buffer.writeInt (0xDEADBEEF)
      bytes = Array.fill (size) (0)
      buffer.readPos = dstOff
      buffer.readBytes (bytes, 0, len)
      expectResult (dstOff + len) (buffer.readPos)
      for (i <- 0 until len)
        expectResult (srcOff+i+1, s"at pos=$i") (bytes (i))
      expectResult (0xDEADBEEF) (buffer.readInt())
    }}

  behavior of "A Buffer"
  readWriteBytes (0, 0, 0, 0)
  readWriteBytes (1, 0, 0, 1)
  readWriteBytes (31, 0, 0, 31)
  readWriteBytes (32, 0, 0, 32)
  readWriteBytes (33, 0, 0, 32)
  readWriteBytes (63, 0, 0, 63)
  readWriteBytes (64, 0, 0, 64)
  readWriteBytes (65, 0, 0, 65)
  readWriteBytes (1, 0, 21, 1)
  readWriteBytes (10, 0, 21, 10)
  readWriteBytes (11, 0, 21, 11)
  readWriteBytes (12, 0, 21, 12)
  readWriteBytes (42, 0, 21, 42)
  readWriteBytes (43, 0, 21, 43)
  readWriteBytes (44, 0, 21, 44)
  readWriteBytes (32, 7, 21, 1)
  readWriteBytes (32, 7, 21, 10)
  readWriteBytes (32, 7, 21, 11)
  readWriteBytes (32, 7, 21, 12)
  readWriteBytes (64, 7, 21, 42)
  readWriteBytes (64, 7, 21, 43)
  readWriteBytes (64, 7, 21, 44)
}

private object BufferProperties extends PropSpec with PropertyChecks {

  property ("A buffer reads and writes shorts within a page") {
    forAll ("x") { x: Short =>
      val buffer = Buffer (5)
      buffer.writeShort (x)
      expectResult (x) (buffer.readShort())
    }}

  property ("A buffer reads and writes shorts across a page boundry") {
    forAll ("x") { x: Short =>
      val buffer = Buffer (1)
      buffer.writeShort (x)
      expectResult (x) (buffer.readShort())
    }}

  property ("A buffer reads and writes ints within a page") {
    forAll ("x") { x: Int =>
      val buffer = Buffer (5)
      buffer.writeInt (x)
      expectResult (x) (buffer.readInt())
    }}

  property ("A buffer reads and writes ints across a page boundry") {
    forAll ("x") { x: Int =>
      val buffer = Buffer (1)
      buffer.writeInt (x)
      expectResult (x) (buffer.readInt())
    }}

  property ("A buffer reads and writes var ints within a page") {
    forAll ("x") { x: Int =>
      val buffer = Buffer (5)
      buffer.writeVarInt (x)
      expectResult (x) (buffer.readVarInt())
    }}

  property ("A buffer reads and writes var ints across a page boundry") {
    forAll ("x") { x: Int =>
      val buffer = Buffer (1)
      buffer.writeVarInt (x)
      expectResult (x) (buffer.readVarInt())
    }}

  property ("A buffer reads and writes unsigned var ints within a page") {
    forAll ("x") { x: Int =>
      val buffer = Buffer (5)
      buffer.writeVarUInt (x)
      expectResult (x) (buffer.readVarUInt())
    }}

  property ("A buffer reads and writes unsigned var ints across a page boundry") {
    forAll ("x") { x: Int =>
      val buffer = Buffer (1)
      buffer.writeVarUInt (x)
      expectResult (x) (buffer.readVarUInt())
    }}

  property ("A buffer reads and writes longs within a page") {
    forAll ("x") { x: Long =>
      val buffer = Buffer (5)
      buffer.writeLong (x)
      expectResult (x) (buffer.readLong())
    }}

  property ("A buffer reads and writes longs across a page boundry") {
    forAll ("x") { x: Long =>
      val buffer = Buffer (1)
      buffer.writeLong (x)
      expectResult (x) (buffer.readLong())
    }}

  property ("A buffer reads and writes var longs within a page") {
    forAll ("x") { x: Byte =>
      val buffer = Buffer (5)
      buffer.writeVarLong (-1L)
      expectResult (-1L) (buffer.readVarLong())
    }}

  property ("A buffer reads and writes var longs across a page boundry") {
    forAll ("x") { x: Long =>
      val buffer = Buffer (1)
      buffer.writeVarLong (x)
      expectResult (x) (buffer.readVarLong())
    }}

  property ("A buffer reads and writes unsigned var longs within a page") {
    forAll ("x") { x: Long =>
      val buffer = Buffer (5)
      buffer.writeVarULong (x)
      expectResult (x) (buffer.readVarULong())
    }}

  property ("A buffer reads and writes unsigned var longs across a page boundry") {
    forAll ("x") { x: Long =>
      val buffer = Buffer (1)
      buffer.writeVarULong (x)
      expectResult (x) (buffer.readVarULong())
    }}

  property ("A buffer reads and writes floats within a page") {
    forAll ("x") { x: Float =>
      val buffer = Buffer (5)
      buffer.writeFloat (x)
      expectResult (x) (buffer.readFloat())
    }}

  property ("A buffer reads and writes floats across a page boundry") {
    forAll ("x") { x: Float =>
      val buffer = Buffer (1)
      buffer.writeFloat (x)
      expectResult (x) (buffer.readFloat())
    }}

  property ("A buffer reads and writes doubles within a page") {
    forAll ("x") { x: Double =>
      val buffer = Buffer (5)
      buffer.writeDouble (x)
      expectResult (x) (buffer.readDouble())
    }}

  property ("A buffer reads and writes doubles across a page boundry") {
    forAll ("x") { x: Double =>
      val buffer = Buffer (1)
      buffer.writeDouble (x)
      expectResult (x) (buffer.readDouble())
    }}

  property ("A buffer reads and writes strings within a page") {
    forAll ("x") { x: String =>
      val buffer = Buffer (9)
      buffer.writeString (x)
      expectResult (x) (buffer.readString())
    }}

  property ("A buffer reads and writes strings across a page boundry") {
    forAll ("x") { x: String =>
      val buffer = Buffer (3)
      buffer.writeString (x)
      expectResult (x) (buffer.readString())
    }}}
