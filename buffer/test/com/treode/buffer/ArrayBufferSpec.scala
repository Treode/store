package com.treode.buffer

import com.google.common.hash.Hashing
import org.scalatest.{FlatSpec, PropSpec, Suites}
import org.scalatest.prop.PropertyChecks

class ArrayBufferSpec extends Suites (ArrayBufferBehaviors, ArrayBufferProperties)

object ArrayBufferBehaviors extends FlatSpec {

  "An ArrayBuffer" should "hash bytes at the beginning" in {
    val hashf = Hashing.murmur3_32()
    var bytes = Array.tabulate (11) (i => (i + 1).toByte)
    val buf = ArrayBuffer (32)
    buf.writeBytes (bytes, 0, 11)
    assertResult (hashf.hashBytes (bytes)) (buf.hash (0, 11, hashf))
  }

  it should "hash bytes in the middle" in {
    val hashf = Hashing.murmur3_32()
    var bytes = Array.tabulate (11) (i => (i + 1).toByte)
    val buf = ArrayBuffer (32)
    buf.writePos = 7
    buf.writeBytes (bytes, 0, 11)
    assertResult (hashf.hashBytes (bytes)) (buf.hash (7, 11, hashf))
  }

  it should "hash bytes at the end" in {
    val hashf = Hashing.murmur3_32()
    var bytes = Array.tabulate (11) (i => (i + 1).toByte)
    val buf = ArrayBuffer (32)
    buf.writePos = 21
    buf.writeBytes (bytes, 0, 11)
    assertResult (hashf.hashBytes (bytes)) (buf.hash (21, 11, hashf))
  }}

object ArrayBufferProperties extends PropSpec with PropertyChecks {

  // We regard PageBuffer as the gold standard, and check that ArrayBuffer and read and write data
  // from one.  Whereas in PagedBufferSpec, we check that a PagedBuffer can read and write with
  // itself only, and not with ArrayBuffer.

  private def flip (in: PagedBuffer): ArrayBuffer = {
    val bytes = new Array [Byte] (in.readableBytes)
    in.readBytes (bytes, 0, in.readableBytes)
    ArrayBuffer (bytes)
  }

  private def flip (buf: ArrayBuffer): PagedBuffer = {
    val bytes = new Array [Byte] (buf.readableBytes)
    buf.readBytes (bytes, 0, buf.readableBytes)
    val out = PagedBuffer (12)
    out.writeBytes (bytes, 0, bytes.length)
    out
  }

  property ("An ArrayBuffer reads shorts") {
    forAll ("x") { x: Short =>
      val out = PagedBuffer (5)
      out.writeShort (x)
      val in = flip (out)
      assertResult (x) (in.readShort())
    }}

  property ("An ArrayBuffer writes shorts") {
    forAll ("x") { x: Short =>
      val out = ArrayBuffer (256)
      out.writeShort (x)
      val in = flip (out)
      assertResult (x) (in.readShort())
    }}

  property ("An ArrayBuffer reads ints") {
    forAll ("x") { x: Int =>
      val out = PagedBuffer (5)
      out.writeInt (x)
      val in = flip (out)
      assertResult (x) (in.readInt())
    }}

  property ("An ArrayBuffer writes ints") {
    forAll ("x") { x: Int =>
      val out = ArrayBuffer (256)
      out.writeInt (x)
      val in = flip (out)
      assertResult (x) (in.readInt())
    }}

  property ("An ArrayBuffer reads var ints") {
    forAll ("x") { x: Int =>
      val out = PagedBuffer (5)
      out.writeVarInt (x)
      val in = flip (out)
      assertResult (x) (in.readVarInt())
    }}


  property ("An ArrayBuffer writes var ints") {
    forAll ("x") { x: Int =>
      val out = ArrayBuffer (256)
      out.writeVarInt (x)
      val in = flip (out)
      assertResult (x) (in.readVarInt())
    }}

  property ("An ArrayBuffer reads unsigned var ints") {
    forAll ("x") { x: Int =>
      val out = PagedBuffer (5)
      out.writeVarUInt (x)
      val in = flip (out)
      assertResult (x) (in.readVarUInt())
    }}

  property ("An ArrayBuffer writes unsigned var ints") {
    forAll ("x") { x: Int =>
      val out = ArrayBuffer (256)
      out.writeVarUInt (x)
      val in = flip (out)
      assertResult (x) (in.readVarUInt())
    }}

  property ("An ArrayBuffer reads longs") {
    forAll ("x") { x: Long =>
      val out = PagedBuffer (5)
      out.writeLong (x)
      val in = flip (out)
      assertResult (x) (in.readLong())
    }}

  property ("An ArrayBuffer writes longs") {
    forAll ("x") { x: Long =>
      val out = ArrayBuffer (256)
      out.writeLong (x)
      val in = flip (out)
      assertResult (x) (in.readLong())
    }}

  property ("An ArrayBuffer reads var longs") {
    forAll ("x") { x: Byte =>
      val out = PagedBuffer (5)
      out.writeVarLong (-1L)
      val in = flip (out)
      assertResult (-1L) (in.readVarLong())
    }}

  property ("An ArrayBuffer writes var longs") {
    forAll ("x") { x: Byte =>
      val out = ArrayBuffer (256)
      out.writeVarLong (-1L)
      val in = flip (out)
      assertResult (-1L) (in.readVarLong())
    }}

  property ("An ArrayBuffer reads unsigned var longs") {
    forAll ("x") { x: Long =>
      val out = PagedBuffer (5)
      out.writeVarULong (x)
      val in = flip (out)
      assertResult (x) (in.readVarULong())
    }}

  property ("An ArrayBuffer writes unsigned var longs") {
    forAll ("x") { x: Long =>
      val out = ArrayBuffer (256)
      out.writeVarULong (x)
      val in = flip (out)
      assertResult (x) (in.readVarULong())
    }}

  property ("An ArrayBuffer reads floats") {
    forAll ("x") { x: Float =>
      val out = PagedBuffer (5)
      out.writeFloat (x)
      val in = flip (out)
      assertResult (x) (in.readFloat())
    }}

  property ("An ArrayBuffer writes floats") {
    forAll ("x") { x: Float =>
      val out = ArrayBuffer (256)
      out.writeFloat (x)
      val in = flip (out)
      assertResult (x) (in.readFloat())
    }}

  property ("An ArrayBuffer reads doubles") {
    forAll ("x") { x: Double =>
      val out = PagedBuffer (5)
      out.writeDouble (x)
      val in = flip (out)
      assertResult (x) (in.readDouble())
    }}

  property ("An ArrayBuffer writes doubles") {
    forAll ("x") { x: Double =>
      val out = ArrayBuffer (256)
      out.writeDouble (x)
      val in = flip (out)
      assertResult (x) (in.readDouble())
    }}

  property ("An ArrayBuffer reads strings") {
    forAll ("x") { x: String =>
      try {
      val out = PagedBuffer (9)
      out.writeString (x)
      val in = flip (out)
      assertResult (x) (in.readString())
      } catch {
        case e: Throwable => e.printStackTrace()
        throw e
      }
    }}

  property ("An ArrayBuffer writes strings") {
    forAll ("x") { x: String =>
      try {
      val out = ArrayBuffer (1024)
      out.writeString (x)
      val in = flip (out)
      assertResult (x) (in.readString())
      } catch {
        case e: Throwable => e.printStackTrace()
        throw e
      }
    }}}
