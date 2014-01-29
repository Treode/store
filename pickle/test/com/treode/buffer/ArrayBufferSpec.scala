package com.treode.buffer

import org.scalatest.prop.PropertyChecks
import org.scalatest.PropSpec

class ArrayBufferSpec extends PropSpec with PropertyChecks {

  def toBytes (buf: PagedBuffer): ArrayBuffer = {
    val bytes = new Array [Byte] (buf.readableBytes)
    buf.readBytes (bytes, 0, buf.readableBytes)
    ArrayBuffer (bytes)
  }

  property ("An ArrayBuffer reads shorts") {
    forAll ("x") { x: Short =>
      val out = PagedBuffer (5)
      out.writeShort (x)
      val in = toBytes (out)
      expectResult (x) (in.readShort())
    }}

  property ("An ArrayBuffer reads ints") {
    forAll ("x") { x: Int =>
      val out = PagedBuffer (5)
      out.writeInt (x)
      val in = toBytes (out)
      expectResult (x) (in.readInt())
    }}

  property ("An ArrayBuffer reads var ints") {
    forAll ("x") { x: Int =>
      val out = PagedBuffer (5)
      out.writeVarInt (x)
      val in = toBytes (out)
      expectResult (x) (in.readVarInt())
    }}

  property ("An ArrayBuffer reads unsigned var ints") {
    forAll ("x") { x: Int =>
      val out = PagedBuffer (5)
      out.writeVarUInt (x)
      val in = toBytes (out)
      expectResult (x) (in.readVarUInt())
    }}

  property ("An ArrayBuffer reads longs") {
    forAll ("x") { x: Long =>
      val out = PagedBuffer (5)
      out.writeLong (x)
      val in = toBytes (out)
      expectResult (x) (in.readLong())
    }}

  property ("An ArrayBuffer reads var longs") {
    forAll ("x") { x: Byte =>
      val out = PagedBuffer (5)
      out.writeVarLong (-1L)
      val in = toBytes (out)
      expectResult (-1L) (in.readVarLong())
    }}

  property ("An ArrayBuffer reads unsigned var longs") {
    forAll ("x") { x: Long =>
      val out = PagedBuffer (5)
      out.writeVarULong (x)
      val in = toBytes (out)
      expectResult (x) (in.readVarULong())
    }}

  property ("An ArrayBuffer reads floats") {
    forAll ("x") { x: Float =>
      val out = PagedBuffer (5)
      out.writeFloat (x)
      val in = toBytes (out)
      expectResult (x) (in.readFloat())
    }}

  property ("An ArrayBuffer reads doubles") {
    forAll ("x") { x: Double =>
      val out = PagedBuffer (5)
      out.writeDouble (x)
      val in = toBytes (out)
      expectResult (x) (in.readDouble())
    }}

  property ("An ArrayBuffer reads strings") {
    forAll ("x") { x: String =>
      try {
      val out = PagedBuffer (9)
      out.writeString (x)
      val in = toBytes (out)
      expectResult (x) (in.readString())
      } catch {
        case e: Throwable => e.printStackTrace()
        throw e
      }
    }}}
