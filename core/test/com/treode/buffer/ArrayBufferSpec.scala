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

import com.google.common.hash.Hashing
import org.scalatest.FlatSpec
import org.scalatest.prop.PropertyChecks

import PropertyChecks._

class ArrayBufferSpec extends FlatSpec {

  "An ArrayBuffer" should "hash bytes at the beginning" in {
    val hashf = Hashing.murmur3_32()
    val bytes = Array.tabulate (11) (i => (i + 1).toByte)
    val buf = ArrayBuffer.writable (32)
    buf.writeBytes (bytes, 0, 11)
    assertResult (hashf.hashBytes (bytes)) (buf.hash (0, 11, hashf))
  }

  it should "hash bytes in the middle" in {
    val hashf = Hashing.murmur3_32()
    val bytes = Array.tabulate (11) (i => (i + 1).toByte)
    val buf = ArrayBuffer.writable (32)
    buf.writePos = 7
    buf.writeBytes (bytes, 0, 11)
    assertResult (hashf.hashBytes (bytes)) (buf.hash (7, 11, hashf))
  }

  it should "hash bytes at the end" in {
    val hashf = Hashing.murmur3_32()
    val bytes = Array.tabulate (11) (i => (i + 1).toByte)
    val buf = ArrayBuffer.writable (32)
    buf.writePos = 21
    buf.writeBytes (bytes, 0, 11)
    assertResult (hashf.hashBytes (bytes)) (buf.hash (21, 11, hashf))
  }

  // We regard PageBuffer as the gold standard, and check that ArrayBuffer can read and write data
  // from one.  Whereas in PagedBufferSpec, we check that a PagedBuffer can read and write with
  // itself only, and not with ArrayBuffer.
  private def flip (in: PagedBuffer): ArrayBuffer = {
    val bytes = new Array [Byte] (in.readableBytes)
    in.readBytes (bytes, 0, in.readableBytes)
    ArrayBuffer.readable (bytes)
  }

  private def flip (buf: ArrayBuffer): PagedBuffer = {
    val bytes = new Array [Byte] (buf.readableBytes)
    buf.readBytes (bytes, 0, buf.readableBytes)
    val out = PagedBuffer (12)
    out.writeBytes (bytes, 0, bytes.length)
    out
  }

  it should "read shorts" in {
    forAll ("x") { x: Short =>
      val out = PagedBuffer (5)
      out.writeShort (x)
      val in = flip (out)
      assertResult (x) (in.readShort())
    }}

  it should "write shorts" in {
    forAll ("x") { x: Short =>
      val out = ArrayBuffer.writable (256)
      out.writeShort (x)
      val in = flip (out)
      assertResult (x) (in.readShort())
    }}

  it should "read ints" in {
    forAll ("x") { x: Int =>
      val out = PagedBuffer (5)
      out.writeInt (x)
      val in = flip (out)
      assertResult (x) (in.readInt())
    }}

  it should "write ints" in {
    forAll ("x") { x: Int =>
      val out = ArrayBuffer.writable (256)
      out.writeInt (x)
      val in = flip (out)
      assertResult (x) (in.readInt())
    }}

  it should "read var ints" in {
    forAll ("x") { x: Int =>
      val out = PagedBuffer (5)
      out.writeVarInt (x)
      val in = flip (out)
      assertResult (x) (in.readVarInt())
    }}

  it should "write var ints" in {
    forAll ("x") { x: Int =>
      val out = ArrayBuffer.writable (256)
      out.writeVarInt (x)
      val in = flip (out)
      assertResult (x) (in.readVarInt())
    }}

  it should "read unsigned var ints" in {
    forAll ("x") { x: Int =>
      val out = PagedBuffer (5)
      out.writeVarUInt (x)
      val in = flip (out)
      assertResult (x) (in.readVarUInt())
    }}

  it should "write unsigned var ints" in {
    forAll ("x") { x: Int =>
      val out = ArrayBuffer.writable (256)
      out.writeVarUInt (x)
      val in = flip (out)
      assertResult (x) (in.readVarUInt())
    }}

  it should "read longs" in {
    forAll ("x") { x: Long =>
      val out = PagedBuffer (5)
      out.writeLong (x)
      val in = flip (out)
      assertResult (x) (in.readLong())
    }}

  it should "write longs" in {
    forAll ("x") { x: Long =>
      val out = ArrayBuffer.writable (256)
      out.writeLong (x)
      val in = flip (out)
      assertResult (x) (in.readLong())
    }}

  it should "read var longs" in {
    forAll ("x") { x: Byte =>
      val out = PagedBuffer (5)
      out.writeVarLong (-1L)
      val in = flip (out)
      assertResult (-1L) (in.readVarLong())
    }}

  it should "write var longs" in {
    forAll ("x") { x: Byte =>
      val out = ArrayBuffer.writable (256)
      out.writeVarLong (-1L)
      val in = flip (out)
      assertResult (-1L) (in.readVarLong())
    }}

  it should "read unsigned var longs" in {
    forAll ("x") { x: Long =>
      val out = PagedBuffer (5)
      out.writeVarULong (x)
      val in = flip (out)
      assertResult (x) (in.readVarULong())
    }}

  it should "write unsigned var longs" in {
    forAll ("x") { x: Long =>
      val out = ArrayBuffer.writable (256)
      out.writeVarULong (x)
      val in = flip (out)
      assertResult (x) (in.readVarULong())
    }}

  it should "read floats" in {
    forAll ("x") { x: Float =>
      val out = PagedBuffer (5)
      out.writeFloat (x)
      val in = flip (out)
      assertResult (x) (in.readFloat())
    }}

  it should "write floats" in {
    forAll ("x") { x: Float =>
      val out = ArrayBuffer.writable (256)
      out.writeFloat (x)
      val in = flip (out)
      assertResult (x) (in.readFloat())
    }}

  it should "read doubles" in {
    forAll ("x") { x: Double =>
      val out = PagedBuffer (5)
      out.writeDouble (x)
      val in = flip (out)
      assertResult (x) (in.readDouble())
    }}

  it should "write doubles" in {
    forAll ("x") { x: Double =>
      val out = ArrayBuffer.writable (256)
      out.writeDouble (x)
      val in = flip (out)
      assertResult (x) (in.readDouble())
    }}

  it should "read strings" in {
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

  it should "write strings" in {
    forAll ("x") { x: String =>
      try {
      val out = ArrayBuffer.writable (1024)
      out.writeString (x)
      val in = flip (out)
      assertResult (x) (in.readString())
      } catch {
        case e: Throwable => e.printStackTrace()
        throw e
      }
    }}}
