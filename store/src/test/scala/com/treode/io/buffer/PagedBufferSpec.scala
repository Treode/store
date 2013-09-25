/* Copyright (C) 2012-2013 Treode, Inc.
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

package com.treode.io.buffer

import org.scalatest.FlatSpec

class PagedBufferSpec extends FlatSpec {

  // This must match PagedBuffer.pageSize
  val pageSize = 1 << 13

  "A PagedBuffer" should "grow to accomodate a minimum" in {
    val b1 = PagedBuffer ()

    b1.capacity (1)
    expectResult (pageSize)(b1.capacity)

    b1.capacity (pageSize - 1)
    expectResult (pageSize)(b1.capacity)

    b1.capacity (pageSize)
    expectResult (pageSize)(b1.capacity)

    b1.capacity (pageSize + 1)
    expectResult (pageSize * 2)(b1.capacity)

    b1.writeAt = pageSize + 1234
    b1.readAt = pageSize + 1234
    b1.discard (pageSize + 1234)

    b1.capacity (1)
    expectResult (pageSize - 1234)(b1.capacity)

    b1.capacity (pageSize - 1)
    expectResult (pageSize * 2 - 1234)(b1.capacity)
  }

  it should "set and get a byte" in {
    val n = pageSize * 3
    val m = n - 1234
    val b = PagedBuffer ()
    b.capacity (m)
    expectResult ((0, 0, n))(b.readAt, b.writeAt, b.capacity)

    for (i <- 0 until m)
      b.setByte (i, i.toByte)
    expectResult ((0, 0, n))(b.readAt, b.writeAt, b.capacity)
    b.writeAt = m
    for (i <- 0 until m)
      expectResult (i.toByte)(b.getByte (i))
    expectResult ((0, m, n))(b.readAt, b.writeAt, b.capacity)
  }

  it should "write and read a byte" in {
    val n = pageSize * 3
    val m = n - 1234
    val b = PagedBuffer ()

    for (i <- 0 until m)
      b.writeByte (i.toByte)
    expectResult ((0, m, n))(b.readAt, b.writeAt, b.capacity)
    for (i <- 0 until m)
      expectResult (i.toByte)(b.readByte ())
    expectResult ((m, m, n))(b.readAt, b.writeAt, b.capacity)
  }

  it should "discard once" in {
    def checkDiscard (m2: Int, m3: Int) {
      val n = pageSize * 3
      val m = n - 2345
      val b = PagedBuffer ()

      for (i <- 0 until m)
        b.writeByte (i.toByte)
      val (m2, m3) = (1234, 2345)
      b.readAt = m3
      b.discard (m2)
      expectResult ((m3 - m2, m - m2, n - m2))(b.readAt, b.writeAt, b.capacity)
      for (i <- m2 until m)
        expectResult (i.toByte)(b.getByte (i - m2))
      for (i <- m3 until m)
        expectResult (i.toByte)(b.readByte ())
      expectResult ((m - m2, m - m2, n - m2))(b.readAt, b.writeAt, b.capacity)
    }

    checkDiscard (1234, 1234)
    checkDiscard (1234, 2345)
    checkDiscard (pageSize + 1234, 2345)
    checkDiscard (pageSize, 2345)
  }

  it should "discard twice" in {
    def checkDiscard (m1: Int, m2: Int) {
      val n = pageSize * 3
      val m = n - 1234
      val b = PagedBuffer ()

      for (i <- 0 until m)
        b.writeByte (i.toByte)
      val m1 = 1234
      b.readAt = m1
      b.discard (m1)
      expectResult ((0, m - m1, n - m1))(b.readAt, b.writeAt, b.capacity)
      for (i <- m1 until m)
        expectResult (i.toByte)(b.getByte (i - m1))
      val m2 = 2345
      b.readAt = m2
      b.discard (m2)
      expectResult ((0, m - m1 - m2, n - m1 - m2))(b.readAt, b.writeAt, b.capacity)
      for (i <- m1 + m2 until m)
        expectResult (i.toByte)(b.getByte (i - m1 - m2))
    }

    checkDiscard (1234, 2345)
    checkDiscard (pageSize + 1234, 2345)
    checkDiscard (1234, pageSize + 2345)
    checkDiscard (4567, 5678)
    checkDiscard (pageSize + 1234, pageSize + 2345)
    checkDiscard (pageSize, 2345)
    checkDiscard (1234, pageSize + 2345)
  }

  it should "slice and dice" in {
    def checkSlice (discard: Int, npages: Int) = {
      val n = pageSize * npages
      val (m1, m2, m3) = (2345, n + 4567, n + 7890)
      val b = PagedBuffer ()

      for (i <- 0 until m3)
        b.writeByte (i.toByte)
      b.readAt = discard
      b.discard (discard)
      val b2 = b.slice (m1 - discard, m2 - m1)
      expectResult ((0, m2 - m1))(b2.readAt, b2.writeAt)
      for (i <- m1 until m2)
        expectResult (i.toByte)(b2.readByte ())
    }

    checkSlice (0, 0)
    checkSlice (0, 1)
    checkSlice (0, 2)
    checkSlice (1234, 0)
    checkSlice (1234, 1)
    checkSlice (1234, 2)
  }

  it should "set and get byte arrays" in {
    def checkSetBytes (npages: Int) = {
      val n = pageSize * npages
      val (m1, m2, m3) = (123, n + 456, n + 789)
      val b = PagedBuffer ()

      for (i <- 0 until m3)
        b.writeByte (i.toByte)
      val bytes = new Array[Byte](m2 - m1)
      for (i <- 0 until m2 - m1)
        bytes (i) = i.toByte
      b.setBytes (m1, bytes, 0, bytes.length)

      for (i <- 0 until 123)
        expectResult (i.toByte)(b.readByte ())
      for (i <- 0 until m2 - m1)
        expectResult (i.toByte)(b.readByte ())
      for (i <- m2 until m3)
        expectResult (i.toByte)(b.readByte ())
    }

    def checkGetBytes (npages: Int) = {
      val n = pageSize * npages
      val (m1, m2, m3) = (123, n + 456, n + 789)
      val b = PagedBuffer ()

      for (i <- 0 until m3)
        b.writeByte (i.toByte)
      val bytes = new Array[Byte](m2 - m1)
      b.getBytes (m1, bytes, 0, bytes.length)
      for (i <- m1 until m2)
        expectResult (i.toByte)(bytes (i - m1))
    }

    // Within a page, crossing one boundary, crossing two boundaries
    checkSetBytes (0)
    checkGetBytes (0)
    checkSetBytes (1)
    checkGetBytes (1)
    checkSetBytes (2)
    checkGetBytes (2)
  }

  it should "set and get byte arrays after discarding" in {
    def checkSetBytes (npages: Int) = {
      val n = pageSize * npages
      val (m0, m1, m2, m3) = (123, 234, n + 456, n + 789)
      val b = PagedBuffer ()

      for (i <- 0 until m3)
        b.writeByte (i.toByte)
      b.readAt = m0
      b.discard (m0)
      val bytes = new Array[Byte](m2 - m1)
      for (i <- 0 until m2 - m1)
        bytes (i) = i.toByte
      b.setBytes (m1 - m0, bytes, 0, bytes.length)

      for (i <- m0 until m1)
        expectResult (i.toByte)(b.readByte ())
      for (i <- 0 until m2 - m1)
        expectResult (i.toByte)(b.readByte ())
      for (i <- m2 until m3)
        expectResult (i.toByte)(b.readByte ())
    }

    def checkGetBytes (npages: Int) = {
      val n = pageSize * npages
      val (m0, m1, m2, m3) = (123, 234, n + 456, n + 789)
      val b = PagedBuffer ()

      for (i <- 0 until m3)
        b.writeByte (i.toByte)
      b.readAt = m0
      b.discard (m0)
      val bytes = new Array[Byte](m2 - m1)
      b.getBytes (m1 - m0, bytes, 0, bytes.length)
      for (i <- m1 until m2)
        expectResult (i.toByte)(bytes (i - m1))
    }

    // Within a page, crossing one boundary, crossing two boundaries
    checkSetBytes (0)
    checkGetBytes (0)
    checkSetBytes (1)
    checkGetBytes (1)
    checkSetBytes (2)
    checkGetBytes (2)
  }

  it should "provide readable ByteBuffers" in {

    def checkBuffers (discard: Int, readAt: Int, writeAt: Int, pos: Int, limit: Int, len: Int) {
      val b = PagedBuffer ()
      b.capacity (writeAt)
      b.writeAt = writeAt
      b.readAt = readAt
      b.discard (discard)
      val bs = b.readableByteBuffers
      expectResult (len)(bs.length)
      if (bs.length > 0) {
        expectResult (pos)(bs (0).position)
        expectResult (limit)(bs (bs.length - 1).limit)
      }
      if (bs.length > 1) {
        expectResult (pageSize)(bs (0).limit)
        expectResult (0)(bs (bs.length - 1).position)
      }
    }

    checkBuffers (0, 0, 0, 0, 0, 1)
    checkBuffers (0, 0, 7890, 0, 7890, 1)
    checkBuffers (0, 0, pageSize, 0, pageSize, 1)
    checkBuffers (0, 1234, 7890, 1234, 7890, 1)
    checkBuffers (0, 1234, pageSize, 1234, pageSize, 1)
    checkBuffers (0, 7890, 7890, 7890, 7890, 1)
    checkBuffers (0, pageSize, pageSize, -1, -1, 0)

    checkBuffers (123, 123, 123, 123, 123, 1)
    checkBuffers (123, 123, 7890, 123, 7890, 1)
    checkBuffers (123, 123, pageSize, 123, pageSize, 1)
    checkBuffers (123, 1234, 7890, 1234, 7890, 1)
    checkBuffers (123, 1234, pageSize, 1234, pageSize, 1)
    checkBuffers (123, 7890, 7890, 7890, 7890, 1)
    checkBuffers (123, pageSize, pageSize, -1, -1, 0)

    checkBuffers (0, 0, pageSize + 7890, 0, 7890, 2)
    checkBuffers (0, 0, pageSize * 2, 0, pageSize, 2)
    checkBuffers (0, 1234, pageSize + 7890, 1234, 7890, 2)
    checkBuffers (0, 1234, pageSize * 2, 1234, pageSize, 2)
    checkBuffers (0, pageSize + 7890, pageSize + 7890, 7890, 7890, 1)
    checkBuffers (0, pageSize * 2, pageSize * 2, -1, -1, 0)

    checkBuffers (0, pageSize, pageSize + 7890, 0, 7890, 1)
    checkBuffers (0, pageSize, pageSize * 2, 0, pageSize, 1)
    checkBuffers (0, pageSize + 1234, pageSize + 7890, 1234, 7890, 1)
    checkBuffers (0, pageSize + 1234, pageSize * 2, 1234, pageSize, 1)

    checkBuffers (pageSize, pageSize, pageSize + 7890, 0, 7890, 1)
    checkBuffers (pageSize, pageSize, pageSize * 2, 0, pageSize, 1)
    checkBuffers (pageSize, pageSize + 1234, pageSize + 7890, 1234, 7890, 1)
    checkBuffers (pageSize, pageSize + 1234, pageSize * 2, 1234, pageSize, 1)
  }

  it should "provide writable ByteBuffers" in {
    def checkBuffers (discard: Int, writeAt: Int, capacity: Int, pos: Int, limit: Int, len: Int) {
      val b = PagedBuffer ()
      b.capacity (capacity)
      b.writeAt = writeAt
      b.readAt = discard
      b.discard (discard)
      val bs = b.writableByteBuffers
      expectResult (len)(bs.length)
      if (bs.length > 0) {
        expectResult (pos)(bs (0).position)
        expectResult (limit)(bs (bs.length - 1).limit)
      }
      if (bs.length > 1) {
        expectResult (pageSize)(bs (0).limit)
        expectResult (0)(bs (bs.length - 1).position)
      }
    }

    checkBuffers (0, 0, 0, 0, pageSize, 1)
    checkBuffers (0, 0, 7890, 0, pageSize, 1)
    checkBuffers (0, 0, pageSize, 0, pageSize, 1)
    checkBuffers (0, 1234, 7890, 1234, pageSize, 1)
    checkBuffers (0, 1234, pageSize, 1234, pageSize, 1)
    checkBuffers (0, 7890, 7890, 7890, pageSize, 1)
    checkBuffers (0, pageSize, pageSize, -1, -1, 0)

    checkBuffers (123, 123, 123, 123, pageSize, 1)
    checkBuffers (123, 123, 7890, 123, pageSize, 1)
    checkBuffers (123, 123, pageSize, 123, pageSize, 1)
    checkBuffers (123, 1234, 7890, 1234, pageSize, 1)
    checkBuffers (123, 1234, pageSize, 1234, pageSize, 1)
    checkBuffers (123, 7890, 7890, 7890, pageSize, 1)
    checkBuffers (123, pageSize, pageSize, -1, -1, 0)

    checkBuffers (0, 0, pageSize + 7890, 0, pageSize, 2)
    checkBuffers (0, 0, pageSize * 2, 0, pageSize, 2)
    checkBuffers (0, 1234, pageSize + 7890, 1234, pageSize, 2)
    checkBuffers (0, 1234, pageSize * 2, 1234, pageSize, 2)
    checkBuffers (0, pageSize + 7890, pageSize + 7890, 7890, pageSize, 1)
    checkBuffers (0, pageSize * 2, pageSize * 2, -1, -1, 0)

    checkBuffers (pageSize, pageSize, pageSize + 7890, 0, pageSize, 1)
    checkBuffers (pageSize, pageSize, pageSize * 2, 0, pageSize, 1)
    checkBuffers (pageSize, pageSize + 1234, pageSize + 7890, 1234, pageSize, 1)
    checkBuffers (pageSize, pageSize + 1234, pageSize * 2, 1234, pageSize, 1)
  }
}
