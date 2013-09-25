package com.treode.pickle

import org.scalatest.FlatSpec

import Picklers._

class HashSpec extends FlatSpec {

  trait Hash [T] {
    def apply [A] (p: Pickler [A], v: A): T
    def apply (bytes: Array [Byte]): T
  }

  def aHash [T] (hash: Hash [T]) = {

    // The Hash was not designed to distinguish between four bytes and one int, or eight bytes and
    // one long, or two ints and one long, etc.  This test explores various interleavings of bytes,
    // ints, and longs to check if it's handling unaligned data properly.
    it should "agree whether bytes, ints or longs are written" in {

      val expected = hash (
          tuple (
              tuple (byte, byte, byte, byte),
              tuple (byte, byte, byte, byte),
              tuple (byte, byte, byte, byte),
              tuple (byte, byte, byte, byte),
              tuple (byte, byte)),
          (   (0x38.toByte, 0xE5.toByte, 0xA9.toByte, 0x53.toByte ),
              (0x31.toByte, 0x67.toByte, 0x09.toByte, 0xEA.toByte),
              (0x2E.toByte, 0x21.toByte, 0x18.toByte, 0x92.toByte),
              (0xC6.toByte, 0x66.toByte, 0xCF.toByte, 0xDA.toByte),
              (0x0E.toByte, 0x6B.toByte)))

      expectResult (expected) {
        hash (
            tuple (fixedInt, fixedInt, fixedInt, fixedInt, tuple (byte, byte)),
            (0x38E5A953, 0x316709EA, 0x2E211892, 0xC666CFDA, (0x0E.toByte, 0x6B.toByte)))
      }

      expectResult (expected) {
        hash (
            (tuple (fixedLong, fixedLong, tuple (byte, byte))),
            (0x38E5A953316709EAL, 0x2E211892C666CFDAL, (0x0E.toByte, 0x6B.toByte)))
      }

      expectResult (expected) {
        hash (
            tuple (
                tuple (byte, byte, fixedInt, fixedInt),
                tuple (fixedInt, fixedInt)),
            (   (0x38.toByte, 0xE5.toByte, 0xA9533167L.toInt, 0x09EA2E21),
                (0x1892C666, 0xCFDA0E6BL.toInt)))
      }

      expectResult (expected) {
        hash (
            (tuple (byte, fixedLong, fixedLong, byte)),
            (0x38.toByte, 0xE5A953316709EA2EL, 0x211892C666CFDA0EL, 0x6B.toByte))
      }}

    // The method which takes a pickler should be consistent with the method which takes an array
    // of bytes.  Then one user can pickle to a byte array before hashing and know that value may
    // be compare that of another user who pickled and hashed in one step.
    it should "agree when pickling and when handling bytes" in {
      val p1 = tuple (long, long)
      val v1 = (0x1CA946467D8L, 0xF3D0671A814L)
      expectResult (hash (p1, v1)) (hash (toByteArray (p1, v1)))

      val p2 = tuple (long, long)
      val v2 = (0xC40ED102015L, 0x2C96D9C0FBBL)
      val p3 = tuple (p1, p2)
      expectResult (hash (p3, (v1, v2))) (hash (toByteArray (p1, v1, toByteArray (p2, v2))))
    }}

  "Hash32" should behave like aHash (
      new Hash [Int] {
        def apply [A] (p: Pickler [A], v: A): Int = Hash32.hash (p, 0, v)
        def apply (bytes: Array [Byte]): Int = Hash32.hash (0, bytes)
      })

  "Hash128" should behave like aHash (
      new Hash [(Long, Long)] {
        def apply [A] (p: Pickler [A], v: A): (Long, Long) = Hash128.hash (p, 0, v)
        def apply (bytes: Array [Byte]): (Long, Long) = Hash128.hash (0, bytes)
      })
}
