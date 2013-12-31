package com.treode.buffer

import org.scalatest.FlatSpec

class PackageSpec extends FlatSpec {

  def twopow (in: Int, out: Int) {
    it should (s"yield $out for $in") in {
      expectResult (out) (com.treode.buffer.twopow (in))
    }}

  behavior of "PagedBuffer.twopow"
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
}
