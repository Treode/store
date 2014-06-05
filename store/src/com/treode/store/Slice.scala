package com.treode.store

import java.lang.Integer.highestOneBit

import com.treode.async.AsyncIterator
import com.treode.pickle.Pickler

case class Slice (slice: Int, nslices: Int) {
  require (
      1 <= nslices && highestOneBit (nslices) == nslices,
      "Number of slices must be a power of two and at least one.")
  require (
      0 <= slice && slice < nslices,
      "The slice must be between 0 (inclusive) and the number of slices (exclusive)")

  def contains (n: Int): Boolean =
    (n & (nslices-1)) == slice

  def contains [A] (p: Pickler [A], v: A): Boolean =
    contains (p.murmur32 (v))
}

object Slice {

  val pickler = {
    import StorePicklers._
    wrap (tuple (uint, uint))
    .build (v => Slice (v._1, v._2))
    .inspect (v => (v.slice, v.nslices))
  }}
