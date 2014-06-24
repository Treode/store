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

package com.treode.store.tier

import com.treode.pickle.Pickler
import com.treode.store.{Bytes, StorePicklers}

private class BloomFilter private (
    val numBits: Int,
    val numHashes: Int,
    val bits: Array [Long]
) extends TierPage {

  private def getBit (bit: Int): Boolean = {
    val longOfBits = bit >>> 6
    val bitOfLong = 1L << (bit & 0x3F)
    val k = bits (longOfBits)
    (k & bitOfLong) != 0
  }

  private def setBit (bit: Int) {
    val longOfBits = bit >>> 6
    val bitOfLong = 1L << (bit & 0x3F)
    val k = bits (longOfBits)
    if ((k & bitOfLong) == 0)
      bits (longOfBits) = k | bitOfLong
  }

  // See http://dl.acm.org/citation.cfm?id=1400125
  // or http://www.eecs.harvard.edu/~michaelm/postscripts/rsa2008.pdf
  // for more about how this computes multiple hashes.
  // See Guava's BloomFilter for another implementation of this technique.

  private def getHash (h: (Long, Long)): Boolean = {
    val (h1, h2) = h
    var k = h1
    var i = 0
    while (i < numHashes) {
      if (!getBit (((k & Long.MaxValue) % numBits).toInt))
        return false
      k += h2
      i += 1
    }
    return true
  }

  private def putHash (h: (Long, Long)) {
    val (h1, h2) = h
    var k = h1
    var i = 0
    while (i < numHashes) {
      setBit (((k & Long.MaxValue) % numBits).toInt)
      k += h2
      i += 1
    }}

  def contains (v: Bytes): Boolean =
    getHash (v.murmur128)

  def contains [A] (p: Pickler [A], v: A): Boolean =
    getHash (p.murmur128 (v))

  def put (v: Bytes): Unit =
    putHash (v.murmur128)

  def put [A] (p: Pickler [A], v: A): Unit =
    putHash (p.murmur128 (v))
}

private object BloomFilter {

  // See http://en.wikipedia.org/wiki/Bloom_filter
  // for more about the optimal number of bits and hashes.

  def optimalNumBits (numKeys: Double, fpp: Double): Long =
    math.max (1, math.round (-numKeys * math.log (fpp) / (math.log (2) * math.log (2))))

  def optimalNumHashes (numKeys: Double, numBits: Double): Long =
    math.max (1, math.round (numBits / numKeys * math.log (2)))

  def apply (numKeys: Long, fpp: Double): BloomFilter = {
    require (
        numKeys > 0,
        "Estimated number of keys must be greater than zero.")
    require (
        0 < fpp && fpp < 1,
        "Desired false positive probability must be between 0 and 1, exclusive.")
    val numBits = (optimalNumBits (numKeys, fpp) .toInt + 63) & (~0x3F)
    val numHashes = optimalNumHashes (numKeys, numBits) .toInt
    val bits = new Array [Long] (numBits >>> 6)
    new BloomFilter (numBits, numHashes, bits)
  }

  def apply (numHashes: Int, bits: Array [Long]): BloomFilter = {
    val numBits = bits.length << 6
    new BloomFilter (numBits, numHashes, bits)
  }

  // The key different between this BloomFilter and the one in Guava is that this one can be
  // serialized without Java serialization.
  val pickler = {
    import StorePicklers._
    wrap (uint, array (long))
    .build (v => apply (v._1, v._2))
    .inspect (v => (v.numHashes, v.bits))
  }}
