package com.treode.store.tier

import com.treode.pickle.Pickler
import com.treode.store.StorePicklers

private class BloomFilter private (
    val numBits: Int,
    val numHashes: Int,
    val bits: Array [Long]
) {

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

  private def getHash (h1: Long, h2: Long): Boolean = {
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

  private def putHash (h1: Long, h2: Long) {
    var k = h1
    var i = 0
    while (i < numHashes) {
      setBit (((k & Long.MaxValue) % numBits).toInt)
      k += h2
      i += 1
    }}

  def contains [A] (p: Pickler [A], v: A): Boolean = {
    val (h1, h2) = p.murmur128 (v)
    getHash (h1, h2)
  }

  def put [A] (p: Pickler [A], v: A) {
    val (h1, h2) = p.murmur128 (v)
    putHash (h1, h2)
  }}

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
