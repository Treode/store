package com.treode.pickle

import java.lang.{Double => JDouble, Float => JFloat}
import java.lang.Integer.{rotateLeft => rotl32}
import java.lang.Long.{rotateLeft => rotl64}

import io.netty.buffer.ByteBuf

// The constants come from MurmurHash3 as of Oct 5, 2011, which was published under the MIT license.
class Murmur32 (seed: Int) {

  private[this] val c1 = 0xCC9E2D51
  private[this] val c2 = 0x1B873593

  private[this] var h1 = seed

  def mix (_k1: Int) {
    var k1 = _k1
    k1 *= c1; k1 = rotl32 (k1, 15); k1 *= c2
    h1 ^= k1; h1 = rotl32 (h1, 13); h1 = h1 * 5 + 0xE6546B64
  }

  def finish (_k1: Int): Int = {
    var k1 = _k1
    k1 *= c1; k1  = rotl32 (k1, 15); k1 *= c2; h1 ^= k1
    h1
  }}

class HashContext32 (seed: Int) extends PickleContext {

  private[this] var k = 0
  private[this] var len = 0
  private[this] val hash = new Murmur32 (seed)

  def writeByte (v: Byte) {
    len & 0x3 match {
      case 0 => k = v.toInt & 0xFF
      case 1 => k = (k << 8) | (v.toInt & 0xFF)
      case 2 => k = (k << 8) | (v.toInt & 0xFF)
      case 3 => k = (k << 8) | (v.toInt & 0xFF); hash.mix (k)
    }
    len += 1
  }

  def writeBytes (v: Array[Byte], offset: Int, length: Int) {
    var i = offset
    while (i < offset + length) {
      writeByte (v (i))
      i += 1
    }}

  def writeShort (v: Short) {
    writeByte ((v >> 8).toByte)
    writeByte (v.toByte)
  }

  def writeInt (v: Int) {
    len & 0x3 match {
      case 0 => hash.mix (v)
      case 1 => k = (k << 24) | ((v >> 8) & 0xFFFFFF); hash.mix (k); k = v & 0xFF
      case 2 => k = (k << 16) | ((v >> 16) & 0xFFFF); hash.mix (k); k = v & 0xFFFF
      case 3 => k = (k << 8) | ((v >> 24) & 0xFF); hash.mix (k); k = v & 0xFFFFFF
    }
    len += 4
  }

  def writeLong (v: Long) {
    writeInt ((v >> 32).toInt)
    writeInt (v.toInt)
  }

  def writeFloat (v: Float): Unit =
    writeInt (JFloat.floatToRawIntBits (v))

  def writeDouble (v: Double): Unit =
    writeLong (JDouble.doubleToLongBits (v))

  def finish (): Int = {
    // Fill the tail with zeros.
    len & 0x3 match {
      case 0 => k = 0
      case 1 => k = k << 24
      case 2 => k = k << 16
      case 3 => ()
    }
    hash.finish (k)
  }}

object Hash32 {

  def hash [T] (p: Pickler [T], seed: Int, v: T): Int = {
    val h = new HashContext32 (seed)
    p.p (v, h)
    h.finish()
  }

  def hash (seed: Int, buffer: ByteBuf): Int = {
    val h = new HashContext32 (seed)
    h.writeBytes (buffer.array, buffer.readerIndex, buffer.readableBytes)
    h.finish()
  }}

class Murmur128 (seed: Long) {

  private[this] val c1 = 0x87C37B91114253D5L
  private[this] val c2 = 0x4CF5AD432745937FL

  private[this] var h1 = seed
  private[this] var h2 = seed

  def mix (_k1: Long, _k2: Long) {
    var k1 = _k1; var k2 = _k2
    k1 *= c1; k1 = rotl64 (k1, 31); k1 *= c2; h1 ^= k1
    h1 = rotl64 (h1, 27); h1 += h2; h1 = h1 * 5 + 0x52DCE729L
    k2 *= c2; k2 = rotl64 (k2, 33); k2 *= c1; h2 ^= k2
    h2 = rotl64 (h2, 31); h2 += h1; h2 = h2 * 5 + 0x38495AB5L
  }

  private def fmix (_k: Long): Long = {
    var k = _k
    k ^= k >> 33
    k *= 0xFF51AFD7ED558CCDL
    k ^= k >> 33
    k *= 0xC4CEB9FE1A85EC53L
    k ^= k >> 33
    k
  }

  def finish (_k1: Long, _k2: Long, len: Long): (Long, Long) = {
    var k1 = _k1; var k2 = _k2
    k1 *= c1; k1  = rotl64 (k1, 31); k1 *= c2; h1 ^= k1
    k2 *= c2; k2  = rotl64 (k2, 33); k2 *= c1; h2 ^= k2
    h1 ^= len; h2 ^= len
    h1 += h2; h2 += h1
    h1 = fmix (h1); h2 = fmix (h2)
    h1 += h2; h2 += h1
    (h1, h2)
  }}

class HashOutput128 (seed: Long) extends PickleContext {

  private[this] var k1 = 0L
  private[this] var k2 = 0L
  private[this] var len = 0
  private[this] val hash = new Murmur128 (seed)

  def writeByte (v: Byte) {
    val n = len & 0xF
    if (n < 8)
      k1 = (k1 << 8) | (v.toLong & 0xFFL)
    else if (n < 15)
      k2 = (k2 << 8) | (v.toLong & 0xFFL)
    else {
      k2 = (k2 << 8) | (v.toLong & 0xFFL)
      hash.mix (k1, k2)
    }
    len += 1
  }

  def writeBytes (v: Array[Byte], offset: Int, length: Int) {
    var i = offset
    while (i < offset + length) {
      writeByte (v (i))
      i += 1
    }}

  def writeShort (v: Short) {
    writeByte ((v >> 8).toByte)
    writeByte (v.toByte)
  }

  def writeInt (_v: Int) {
    val v = _v.toLong
    // If there is space in the current part of the key then great.  If this int crosses long
    // boundaries, then we need to break it up.  If we are at <8, we break it up over k1 and k2 and
    // are done.  If we are >=8, then we break it up over k2, mix and then k1.
    len & 0xF match {
      case n if n == 0 =>
        k1 = v & 0xFFFFFFFFL
      case n if n < 5 =>
        k1 = (k1 << 32) | (v & 0xFFFFFFFFL)
      case n if n == 5 =>
        k1 = (k1 << 24) | ((v >> 8) & 0xFFFFFFL)
        k2 = v & 0xFFL
      case n if n == 6 =>
        k1 = (k1 << 16) | ((v >> 16) & 0xFFFFL)
        k2 = v & 0xFFFFL
      case n if n == 7 =>
        k1 = (v >> 24) & 0xFFL
        k2 = v & 0xFFFFFFL
      case n if n == 8 =>
        k2 = v & 0xFFFFFFFFL
      case _ if len < 11 =>
        k2 = (k2 << 32) | (v & 0xFFFFFFFFL)
      case n if n == 12 =>
        k2 = (k2 << 32) | (v & 0xFFFFFFFFL)
        hash.mix (k1, k2)
      case n if n == 13 =>
        k2 = (k2 << 24) | ((v >> 8) & 0xFFFFFFL)
        hash.mix (k1, k2)
        k1 = v & 0xFFL
      case n if n == 14 =>
        k2 = (k2 << 16) | ((v >> 16) & 0xFFFFL)
        hash.mix (k1, k2)
        k1 = v & 0xFFFFL
      case n if n == 15 =>
        k2 = (k2 << 8) | ((v >> 24) & 0xFFL)
        hash.mix (k1, k2)
        k1 = v & 0xFFFFFFL
    }
    len += 4
  }

  def writeLong (v: Long) {
    // If we are on a long boundary then great.  Otherwise this long crosses boundaries, and we need
    // to break it up.  If we are at <8, we break it up over k1 and k2 and are done.  If we are >=8,
    // then we break it up over k2, mix and then k1.
    len & 0xF match {
      case n if n == 0 =>
        k1 = v
      case n if n == 1 =>
        k1 = (k1 << 56) | ((v >> 8) & 0xFFFFFFFFFFFFFFL)
        k2 = v & 0xFFL
      case n if n == 2 =>
        k1 = (k1 << 48) | ((v >> 16) & 0xFFFFFFFFFFFFL)
        k2 = v & 0xFFFFL
      case n if n == 3 =>
        k1 = (k1 << 40) | ((v >> 24) & 0xFFFFFFFFFFL)
        k2 = v & 0xFFFFFFL
      case n if n == 4 =>
        k1 = (k1 << 32) | ((v >> 32) & 0xFFFFFFFFL)
        k2 = v & 0xFFFFFFFFL
      case n if n == 5 =>
        k1 = (k1 << 24) | ((v >> 40) & 0xFFFFFFL)
        k2 = v & 0xFFFFFFFFFFL
      case n if n == 6 =>
        k1 = (k1 << 16) | ((v >> 48) & 0xFFFFL)
        k2 = v & 0xFFFFFFFFFFFFL
      case n if n == 7 =>
        k1 = (k1 << 8) | ((v >> 56) & 0xFFL)
        k2 = v & 0xFFFFFFFFFFFFFFL
      case n if n == 8 =>
        k2 = v
        hash.mix (k1, k2)
      case n if n == 9 =>
        k2 = (k2 << 56) | ((v >> 8) & 0xFFFFFFFFFFFFFFL)
        hash.mix (k1, k2)
        k1 = v & 0xFFL
      case n if n == 10 =>
        k2 = (k2 << 48) | ((v >> 16) & 0xFFFFFFFFFFFFL)
        hash.mix (k1, k2)
        k1 = v & 0xFFFFL
      case n if n == 11 =>
        k2 = (k2 << 40) | ((v >> 24) & 0xFFFFFFFFFFL)
        hash.mix (k1, k2)
        k1 = v & 0xFFFFFFL
      case n if n == 12 =>
        k2 = (k2 << 32) | ((v >> 32) & 0xFFFFFFFFL)
        hash.mix (k1, k2)
        k1 = v & 0xFFFFFFFFL
      case n if n == 13 =>
        k2 = (k2 << 24) | ((v >> 40) & 0xFFFFFFL)
        hash.mix (k1, k2)
        k1 = v & 0xFFFFFFFFFFL
      case n if n == 14 =>
        k2 = (k2 << 16) | ((v >> 48) & 0xFFFFL)
        hash.mix (k1, k2)
        k1 = v & 0xFFFFFFFFFFFFL
      case n if n == 15 =>
        k2 = (k2 << 8) | ((v >> 56) & 0xFFL)
        hash.mix (k1, k2)
        k1 = v & 0xFFFFFFFFFFFFFFL
    }
    len += 8
  }

  def writeFloat (v: Float): Unit =
    writeInt (JFloat.floatToRawIntBits (v))

  def writeDouble (v: Double): Unit =
    writeLong (JDouble.doubleToLongBits (v))

  def finish (): (Long, Long) = {
    // Fill the tail with zeros.
    len & 0xF match {
      case n if n == 0 =>
        k1 = 0
        k2 = 0
      case n if n < 7 =>
        k1 = k1 << ((8 - n) << 3)
        k2 = 0
      case n if n == 8 =>
        k2 = 0
      case n if n < 15 =>
        k2 = k2 << ((16 - n) << 3)
      case n if n == 15 =>
        ()
    }
    hash.finish (k1, k2, len)
  }}

object Hash128 {

  def hash [T] (p: Pickler [T], seed: Long, v: T): (Long, Long) = {
    val h = new HashOutput128 (seed)
    p.p (v, h)
    h.finish()
  }

  def hash (seed: Long, buffer: ByteBuf): (Long, Long) = {
    val h = new HashOutput128 (seed)
    h.writeBytes (buffer.array, buffer.readerIndex, buffer.readableBytes)
    h.finish()
  }}
