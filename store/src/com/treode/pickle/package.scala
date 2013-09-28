package com.treode

import scala.collection.mutable

import io.netty.buffer.{ByteBuf, Unpooled}

package pickle {

/** Superclass of all pickling and unpickling exceptions. */
class PickleException extends Exception

/** A tagged structure encountered an unknown tag. */
class InvalidTagException (name: String, found: Long) extends PickleException {
  override def getMessage = "Invalid tag for " + name + ", found " + found
}

class BytesRemainException (p: String) extends PickleException {
  override def getMessage = "Bytes remain after unpickling " + p
}

abstract class PickleContext private [pickle] {

  private [this] val m = mutable.Map [Any, Int]()

  private [pickle] def contains (v: Any) = m contains v

  private [pickle] def get (v: Any) = m (v)

  private [pickle] def put (v: Any) = m.put (v, m.size)

  def writeVariableLengthUnsignedInt (v: Int) {
    var u = v
    while ((u & 0xFFFFFF80) != 0) {
      writeByte ((u & 0x7F | 0x80).toByte)
      u = u >>> 7
    }
    writeByte ((u & 0x7F).toByte)
  }

  def writeVariableLengthInt (v: Int): Unit =
    writeVariableLengthUnsignedInt ((v << 1) ^ (v >> 31))

  def writeVariableLengthUnsignedLong (v: Long) {
    var u = v
    while ((u & 0xFFFFFF80) != 0) {
      writeByte ((u & 0x7F | 0x80).toByte)
      u = u >>> 7
    }
    writeByte ((u & 0x7F).toByte)
  }

  def writeVariableLengthLong (v: Long): Unit =
    writeVariableLengthUnsignedLong ((v << 1) ^ (v >> 63))

  def writeByte (v: Byte)
  def writeShort (v: Short)
  def writeInt (v: Int)
  def writeLong (v: Long)
  def writeFloat (v: Float)
  def writeDouble (v: Double)
  def writeBytes (data: Array [Byte], offset: Int, length: Int)
}

private [pickle] class ByteBufPickleContext (buffer: ByteBuf) extends PickleContext {

  def writeByte (v: Byte) = buffer.writeByte (v)
  def writeInt (v: Int) = buffer.writeInt (v)
  def writeShort (v: Short) = buffer.writeShort (v)
  def writeLong (v: Long) = buffer.writeLong (v)
  def writeFloat (v: Float) = buffer.writeFloat (v)
  def writeDouble (v: Double) = buffer.writeDouble (v)
  def writeBytes (v: Array [Byte], offset: Int, length: Int) = buffer.writeBytes (v, offset, length)
}

abstract class UnpickleContext private [pickle] {

  private [this] val m = mutable.Map [Int, Any]()

  private [pickle] def get [A] (idx: Int) = m (idx) .asInstanceOf [A]

  private [pickle] def put [A] (v: A) = m.put (m.size, v)

  def readVariableLengthInt(): Int = {
    val b1 = readByte().toInt
    var b = b1
    var v = b & 0x7F
    var shift = 7
    while ((b & 0x80) != 0) {
      b = readByte().toInt
      v = v | ((b & 0x7F) << shift)
      shift += 7
    }
    (if ((b1 & 1) == 0) 0 else -1) ^ (v >>> 1)
  }

  def readVariableLengthUnsignedInt(): Int = {
    val b1 = readByte().toInt
    var b = b1
    var v = b & 0x7F
    var shift = 7
    while ((b & 0x80) != 0) {
      b = readByte().toInt
      v = v | ((b & 0x7F) << shift)
      shift += 7
    }
    v
  }

  def readVariableLengthLong(): Long = {
    val b1 = readByte().toLong
    var b = b1
    var v = b & 0x7F
    var shift = 7
    while ((b & 0x80) != 0) {
      b = readByte().toLong
      v = v | ((b & 0x7F) << shift)
      shift += 7
    }
    (if ((b1 & 1) == 0) 0 else -1) ^ (v >>> 1)
  }

  def readVariableLengthUnsignedLong(): Long = {
    val b1 = readByte().toLong
    var b = b1
    var v = b & 0x7F
    var shift = 7
    while ((b & 0x80) != 0) {
      b = readByte().toLong
      v = v | ((b & 0x7F) << shift)
      shift += 7
    }
    v
  }

  def readByte(): Byte
  def readShort(): Short
  def readInt(): Int
  def readLong(): Long
  def readFloat(): Float
  def readDouble(): Double
  def readBytes (data: Array [Byte], offset: Int, length: Int)
}

private [pickle] class ByteBufUnpickleContext (buffer: ByteBuf) extends UnpickleContext {

  def readByte() = buffer.readByte()
  def readShort() = buffer.readShort()
  def readInt() = buffer.readInt()
  def readLong() = buffer.readLong()
  def readFloat() = buffer.readFloat()
  def readDouble() = buffer.readDouble()
  def readBytes (data: Array [Byte], offset: Int, length: Int) = buffer.readBytes (data, offset, length)
}

/** How to read and write an object of a particular type. */
trait Pickler [A] {
  def p (v: A, ctx: PickleContext)

  def u (ctx: UnpickleContext): A
}

}

package object pickle {

  def hash32 [A] (p: Pickler [A], seed: Int, v: A) = Hash32.hash (p, seed, v)

  def hash64 [A] (p: Pickler [A], seed: Long, v: A) = Hash128.hash (p, seed, v)._2

  def hash128 [A] (p: Pickler [A], seed: Long, v: A) = Hash128.hash (p, seed, v)

  def hash32 (seed: Int, b: ByteBuf) = Hash32.hash (seed, b)

  def hash64 (seed: Long, b: ByteBuf) = Hash128.hash (seed, b)._2

  def hash128 (seed: Long, b: ByteBuf) = Hash128.hash (seed, b)

  def pickle [A] (p: Pickler [A], v: A, b: ByteBuf) =
    p.p (v, new ByteBufPickleContext (b))

  def unpickle [A] (p: Pickler [A], b: ByteBuf): A =
    p.u (new ByteBufUnpickleContext (b))

  def toByteBuf [A] (p: Pickler [A], v: A, extra: ByteBuf = Unpooled.EMPTY_BUFFER): ByteBuf = {
    val b = Unpooled.buffer ()
    pickle (p, v, b)
    b.writeBytes (extra)
    b
  }

  def fromByteBuf [A] (p: Pickler [A], b: ByteBuf, extra: Boolean = false): A = {
    val v = unpickle (p, b)
    if (b.readableBytes > 0 && !extra)
      throw new BytesRemainException (p.toString)
    v
  }}
