package com.treode.io.buffer

import java.io.{EOFException, InputStream, OutputStream}
import java.lang.{Double => JDouble, Float => JFloat}
import java.nio.ByteBuffer
import scala.language.implicitConversions

trait ReadableStream {

  def readByte (): Byte

  def readBytes (v: Array[Byte]) {
    var i = 0
    while (i < v.length) {
      v (i) = readByte ()
      i += 1
    }
  }

  def readBytes (v: ByteBuffer) {
    while (v.remaining > 0)
      v.put (readByte ())
  }

  def readBytes (v: ByteBuffer, off: Int, len: Int) {
    val end = off + len
    var i = off
    while (i < end) {
      v.put (readByte ())
      i += 1
    }
  }

  private[this] def readi (shift: Int): Int =
    (readByte ().toInt & 0xFF) << shift

  def readShort (): Short =
    (readi (8) | readi (0)).toShort

  def readInt (): Int =
    readi (24) | readi (16) | readi (8) | readi (0)

  private def readl (shift: Int): Long =
    (readByte ().toLong & 0xFF) << shift

  def readLong (): Long =
    readl (56) | readl (48) | readl (40) | readl (32) | readl (24) | readl (16) | readl (8) | readl (0)

  def readFloat (): Float = JFloat.intBitsToFloat (readInt ())

  def readDouble (): Double = JDouble.longBitsToDouble (readLong ())
}

private class JavaReadableStream (in: InputStream) extends ReadableStream {

  def readByte (): Byte =
    in.read () match {
      case x if x < 0 => throw new EOFException
      case x => x.toByte
    }
}

object ReadableStream {

  implicit def apply (in: InputStream): ReadableStream = new JavaReadableStream (in)
}

trait ReadableStreamEnvoy {

  protected val stream: ReadableStream

  def readByte() = stream.readByte()
  def readBytes (v: Array [Byte]) = stream.readBytes (v)
  def readBytes (v: ByteBuffer) = stream.readBytes (v)
  def readBytes (v: ByteBuffer, off: Int, len: Int) = stream.readBytes (v, off, len)
  def readShort() = stream.readShort()
  def readInt() = stream.readInt()
  def readLong() = stream.readLong()
  def readFloat() = stream.readFloat()
  def readDouble() = stream.readDouble()
}

trait WritableStream {

  def writeByte (v: Byte): Unit

  def writeBytes (v: Array[Byte]) {
    var i = 0
    while (i < v.length) {
      writeByte (v (i))
      i += 1
    }
  }

  def writeBytes (v: ByteBuffer) {
    while (v.remaining > 0)
      writeByte (v.get)
  }

  def writeShort (v: Short) {
    writeByte ((v >> 8).toByte)
    writeByte (v.toByte)
  }

  def writeInt (v: Int): Unit = {
    writeByte ((v >> 24).toByte)
    writeByte ((v >> 16).toByte)
    writeByte ((v >> 8).toByte)
    writeByte (v.toByte)
  }

  def writeLong (v: Long) {
    writeByte ((v >> 56).toByte)
    writeByte ((v >> 48).toByte)
    writeByte ((v >> 40).toByte)
    writeByte ((v >> 32).toByte)
    writeByte ((v >> 24).toByte)
    writeByte ((v >> 16).toByte)
    writeByte ((v >> 8).toByte)
    writeByte (v.toByte)
  }

  def writeFloat (v: Float): Unit =
    writeInt (JFloat.floatToRawIntBits (v))

  def writeDouble (v: Double): Unit =
    writeLong (JDouble.doubleToRawLongBits (v))
}

private class JavaWritableStream (out: OutputStream) extends WritableStream {

  def writeByte (value: Byte) = out.write (value)
}

object WritableStream {

  implicit def apply (out: OutputStream): WritableStream = new JavaWritableStream (out)
}

trait WritableStreamEnvoy extends WritableStream {

  protected val stream: WritableStream

  def writeByte (v: Byte) = stream.writeByte (v)
  override def writeBytes (v: Array [Byte]) = stream.writeBytes (v)
  override def writeBytes (v: ByteBuffer) = stream.writeBytes (v)
  override def writeShort (v: Short) = stream.writeShort (v)
  override def writeInt (v: Int) = stream.writeInt (v)
  override def writeLong (v: Long) = stream.writeLong (v)
  override def writeFloat (v: Float) = stream.writeFloat (v)
  override def writeDouble (v: Double) = stream.writeDouble (v)
}
