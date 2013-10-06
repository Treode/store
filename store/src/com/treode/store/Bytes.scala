package com.treode.store

import java.nio.ByteBuffer
import java.nio.charset.{Charset, StandardCharsets}
import java.util.Arrays

import com.google.common.primitives.UnsignedBytes
import com.treode.pickle._
import io.netty.buffer.{Unpooled, ByteBuf}

class Bytes private (val bytes: Array [Byte]) extends Ordered [Bytes] {

  def unpickle [A] (p: Pickler [A]): A = {
    val buf = Unpooled.wrappedBuffer (bytes)
    val v = com.treode.pickle.unpickle (p, buf)
    buf.release()
    require (buf.readableBytes == 0, "Bytes remain after unpickling.")
    v
  }

  def string (cs: Charset): String = {
    val b = ByteBuffer.wrap (bytes)
    cs.decode (b) .toString
  }

  def string: String =
    string (StandardCharsets.UTF_8)

  def compare (that: Bytes): Int =
    UnsignedBytes.lexicographicalComparator.compare (this.bytes, that.bytes)

  override def equals (other: Any) =
    other match {
      case that: Bytes => Arrays.equals (this.bytes, that.bytes)
      case _ => false
    }

  override def hashCode = Arrays.hashCode (bytes)

  override def toString = "Bytes:%08X" format hashCode
}

object Bytes extends Ordering [Bytes] {

  def apply (bytes: Array [Byte]): Bytes =
    new Bytes (bytes)

  def apply [A] (pk: Pickler [A], v: A): Bytes = {
    val buf = Unpooled.buffer()
    com.treode.pickle.pickle (pk, v, buf)
    val bytes = new Bytes (Arrays.copyOf (buf.array, buf.readableBytes))
    buf.release()
    bytes
  }

  /** Yield a Bytes object that will sort identically to the string; using the string pickler will
    * yield a Bytes object that sorts first by string length.
    */
  def apply (s: String, cs: Charset = StandardCharsets.UTF_8): Bytes =
    new Bytes (s.getBytes (cs))

  def apply (n: Int): Bytes =
    Bytes (Picklers.fixedInt, n)

  def compare (x: Bytes, y: Bytes): Int =
    x compare y

  val pickle = {
    new Pickler [Bytes] {

      def p (v: Bytes, ctx: PickleContext) {
        ctx.writeVariableLengthUnsignedInt (v.bytes.length)
        ctx.writeBytes (v.bytes, 0, v.bytes.length)
      }

      def u (ctx: UnpickleContext): Bytes = {
        val length = ctx.readVariableLengthUnsignedInt()
        val bytes = new Array [Byte] (length)
        ctx.readBytes (bytes, 0, length)
        new Bytes (bytes)
      }}}}
