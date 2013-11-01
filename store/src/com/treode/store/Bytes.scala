package com.treode.store

import java.nio.ByteBuffer
import java.nio.charset.{Charset, StandardCharsets}
import java.util.Arrays

import com.esotericsoftware.kryo.io.{Input, Output}
import com.google.common.primitives.UnsignedBytes
import com.treode.pickle._

class Bytes private (val bytes: Array [Byte]) extends Ordered [Bytes] {

  def unpickle [A] (p: Pickler [A]): A = {
    val buf = Buffer (12)
    buf.writeBytes (bytes, 0, bytes.length)
    val v = com.treode.pickle.unpickle (p, buf)
    require (buf.readableBytes == 0, "Bytes remain after unpickling.")
    v
  }

  /** Only applies if this was created using `Bytes (String, Charset)`. */
  def string (cs: Charset): String = {
    val b = ByteBuffer.wrap (bytes)
    cs.decode (b) .toString
  }

  /** Only applies if this was created using `Bytes (String)`. */
  def string: String =
    string (StandardCharsets.UTF_8)

  /** Only applies if this was created using `Bytes (Int)`. */
  def int: Int = unpickle (Picklers.fixedInt)

  /** Only applies if this was created using `Bytes (Long)`. */
  def long: Long = unpickle (Picklers.fixedLong)

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
    val buf = Buffer (12)
    com.treode.pickle.pickle (pk, v, buf)
    val bytes = new Array [Byte] (buf.readableBytes)
    buf.readBytes (bytes, 0, bytes.length)
    new Bytes (bytes)
  }

  /** Yield a Bytes object directly from the string. */
  def apply (s: String, cs: Charset = StandardCharsets.UTF_8): Bytes =
    new Bytes (s.getBytes (cs))

  /** Yield a Bytes object that will sort identically to the int. */
  def apply (n: Int): Bytes =
    Bytes (Picklers.fixedInt, n)

  /** Yield a Bytes object that will sort identically to the long. */
  def apply (n: Long): Bytes =
    Bytes (Picklers.fixedLong, n)

  def compare (x: Bytes, y: Bytes): Int =
    x compare y

  val pickle = {
    new Pickler [Bytes] {

      def p (v: Bytes, ctx: PickleContext) {
        ctx.writeVarUInt (v.bytes.length)
        ctx.writeBytes (v.bytes, 0, v.bytes.length)
      }

      def u (ctx: UnpickleContext): Bytes = {
        val length = ctx.readVarUInt()
        val bytes = new Array [Byte] (length)
        ctx.readBytes (bytes, 0, length)
        new Bytes (bytes)
      }}}}
