package com.treode.store

import java.util.Arrays

import com.treode.pickle._
import io.netty.buffer.{Unpooled, ByteBuf}

class Bytes (val bytes: Array [Byte]) {

  def unpickle [A] (p: Pickler [A]): A = {
    val buf = Unpooled.wrappedBuffer (bytes)
    val v = com.treode.pickle.unpickle (p, buf)
    buf.release()
    require (buf.readableBytes == 0, "Bytes remain after unpickling.")
    v
  }

  override def equals (other: Any) =
    other match {
      case that: Bytes => Arrays.equals (this.bytes, that.bytes)
      case _ => false
    }

  override def hashCode = Arrays.hashCode (bytes)

  override def toString = "Bytes:%08X" format hashCode
}

object Bytes {

  def apply [A] (pk: Pickler [A], v: A): Bytes = {
    val buf = Unpooled.buffer()
    com.treode.pickle.pickle (pk, v, buf)
    val bytes = new Bytes (Arrays.copyOf (buf.array, buf.readableBytes))
    buf.release()
    bytes
  }

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
