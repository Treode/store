package com.treode.disk

import java.util.Arrays
import com.treode.pickle.{Pickler, PickleContext, UnpickleContext}

class PageGroup private (val bytes: Array [Byte]) {

  def unpickle [A] (p: Pickler [A]): A =
    p.fromByteArray (bytes)

  def byteSize: Int =
    bytes.length + 5

  override def equals (other: Any): Boolean =
    other match {
      case that: PageGroup => Arrays.equals (this.bytes, that.bytes)
      case _ => false
    }

  override def hashCode: Int =
    Arrays.hashCode (bytes)

  override def toString: String =
    "PageGroup:%08X" format hashCode
}

object PageGroup {

  def apply (bytes: Array [Byte]): PageGroup =
    new PageGroup (bytes)

  def apply [A] (pk: Pickler [A], v: A): PageGroup =
    new PageGroup (pk.toByteArray (v))

  val pickler = {
    new Pickler [PageGroup] {

      def p (v: PageGroup, ctx: PickleContext) {
        ctx.writeVarUInt (v.bytes.length)
        ctx.writeBytes (v.bytes, 0, v.bytes.length)
      }

      def u (ctx: UnpickleContext): PageGroup = {
        val length = ctx.readVarUInt()
        val bytes = new Array [Byte] (length)
        ctx.readBytes (bytes, 0, length)
        new PageGroup (bytes)
      }}}}
