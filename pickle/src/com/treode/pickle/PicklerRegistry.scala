package com.treode.pickle

import java.util.concurrent.ConcurrentHashMap
import scala.reflect.ClassTag

import com.treode.buffer.{PagedBuffer}

import PicklerRegistry._

class PicklerRegistry [T <: Tag] private (default: Long => T) {

  private val openers = new ConcurrentHashMap [Long, Opener [T]]

  def register [P] (p: Pickler [P], tag: Long) (read: P => T) {
    val u1 = Opener (p, tag, read)
    val u0 = openers.putIfAbsent (tag, u1)
    require (u0 == null, f"$tag%X already registered")
  }

  def open [P] (p: Pickler [P]) (random: (Long, P => T)): Long = {
    var r = random
    var u1 = Opener (p, r._1, r._2)
    var u0 = openers.putIfAbsent (r._1, u1)
    while (u0 != null) {
      r = random
      u1 = Opener (p, r._1, r._2)
      u0 = openers.putIfAbsent (r._1, u1)
    }
    r._1
  }

  def unregister (tag: Long): Unit =
    openers.remove (tag)

  def unpickle (id: Long, ctx: UnpickleContext): T = {
    val u = openers.get (id)
    if (u == null)
      default (id)
    else
      u.read (ctx)
  }

  def unpickle (ctx: UnpickleContext): T =
    unpickle (ctx.readVarULong(), ctx)

  def unpickle (id: Long, buf: PagedBuffer, len: Int): T = {
    val end = buf.readPos + len
    val u = openers.get (id)
    if (u == null) {
      buf.readPos = end
      buf.discard (buf.readPos)
      return default (id)
    }
    val v = u.read (buf)
    if (buf.readPos != end) {
      buf.readPos = end
      buf.discard (buf.readPos)
      throw new FrameBoundsException
    }
    buf.discard (buf.readPos)
    v
  }

  def unpickle (buf: PagedBuffer, len: Int): T =
    unpickle (buf.readVarULong(), buf, len)

  def loopback [P] (p: Pickler [P], id: Long, v: P): T = {
    val buf = PagedBuffer (12)
    p.pickle (v, buf)
    unpickle (id, buf, buf.writePos)
  }

  val pickler: Pickler [T] =
    new Pickler [T] {

      def p (v: T, ctx: PickleContext): Unit =
        v.pickle (ctx)

      def u (ctx: UnpickleContext): T =
        PicklerRegistry.this.unpickle (ctx)
    }}

object PicklerRegistry {

  def apply [T <: Tag] (default: Long => T): PicklerRegistry [T] =
    new PicklerRegistry (default)

  def apply [T <: Tag] (name: String): PicklerRegistry [T] =
    new PicklerRegistry (id => throw new InvalidTagException (name, id))

  trait Tag {

    def id: Long
    def pickle (ctx: PickleContext)
    def byteSize: Int
  }

  class BaseTag [P] (p: Pickler [P], val id: Long, val v: P) extends Tag {

    def pickle (ctx: PickleContext) {
      ctx.writeVarULong (id)
      p.p (v, ctx)
    }

    def byteSize: Int =
      p.byteSize (v) + 9

    override def hashCode: Int =
      (id, v).hashCode

    override def equals (other: Any): Boolean =
      other match {
        case that: BaseTag [_] =>
          id == that.id && v == that.v
        case _ =>
          false
      }

    override def toString: String =
      f"Tag($id%X,$v)"
  }

  def tag [P] (p: Pickler [P], id: Long, v: P): Tag =
    new BaseTag (p, id, v)

  def pickler [T <: Tag]: Pickler [T] =
    new Pickler [T] {
      def p (v: T, ctx: PickleContext): Unit = v.pickle (ctx)
      def u (ctx: UnpickleContext): T = ???
    }

  trait FunctionTag [A, B] extends (A => B) with Tag

  def const [A, B] (_id: Long, v: B): FunctionTag [A, B] =
    new FunctionTag [A, B] {
      def id: Long = _id
      def apply (v2: A): B = v
      def pickle (ctx: PickleContext): Unit = ???
      def byteSize: Int = ???
      override def toString: String = f"Tag($id%X,$v)"
    }

  def curried [P, A, B] (p: Pickler [P], id: Long) (f: P => A => B): P => FunctionTag [A, B] =
    (v1 =>
      new BaseTag (p, id, v1) with FunctionTag [A, B] {
        def apply (v2: A) = f (v1) (v2)
        override def toString: String = f"Tag($id%X,$v1)"
      })

  def curried [P, A, B] (reg: PicklerRegistry [FunctionTag [A, B]], p: Pickler [P], id: Long) (f: P => A => B): Unit =
    reg.register (p, id) (curried (p, id) (f))

  def tupled [P, A, B] (p: Pickler [P], id: Long) (f: (P, A) => B): P => FunctionTag [A, B] =
    (v1 =>
      new BaseTag [P] (p, id, v1) with FunctionTag [A, B] {
        def apply (v2: A) = f (v1, v2)
        override def toString: String = f"Tag($id%X,$v1)"
      })

  def tupled [P, A, B] (reg: PicklerRegistry [FunctionTag [A, B]], p: Pickler [P], id: Long) (f: (P, A) => B): Unit =
    reg.register (p, id) (tupled (p, id) (f))

  def delayed [P, B] (p: Pickler [P], id: Long) (f: P => B): P => FunctionTag [Unit, B] =
    (v1 =>
      new BaseTag [P] (p, id, v1) with FunctionTag [Unit, B] {
        def apply (v2: Unit) = f (v1)
        override def toString: String = f"Tag($id%X,$v1)"
      })

  def delayed [P, B] (reg: PicklerRegistry [FunctionTag [Unit, B]], p: Pickler [P], id: Long) (f: P => B): Unit =
    reg.register (p, id) (delayed (p, id) (f))

  private trait Opener [T] {
    def read (ctx: UnpickleContext): T
    def read (buf: PagedBuffer): T
  }

  private object Opener {

    def apply [ID, P, T] (p: Pickler [P], id: Long, reader: P => T): Opener [T] =
      new Opener [T] {

        def read (ctx: UnpickleContext): T =
          reader (p.u (ctx))

        def read (buf: PagedBuffer): T =
            reader (p.unpickle (buf))

        override def toString = f"Opener($id%X)"
    }}}
