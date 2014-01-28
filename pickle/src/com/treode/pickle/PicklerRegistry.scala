package com.treode.pickle

import java.util.concurrent.ConcurrentHashMap
import scala.reflect.ClassTag

import com.treode.buffer.{PagedBuffer}

import PicklerRegistry._

class PicklerRegistry [T <: Tagged] private (default: Long => T) {

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
      default (id)
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
        v.tag.pickle (ctx)

      def u (ctx: UnpickleContext): T =
        PicklerRegistry.this.unpickle (ctx)
    }}

object PicklerRegistry {

  def error [T] (implicit tag: ClassTag [T]): Long => T = {
    val name = s"PicklerRegistry(${tag.runtimeClass.getSimpleName})"
    (id => throw new InvalidTagException (name, id))
  }

  def apply [T <: Tagged] (default: Long => T = error): PicklerRegistry [T] =
    new PicklerRegistry (default)

  trait Tagger {
    def pickle (ctx: PickleContext)
    def byteSize: Int
  }

  private class TaggerImpl [P] (p: Pickler [P], val id: Long, val v: P) extends Tagger {

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
        case that: TaggerImpl [_] =>
          id == that.id && v == that.v
        case _ =>
          false
      }

    override def toString: String =
      f"Tagger($id%X,$v)"
  }

  def tagger [P] (p: Pickler [P], id: Long, v: P): Tagger =
    new TaggerImpl (p, id, v)

  def pickler: Pickler [Tagger] =
    new Pickler [Tagger] {
      def p (v: Tagger, ctx: PickleContext): Unit = v.pickle (ctx)
      def u (ctx: UnpickleContext): Tagger = ???
    }

  trait Tagged {
    def tag: Tagger
  }

  trait TaggedFunction [A, B] extends (A => B) with Tagged

  def const [A, B] (v: B): TaggedFunction [A, B] =
    new TaggedFunction [A, B] {
      def apply (v2: A): B = v
      def tag = ???
    }

  def curried [P, A, B] (p: Pickler [P], id: Long) (f: P => A => B): P => TaggedFunction [A, B] =
    (v1 =>
      new TaggerImpl [P] (p, id, v1) with TaggedFunction [A, B] {
        def apply (v2: A) = f (v1) (v2)
        def tag = tagger (p, id, v1)
      })

  def curried [P, A, B] (reg: PicklerRegistry [TaggedFunction [A, B]], p: Pickler [P], id: Long) (f: P => A => B): Unit =
    reg.register (p, id) (curried (p, id) (f))

  def tupled [P, A, B] (p: Pickler [P], id: Long) (f: (P, A) => B): P => TaggedFunction [A, B] =
    (v1 =>
      new TaggerImpl [P] (p, id, v1) with TaggedFunction [A, B] {
        def apply (v2: A) = f (v1, v2)
        def tag = tagger (p, id, v1)
      })

  def tupled [P, A, B] (reg: PicklerRegistry [TaggedFunction [A, B]], p: Pickler [P], id: Long) (f: (P, A) => B): Unit =
    reg.register (p, id) (tupled (p, id) (f))

  def delayed [P, B] (p: Pickler [P], id: Long) (f: P => B): P => TaggedFunction [Unit, B] =
    (v1 =>
      new TaggerImpl [P] (p, id, v1) with TaggedFunction [Unit, B] {
        def apply (v2: Unit) = f (v1)
        def tag = tagger (p, id, v1)
      })

  def delayed [P, B] (reg: PicklerRegistry [TaggedFunction [Unit, B]], p: Pickler [P], id: Long) (f: P => B): Unit =
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
