package com.treode.disk

import java.util.concurrent.ConcurrentHashMap
import com.treode.buffer.Input
import com.treode.pickle.{InvalidTagException, Pickler, PickleContext, UnpickleContext}

private class TagRegistry [T] {
  import TagRegistry.Opener

  private val opener = new ConcurrentHashMap [Long, Opener [T]]

  def register [P] (p: Pickler [P], tag: Long) (read: P => T) {
    val u = Opener (p, tag, read)
    val u0 = opener.putIfAbsent (tag, u)
    require (u0 == null, s"$tag already registered")
  }

  def unregister (tag: Long): Unit =
    opener.remove (tag)

  def unpickle (ctx: UnpickleContext): T = {
    val id = ctx.readVarULong
    val u = opener.get (id)
    if (u == null)
      throw new InvalidTagException ("registry", id)
    u.read (ctx)
  }

  def unpickler: Pickler [T] =
    new Pickler [T] {
      def p (v: T, ctx: PickleContext): Unit = ???
      def u (ctx: UnpickleContext): T = TagRegistry.this.unpickle (ctx)
    }}

private object TagRegistry {

  private trait Opener [T] {
    def read (ctx: UnpickleContext): T
  }

  private object Opener {

    def apply [ID, P, T] (p: Pickler [P], id: ID, reader: P => T): Opener [T] =
      new Opener [T] {
        def read (ctx: UnpickleContext): T = reader (p.u (ctx))
        override def toString = s"Opener($id)"
    }}

  trait Tagger {
    def size: Int
    def pickle (ctx: PickleContext)
  }

  private class TaggerImpl [P] (p: Pickler [P], val id: Long, val v: P)
  extends Tagger {

    def size: Int =
      com.treode.pickle.size (p, v) + 9

    def pickle (ctx: PickleContext) {
      ctx.writeVarULong (id)
      p.p (v, ctx)
    }

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
      s"Tagger($id,$v)"
  }

  def tagger [P] (p: Pickler [P], id: Long, v: P): Tagger =
    new TaggerImpl (p, id, v)

  def pickler: Pickler [Tagger] =
    new Pickler [Tagger] {
      def p (v: Tagger, ctx: PickleContext): Unit = v.pickle (ctx)
      def u (ctx: UnpickleContext): Tagger = ???
    }}
