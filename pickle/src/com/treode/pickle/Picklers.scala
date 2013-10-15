package com.treode.pickle

import scala.collection.generic.GenericCompanion
import scala.language.{existentials, higherKinds, implicitConversions}
import scala.reflect.ClassTag
import java.lang.Enum

trait Picklers {
  import Picklers.Tag

  val byte: Pickler [Byte] =
    new Pickler [Byte] {
      def p (v: Byte, ctx: PickleContext) = ctx.writeByte (v)
      def u (ctx: UnpickleContext) = ctx.readByte()
      override def toString = "byte"
    }

  val boolean: Pickler [Boolean] =
    new Pickler [Boolean] {
      def p (v: Boolean, ctx: PickleContext) = byte.p (if (v) 1 else 0, ctx)
      def u (ctx: UnpickleContext) = if (byte.u (ctx) == 0) false else true
      override def toString = "boolean"
    }

  val int: Pickler [Int] =
    new Pickler [Int] {
      def p (v: Int, ctx: PickleContext) = ctx.writeVarInt (v)
      def u (ctx: UnpickleContext) = ctx.readVarInt()
      override def toString = "int"
    }

  val long: Pickler [Long] =
    new Pickler [Long] {
      def p (v: Long, ctx: PickleContext) = ctx.writeVarLong (v)
      def u (ctx: UnpickleContext) = ctx.readVarLong()
      override def toString = "long"
    }

  val uint: Pickler [Int] =
    new Pickler [Int] {
      def p (v: Int, ctx: PickleContext) = ctx.writeVarUInt (v)
      def u (ctx: UnpickleContext) = ctx.readVarUInt()
      override def toString = "int"
    }

  val ulong: Pickler [Long] =
    new Pickler [Long] {
      def p (v: Long, ctx: PickleContext) = ctx.writeVarULong (v)
      def u (ctx: UnpickleContext) = ctx.readVarULong()
      override def toString = "long"
    }

  val fixedInt: Pickler [Int] =
    new Pickler [Int] {
      def p (v: Int, ctx: PickleContext) = ctx.writeInt (v)
      def u (ctx: UnpickleContext) = ctx.readInt()
      override def toString = "fixedInt"
    }

  val fixedLong: Pickler [Long] =
    new Pickler [Long] {
      def p (v: Long, ctx: PickleContext) = ctx.writeLong (v)
      def u (ctx: UnpickleContext) = ctx.readLong()
      override def toString = "fixedLong"
    }

  val float: Pickler [Float]  =
    new Pickler [Float] {
      def p (v: Float, ctx: PickleContext) = ctx.writeFloat (v)
      def u (ctx: UnpickleContext) = ctx.readFloat()
      override def toString = "float"
    }

  val double: Pickler [Double] =
    new Pickler [Double] {
      def p (v: Double, ctx: PickleContext) = ctx.writeDouble (v)
      def u (ctx: UnpickleContext) = ctx.readDouble()
      override def toString = "double"
    }

  val string: Pickler [String] =
    new Pickler [String] {
      def p (v: String, ctx: PickleContext) = ctx.writeString (v)
      def u (ctx: UnpickleContext) = ctx.readString()
      override def toString = "string"
    }

  def const [T] (v: T): Pickler [T] =
    new Pickler [T] {
      def p (v: T, ctx: PickleContext) = ()
      def u (ctx: UnpickleContext) = v
      override def toString = "const (" + v + ")"
    }

  val unit = const (())

  def laze [T] (pa: => Pickler [T]): Pickler [T] =
    new Pickler [T] {
      lazy val _pa = pa
      def p (v: T, ctx: PickleContext) = _pa.p (v, ctx)
      def u (ctx: UnpickleContext) = _pa.u (ctx)
      override def toString = "laze (" + pa.toString + ")"
    }

  implicit def intFormatToTag [A] (tf: (Int, Pickler [A])) (implicit ct: ClassTag [A]): Tag [A] =
    Tag [A] (tf._1.toLong, ct.runtimeClass.asInstanceOf [Class [A]], tf._2)

  implicit def longFormatToTag [A] (tf: (Long, Pickler [A])) (implicit ct: ClassTag [A]): Tag [A] =
    Tag [A] (tf._1, ct.runtimeClass.asInstanceOf [Class [A]], tf._2)

  /** Write per first matching class. */
  def tagged [A] (tags: Tag [_ <: A]*) (implicit ct: ClassTag [A]): Pickler [A] =
    new Pickler [A] {

      val ps = Map (tags map (tag => (tag.tag, tag)): _*)
      require (ps.size == tags.size, "Tag reused.")

      def p (v: A, ctx: PickleContext): Unit = {
        tags.find (_.clazz.isInstance (v)) match {
          case Some (tag) => tag.write (v, ctx)
          case None => throw new Exception ("No tag type for " + v.getClass.getName)
        }}

      def u (in: UnpickleContext): A = {
        val n = long.u (in)
        ps.get (n) match {
          case Some (tag) => tag.read (in)
          case None => throw new InvalidTagException (ct.runtimeClass.getSimpleName, n)
        }}

      override def toString = "tagged (" + ct.runtimeClass.getSimpleName + ")"
    }

  def array [A] (pa: Pickler [A]) (implicit ct: ClassTag [A]): Pickler [Array [A]] = {
    require (pa != null)

    new Pickler [Array [A]] {

      def p (v: Array [A], ctx: PickleContext): Unit = {
        ctx.writeVarUInt (v.length)
        var i = 0
        while (i < v.length) {
          pa.p (v (i), ctx)
          i += 1
        }}

      def u (ctx: UnpickleContext) = {
        val n = ctx.readVarUInt()
        val a = new Array [A] (n)
        var i = 0
        while (i < n) {
          a (i) = pa.u (ctx)
          i += 1
        }
        a
      }

      override def toString = "array (" + pa + ")"
    }}

  private [this] def useBuilder [A, C [A] <: Iterable [A]]
      (c: GenericCompanion [C], s: String)
      (pa: Pickler [A])
      (implicit ct: ClassTag [A]): Pickler [C [A]] = {
    require (pa != null)

    new Pickler [C [A]] {

      def p (v: C [A], ctx: PickleContext): Unit = {
        ctx.writeVarUInt (v.size)
        v foreach (pa.p (_, ctx))
      }

      def u (ctx: UnpickleContext) = {
        val n = ctx.readVarUInt()
        val b = c.newBuilder [A]
        b.sizeHint (n)
        (1 to n) foreach (_ => b += pa.u (ctx))
        b.result()
      }

      override def toString = s + " (" + pa + ")"
    }}

  def list [A] (pa: Pickler [A]) (implicit ct: ClassTag [A]) = useBuilder (List, "list") (pa)

  def seq [A] (pa: Pickler [A]) (implicit ct: ClassTag [A]) = useBuilder (Seq, "seq") (pa)

  def set [A] (pa: Pickler [A]) (implicit ct: ClassTag [A]) = useBuilder (Set, "set") (pa)

  def map [K, V] (pk: Pickler [K], pv: Pickler [V]) = {
    require (pk != null)
    require (pv != null)
    wrap (list (tuple (pk, pv))) (kvs => Map (kvs: _*)) (m => m.toList)
  }

  def option [A] (pa: Pickler [A]): Pickler [Option [A]] = {
    require (pa != null)

    new Pickler [Option [A]] {

      def p (v: Option [A], ctx: PickleContext): Unit = {
        if (v.isEmpty) {
          ctx.writeVarUInt (0)
        } else {
          ctx.writeVarUInt (1)
          pa.p (v.get, ctx)
        }}

      def u (ctx: UnpickleContext) = {
        val i = ctx.readVarUInt()
        if (i == 0)
          None
        else
          Some (pa.u (ctx))
      }

      override def toString = "option (" + pa + ")"
    }}

  def either [A, B] (pa: Pickler [A], pb: Pickler [B]): Pickler [Either [A, B]] = {
    require (pa != null)
    require (pb != null)

    new Pickler [Either [A, B]] {

      def p (v: Either [A, B], ctx: PickleContext): Unit = {
        if (v.isLeft) {
          ctx.writeVarUInt (0)
          pa.p (v.left.get, ctx)
        } else {
          ctx.writeVarUInt (1)
          pb.p (v.right.get, ctx)
        }}

      def u (ctx: UnpickleContext) = {
        val i = ctx.readVarUInt()
        if (i == 0)
          Left (pa.u (ctx))
        else
          Right (pb.u (ctx))
      }

      override def toString = "either " + (pa, pb)
    }}

  object java {
    import _root_.java.lang.Enum
    import _root_.java.util

    def enum [E <: Enum [_]] (es: Array [E]): Pickler [E] = {
      new Pickler [E] {
        def p (v: E, ctx: PickleContext) = ctx.writeVarUInt (v.ordinal)
        def u (ctx: UnpickleContext) = es (ctx.readVarUInt())
      }}

    def list [A] (pa: Pickler [A]): Pickler [util.List [A]] = {
      require (pa != null)

      new Pickler [util.List [A]] {

        def p (v: util.List [A], ctx: PickleContext) = {
          ctx.writeInt (v.size)
          for (i <- 0 until v.size)
            pa.p (v.get (i), ctx)
        }

        def u (ctx: UnpickleContext) = {
          val l = ctx.readInt
          val v = new util.ArrayList [A] (l)
          for (i <- 0 until l)
            v.add (pa.u (ctx))
          v
        }}}}

  def share [A] (pa: Pickler [A]): Pickler [A] = {
    require (pa != null)

    new Pickler [A] {

      def p (v: A, ctx: PickleContext) {
        if (ctx contains v) {
          ctx.writeVarUInt (0)
          ctx.writeVarUInt (ctx get v)
        } else {
          ctx.writeVarUInt (1)
          pa.p (v, ctx)
          ctx.put (v)
        }}

      def u (ctx: UnpickleContext) = {
        val x = ctx.readVarUInt()
        if (x == 0) {
          ctx.get [A] (ctx.readVarUInt())
        } else {
          val v = pa.u (ctx)
          ctx.put (v)
          v
        }}
      override def toString = "share (" + pa + ")"
    }}

  def tuple [A, B] (pa: Pickler [A], pb: Pickler [B]): Pickler [(A, B)] = {

    require (pa != null)
    require (pb != null)

    new Pickler [(A, B)] {

      def p (v: (A, B), ctx: PickleContext) {
        pa.p (v._1, ctx)
        pb.p (v._2, ctx)
      }

      def u (ctx: UnpickleContext) = {
        (pa.u (ctx), pb.u (ctx))
      }

      override def toString =
        "tuple " + (pa, pb)
    }}

  def tuple [A, B, C] (
      pa: Pickler [A],
      pb: Pickler [B],
      pc: Pickler [C]):
      Pickler [(A, B, C)] = {

    require (pa != null)
    require (pb != null)
    require (pc != null)

    new Pickler [(A, B, C)] {

      def p (v: (A, B, C), ctx: PickleContext) {
        pa.p (v._1, ctx)
        pb.p (v._2, ctx)
        pc.p (v._3, ctx)
      }

      def u (ctx: UnpickleContext) = {
        (pa.u (ctx),
            pb.u (ctx),
            pc.u (ctx))
      }

      override def toString =
        "tuple " + (pa, pb, pc)
    }}

  def tuple [A, B, C, D] (
      pa: Pickler [A],
      pb: Pickler [B],
      pc: Pickler [C],
      pd: Pickler [D]):
      Pickler [(A, B, C, D)] = {

    require (pa != null)
    require (pb != null)
    require (pc != null)
    require (pd != null)

    new Pickler [(A, B, C, D)] {

      def p (v: (A, B, C, D), ctx: PickleContext) {
        pa.p (v._1, ctx)
        pb.p (v._2, ctx)
        pc.p (v._3, ctx)
        pd.p (v._4, ctx)
      }

      def u (ctx: UnpickleContext) = {
        (pa.u (ctx),
            pb.u (ctx),
            pc.u (ctx),
            pd.u (ctx))
      }

      override def toString =
        "tuple " + (pa, pb, pc, pd)
    }}

  def tuple [A, B, C, D, E] (
      pa: Pickler [A],
      pb: Pickler [B],
      pc: Pickler [C],
      pd: Pickler [D],
      pe: Pickler [E]):
      Pickler [(A, B, C, D, E)] = {

    require (pa != null)
    require (pb != null)
    require (pc != null)
    require (pd != null)
    require (pe != null)

    new Pickler [(A, B, C, D, E)] {

      def p (v: (A, B, C, D, E), ctx: PickleContext) {
        pa.p (v._1, ctx)
        pb.p (v._2, ctx)
        pc.p (v._3, ctx)
        pd.p (v._4, ctx)
        pe.p (v._5, ctx)
      }

      def u (ctx: UnpickleContext) = {
        (pa.u (ctx),
            pb.u (ctx),
            pc.u (ctx),
            pd.u (ctx),
            pe.u (ctx))
      }

      override def toString =
        "tuple " + (pa, pb, pc, pd, pe)
    }}

  def tuple [A, B, C, D, E, F] (
      pa: Pickler [A],
      pb: Pickler [B],
      pc: Pickler [C],
      pd: Pickler [D],
      pe: Pickler [E],
      pf: Pickler [F]):
      Pickler [(A, B, C, D, E, F)] = {

    require (pa != null)
    require (pb != null)
    require (pc != null)
    require (pd != null)
    require (pe != null)
    require (pf != null)

    new Pickler [(A, B, C, D, E, F)] {

      def p (v: (A, B, C, D, E, F), ctx: PickleContext) {
        pa.p (v._1, ctx)
        pb.p (v._2, ctx)
        pc.p (v._3, ctx)
        pd.p (v._4, ctx)
        pe.p (v._5, ctx)
        pf.p (v._6, ctx)
      }

      def u (ctx: UnpickleContext) = {
        (pa.u (ctx),
            pb.u (ctx),
            pc.u (ctx),
            pd.u (ctx),
            pe.u (ctx),
            pf.u (ctx))
      }

      override def toString =
        "tuple " + (pa, pb, pc, pd, pe, pf)
    }}

  def tuple [A, B, C, D, E, F, G] (
      pa: Pickler [A],
      pb: Pickler [B],
      pc: Pickler [C],
      pd: Pickler [D],
      pe: Pickler [E],
      pf: Pickler [F],
      pg: Pickler [G]):
      Pickler [(A, B, C, D, E, F, G)] = {

    require (pa != null)
    require (pb != null)
    require (pc != null)
    require (pd != null)
    require (pe != null)
    require (pf != null)
    require (pg != null)

    new Pickler [(A, B, C, D, E, F, G)] {

      def p (v: (A, B, C, D, E, F, G), ctx: PickleContext) {
        pa.p (v._1, ctx)
        pb.p (v._2, ctx)
        pc.p (v._3, ctx)
        pd.p (v._4, ctx)
        pe.p (v._5, ctx)
        pf.p (v._6, ctx)
        pg.p (v._7, ctx)
      }

      def u (ctx: UnpickleContext) = {
        (pa.u (ctx),
            pb.u (ctx),
            pc.u (ctx),
            pd.u (ctx),
            pe.u (ctx),
            pf.u (ctx),
            pg.u (ctx))
      }

      override def toString =
        "tuple " + (pa, pb, pc, pd, pe, pf, pg)
    }}

  def tuple [A, B, C, D, E, F, G, H] (
      pa: Pickler [A],
      pb: Pickler [B],
      pc: Pickler [C],
      pd: Pickler [D],
      pe: Pickler [E],
      pf: Pickler [F],
      pg: Pickler [G],
      ph: Pickler [H]):
      Pickler [(A, B, C, D, E, F, G, H)] = {

    require (pa != null)
    require (pb != null)
    require (pc != null)
    require (pd != null)
    require (pe != null)
    require (pf != null)
    require (pg != null)
    require (ph != null)

    new Pickler [(A, B, C, D, E, F, G, H)] {

      def p (v: (A, B, C, D, E, F, G, H), ctx: PickleContext) {
        pa.p (v._1, ctx)
        pb.p (v._2, ctx)
        pc.p (v._3, ctx)
        pd.p (v._4, ctx)
        pe.p (v._5, ctx)
        pf.p (v._6, ctx)
        pg.p (v._7, ctx)
        ph.p (v._8, ctx)
      }

      def u (ctx: UnpickleContext) = {
        (pa.u (ctx),
            pb.u (ctx),
            pc.u (ctx),
            pd.u (ctx),
            pe.u (ctx),
            pf.u (ctx),
            pg.u (ctx),
            ph.u (ctx))
      }

      override def toString =
        "tuple " + (pa, pb, pc, pd, pe, pf, pg, ph)
    }}

  def tuple [A, B, C, D, E, F, G, H, I] (
      pa: Pickler [A],
      pb: Pickler [B],
      pc: Pickler [C],
      pd: Pickler [D],
      pe: Pickler [E],
      pf: Pickler [F],
      pg: Pickler [G],
      ph: Pickler [H],
      pi: Pickler [I]):
      Pickler [(A, B, C, D, E, F, G, H, I)] = {

    require (pa != null)
    require (pb != null)
    require (pc != null)
    require (pd != null)
    require (pe != null)
    require (pf != null)
    require (pg != null)
    require (ph != null)
    require (pi != null)

    new Pickler [(A, B, C, D, E, F, G, H, I)] {

      def p (v: (A, B, C, D, E, F, G, H, I), ctx: PickleContext) {
        pa.p (v._1, ctx)
        pb.p (v._2, ctx)
        pc.p (v._3, ctx)
        pd.p (v._4, ctx)
        pe.p (v._5, ctx)
        pf.p (v._6, ctx)
        pg.p (v._7, ctx)
        ph.p (v._8, ctx)
        pi.p (v._9, ctx)
      }

      def u (ctx: UnpickleContext) = {
        (pa.u (ctx),
            pb.u (ctx),
            pc.u (ctx),
            pd.u (ctx),
            pe.u (ctx),
            pf.u (ctx),
            pg.u (ctx),
            ph.u (ctx),
            pi.u (ctx))
      }

      override def toString =
        "tuple " + (pa, pb, pc, pd, pe, pf, pg, ph, pi)
    }}

  def tuple [A, B, C, D, E, F, G, H, I, J] (
      pa: Pickler [A],
      pb: Pickler [B],
      pc: Pickler [C],
      pd: Pickler [D],
      pe: Pickler [E],
      pf: Pickler [F],
      pg: Pickler [G],
      ph: Pickler [H],
      pi: Pickler [I],
      pj: Pickler [J]):
      Pickler [(A, B, C, D, E, F, G, H, I, J)] = {

    require (pa != null)
    require (pb != null)
    require (pc != null)
    require (pd != null)
    require (pe != null)
    require (pf != null)
    require (pg != null)
    require (ph != null)
    require (pi != null)
    require (pj != null)

    new Pickler [(A, B, C, D, E, F, G, H, I, J)] {

      def p (v: (A, B, C, D, E, F, G, H, I, J), ctx: PickleContext) {
        pa.p (v._1, ctx)
        pb.p (v._2, ctx)
        pc.p (v._3, ctx)
        pd.p (v._4, ctx)
        pe.p (v._5, ctx)
        pf.p (v._6, ctx)
        pg.p (v._7, ctx)
        ph.p (v._8, ctx)
        pi.p (v._9, ctx)
        pj.p (v._10, ctx)
      }

      def u (ctx: UnpickleContext) = {
        (pa.u (ctx),
            pb.u (ctx),
            pc.u (ctx),
            pd.u (ctx),
            pe.u (ctx),
            pf.u (ctx),
            pg.u (ctx),
            ph.u (ctx),
            pi.u (ctx),
            pj.u (ctx))
      }

      override def toString =
        "tuple " + (pa, pb, pc, pd, pe, pf, pg, ph, pi, pj)
    }}

  def tuple [A, B, C, D, E, F, G, H, I, J, K] (
      pa: Pickler [A],
      pb: Pickler [B],
      pc: Pickler [C],
      pd: Pickler [D],
      pe: Pickler [E],
      pf: Pickler [F],
      pg: Pickler [G],
      ph: Pickler [H],
      pi: Pickler [I],
      pj: Pickler [J],
      pk: Pickler [K]):
      Pickler [(A, B, C, D, E, F, G, H, I, J, K)] = {

    require (pa != null)
    require (pb != null)
    require (pc != null)
    require (pd != null)
    require (pe != null)
    require (pf != null)
    require (pg != null)
    require (ph != null)
    require (pi != null)
    require (pj != null)
    require (pk != null)

    new Pickler [(A, B, C, D, E, F, G, H, I, J, K)] {

      def p (v: (A, B, C, D, E, F, G, H, I, J, K), ctx: PickleContext) {
        pa.p (v._1, ctx)
        pb.p (v._2, ctx)
        pc.p (v._3, ctx)
        pd.p (v._4, ctx)
        pe.p (v._5, ctx)
        pf.p (v._6, ctx)
        pg.p (v._7, ctx)
        ph.p (v._8, ctx)
        pi.p (v._9, ctx)
        pj.p (v._10, ctx)
        pk.p (v._11, ctx)
      }

      def u (ctx: UnpickleContext) = {
        (pa.u (ctx),
            pb.u (ctx),
            pc.u (ctx),
            pd.u (ctx),
            pe.u (ctx),
            pf.u (ctx),
            pg.u (ctx),
            ph.u (ctx),
            pi.u (ctx),
            pj.u (ctx),
            pk.u (ctx))
      }

      override def toString =
        "tuple " + (pa, pb, pc, pd, pe, pf, pg, ph, pi, pj, pk)
    }}

  def tuple [A, B, C, D, E, F, G, H, I, J, K, L] (
      pa: Pickler [A],
      pb: Pickler [B],
      pc: Pickler [C],
      pd: Pickler [D],
      pe: Pickler [E],
      pf: Pickler [F],
      pg: Pickler [G],
      ph: Pickler [H],
      pi: Pickler [I],
      pj: Pickler [J],
      pk: Pickler [K],
      pl: Pickler [L]):
      Pickler [(A, B, C, D, E, F, G, H, I, J, K, L)] = {

    require (pa != null)
    require (pb != null)
    require (pc != null)
    require (pd != null)
    require (pe != null)
    require (pf != null)
    require (pg != null)
    require (ph != null)
    require (pi != null)
    require (pj != null)
    require (pk != null)
    require (pl != null)

    new Pickler [(A, B, C, D, E, F, G, H, I, J, K, L)] {

      def p (v: (A, B, C, D, E, F, G, H, I, J, K, L), ctx: PickleContext) {
        pa.p (v._1, ctx)
        pb.p (v._2, ctx)
        pc.p (v._3, ctx)
        pd.p (v._4, ctx)
        pe.p (v._5, ctx)
        pf.p (v._6, ctx)
        pg.p (v._7, ctx)
        ph.p (v._8, ctx)
        pi.p (v._9, ctx)
        pj.p (v._10, ctx)
        pk.p (v._11, ctx)
        pl.p (v._12, ctx)
      }

      def u (ctx: UnpickleContext) = {
        (pa.u (ctx),
            pb.u (ctx),
            pc.u (ctx),
            pd.u (ctx),
            pe.u (ctx),
            pf.u (ctx),
            pg.u (ctx),
            ph.u (ctx),
            pi.u (ctx),
            pj.u (ctx),
            pk.u (ctx),
            pl.u (ctx))
      }

      override def toString =
        "tuple " + (pa, pb, pc, pd, pe, pf, pg, ph, pi, pj, pk, pl)
    }}

  def tuple [A, B, C, D, E, F, G, H, I, J, K, L, M] (
      pa: Pickler [A],
      pb: Pickler [B],
      pc: Pickler [C],
      pd: Pickler [D],
      pe: Pickler [E],
      pf: Pickler [F],
      pg: Pickler [G],
      ph: Pickler [H],
      pi: Pickler [I],
      pj: Pickler [J],
      pk: Pickler [K],
      pl: Pickler [L],
      pm: Pickler [M]):
      Pickler [(A, B, C, D, E, F, G, H, I, J, K, L, M)] = {

    require (pa != null)
    require (pb != null)
    require (pc != null)
    require (pd != null)
    require (pe != null)
    require (pf != null)
    require (pg != null)
    require (ph != null)
    require (pi != null)
    require (pj != null)
    require (pk != null)
    require (pl != null)
    require (pm != null)

    new Pickler [(A, B, C, D, E, F, G, H, I, J, K, L, M)] {

      def p (v: (A, B, C, D, E, F, G, H, I, J, K, L, M), ctx: PickleContext) {
        pa.p (v._1, ctx)
        pb.p (v._2, ctx)
        pc.p (v._3, ctx)
        pd.p (v._4, ctx)
        pe.p (v._5, ctx)
        pf.p (v._6, ctx)
        pg.p (v._7, ctx)
        ph.p (v._8, ctx)
        pi.p (v._9, ctx)
        pj.p (v._10, ctx)
        pk.p (v._11, ctx)
        pl.p (v._12, ctx)
        pm.p (v._13, ctx)
      }

      def u (ctx: UnpickleContext) = {
        (pa.u (ctx),
            pb.u (ctx),
            pc.u (ctx),
            pd.u (ctx),
            pe.u (ctx),
            pf.u (ctx),
            pg.u (ctx),
            ph.u (ctx),
            pi.u (ctx),
            pj.u (ctx),
            pk.u (ctx),
            pl.u (ctx),
            pm.u (ctx))
      }

      override def toString =
        "tuple " + (pa, pb, pc, pd, pe, pf, pg, ph, pi, pj, pk, pl, pm)
    }}

  def tuple [A, B, C, D, E, F, G, H, I, J, K, L, M, N] (
      pa: Pickler [A],
      pb: Pickler [B],
      pc: Pickler [C],
      pd: Pickler [D],
      pe: Pickler [E],
      pf: Pickler [F],
      pg: Pickler [G],
      ph: Pickler [H],
      pi: Pickler [I],
      pj: Pickler [J],
      pk: Pickler [K],
      pl: Pickler [L],
      pm: Pickler [M],
      pn: Pickler [N]):
      Pickler [(A, B, C, D, E, F, G, H, I, J, K, L, M, N)] = {

    require (pa != null)
    require (pb != null)
    require (pc != null)
    require (pd != null)
    require (pe != null)
    require (pf != null)
    require (pg != null)
    require (ph != null)
    require (pi != null)
    require (pj != null)
    require (pk != null)
    require (pl != null)
    require (pm != null)
    require (pn != null)

    new Pickler [(A, B, C, D, E, F, G, H, I, J, K, L, M, N)] {

      def p (v: (A, B, C, D, E, F, G, H, I, J, K, L, M, N), ctx: PickleContext) {
        pa.p (v._1, ctx)
        pb.p (v._2, ctx)
        pc.p (v._3, ctx)
        pd.p (v._4, ctx)
        pe.p (v._5, ctx)
        pf.p (v._6, ctx)
        pg.p (v._7, ctx)
        ph.p (v._8, ctx)
        pi.p (v._9, ctx)
        pj.p (v._10, ctx)
        pk.p (v._11, ctx)
        pl.p (v._12, ctx)
        pm.p (v._13, ctx)
        pn.p (v._14, ctx)
      }

      def u (ctx: UnpickleContext) = {
        (pa.u (ctx),
            pb.u (ctx),
            pc.u (ctx),
            pd.u (ctx),
            pe.u (ctx),
            pf.u (ctx),
            pg.u (ctx),
            ph.u (ctx),
            pi.u (ctx),
            pj.u (ctx),
            pk.u (ctx),
            pl.u (ctx),
            pm.u (ctx),
            pn.u (ctx))
      }

      override def toString =
        "tuple " + (pa, pb, pc, pd, pe, pf, pg, ph, pi, pj, pk, pl, pm, pn)
    }}

  def tuple [A, B, C, D, E, F, G, H, I, J, K, L, M, N, O] (
      pa: Pickler [A],
      pb: Pickler [B],
      pc: Pickler [C],
      pd: Pickler [D],
      pe: Pickler [E],
      pf: Pickler [F],
      pg: Pickler [G],
      ph: Pickler [H],
      pi: Pickler [I],
      pj: Pickler [J],
      pk: Pickler [K],
      pl: Pickler [L],
      pm: Pickler [M],
      pn: Pickler [N],
      po: Pickler [O]):
      Pickler [(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O)] = {

    require (pa != null)
    require (pb != null)
    require (pc != null)
    require (pd != null)
    require (pe != null)
    require (pf != null)
    require (pg != null)
    require (ph != null)
    require (pi != null)
    require (pj != null)
    require (pk != null)
    require (pl != null)
    require (pm != null)
    require (pn != null)
    require (po != null)

    new Pickler [(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O)] {

      def p (v: (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O), ctx: PickleContext) {
        pa.p (v._1, ctx)
        pb.p (v._2, ctx)
        pc.p (v._3, ctx)
        pd.p (v._4, ctx)
        pe.p (v._5, ctx)
        pf.p (v._6, ctx)
        pg.p (v._7, ctx)
        ph.p (v._8, ctx)
        pi.p (v._9, ctx)
        pj.p (v._10, ctx)
        pk.p (v._11, ctx)
        pl.p (v._12, ctx)
        pm.p (v._13, ctx)
        pn.p (v._14, ctx)
        po.p (v._15, ctx)
      }

      def u (ctx: UnpickleContext) = {
        (pa.u (ctx),
            pb.u (ctx),
            pc.u (ctx),
            pd.u (ctx),
            pe.u (ctx),
            pf.u (ctx),
            pg.u (ctx),
            ph.u (ctx),
            pi.u (ctx),
            pj.u (ctx),
            pk.u (ctx),
            pl.u (ctx),
            pm.u (ctx),
            pn.u (ctx),
            po.u (ctx))
      }

      override def toString =
        "tuple " + (pa, pb, pc, pd, pe, pf, pg, ph, pi, pj, pk, pl, pm, pn, po)
    }}

  def tuple [A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P] (
      pa: Pickler [A],
      pb: Pickler [B],
      pc: Pickler [C],
      pd: Pickler [D],
      pe: Pickler [E],
      pf: Pickler [F],
      pg: Pickler [G],
      ph: Pickler [H],
      pi: Pickler [I],
      pj: Pickler [J],
      pk: Pickler [K],
      pl: Pickler [L],
      pm: Pickler [M],
      pn: Pickler [N],
      po: Pickler [O],
      pp: Pickler [P]):
      Pickler [(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P)] = {

    require (pa != null)
    require (pb != null)
    require (pc != null)
    require (pd != null)
    require (pe != null)
    require (pf != null)
    require (pg != null)
    require (ph != null)
    require (pi != null)
    require (pj != null)
    require (pk != null)
    require (pl != null)
    require (pm != null)
    require (pn != null)
    require (po != null)
    require (pp != null)

    new Pickler [(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P)] {

      def p (v: (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P), ctx: PickleContext) {
        pa.p (v._1, ctx)
        pb.p (v._2, ctx)
        pc.p (v._3, ctx)
        pd.p (v._4, ctx)
        pe.p (v._5, ctx)
        pf.p (v._6, ctx)
        pg.p (v._7, ctx)
        ph.p (v._8, ctx)
        pi.p (v._9, ctx)
        pj.p (v._10, ctx)
        pk.p (v._11, ctx)
        pl.p (v._12, ctx)
        pm.p (v._13, ctx)
        pn.p (v._14, ctx)
        po.p (v._15, ctx)
        pp.p (v._16, ctx)
      }

      def u (ctx: UnpickleContext) = {
        (pa.u (ctx),
            pb.u (ctx),
            pc.u (ctx),
            pd.u (ctx),
            pe.u (ctx),
            pf.u (ctx),
            pg.u (ctx),
            ph.u (ctx),
            pi.u (ctx),
            pj.u (ctx),
            pk.u (ctx),
            pl.u (ctx),
            pm.u (ctx),
            pn.u (ctx),
            po.u (ctx),
            pp.u (ctx))
      }

      override def toString =
        "tuple " + (pa, pb, pc, pd, pe, pf, pg, ph, pi, pj, pk, pl, pm, pn, po, pp)
    }}

  def tuple [A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q] (
      pa: Pickler [A],
      pb: Pickler [B],
      pc: Pickler [C],
      pd: Pickler [D],
      pe: Pickler [E],
      pf: Pickler [F],
      pg: Pickler [G],
      ph: Pickler [H],
      pi: Pickler [I],
      pj: Pickler [J],
      pk: Pickler [K],
      pl: Pickler [L],
      pm: Pickler [M],
      pn: Pickler [N],
      po: Pickler [O],
      pp: Pickler [P],
      pq: Pickler [Q]):
      Pickler [(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q)] = {

    require (pa != null)
    require (pb != null)
    require (pc != null)
    require (pd != null)
    require (pe != null)
    require (pf != null)
    require (pg != null)
    require (ph != null)
    require (pi != null)
    require (pj != null)
    require (pk != null)
    require (pl != null)
    require (pm != null)
    require (pn != null)
    require (po != null)
    require (pp != null)
    require (pq != null)

    new Pickler [(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q)] {

      def p (v: (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q), ctx: PickleContext) {
        pa.p (v._1, ctx)
        pb.p (v._2, ctx)
        pc.p (v._3, ctx)
        pd.p (v._4, ctx)
        pe.p (v._5, ctx)
        pf.p (v._6, ctx)
        pg.p (v._7, ctx)
        ph.p (v._8, ctx)
        pi.p (v._9, ctx)
        pj.p (v._10, ctx)
        pk.p (v._11, ctx)
        pl.p (v._12, ctx)
        pm.p (v._13, ctx)
        pn.p (v._14, ctx)
        po.p (v._15, ctx)
        pp.p (v._16, ctx)
        pq.p (v._17, ctx)
      }

      def u (ctx: UnpickleContext) = {
        (pa.u (ctx),
            pb.u (ctx),
            pc.u (ctx),
            pd.u (ctx),
            pe.u (ctx),
            pf.u (ctx),
            pg.u (ctx),
            ph.u (ctx),
            pi.u (ctx),
            pj.u (ctx),
            pk.u (ctx),
            pl.u (ctx),
            pm.u (ctx),
            pn.u (ctx),
            po.u (ctx),
            pp.u (ctx),
            pq.u (ctx))
      }

      override def toString =
        "tuple " + (pa, pb, pc, pd, pe, pf, pg, ph, pi, pj, pk, pl, pm, pn, po, pp, pq)
    }}

  def tuple [A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R] (
      pa: Pickler [A],
      pb: Pickler [B],
      pc: Pickler [C],
      pd: Pickler [D],
      pe: Pickler [E],
      pf: Pickler [F],
      pg: Pickler [G],
      ph: Pickler [H],
      pi: Pickler [I],
      pj: Pickler [J],
      pk: Pickler [K],
      pl: Pickler [L],
      pm: Pickler [M],
      pn: Pickler [N],
      po: Pickler [O],
      pp: Pickler [P],
      pq: Pickler [Q],
      pr: Pickler [R]):
      Pickler [(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R)] = {

    require (pa != null)
    require (pb != null)
    require (pc != null)
    require (pd != null)
    require (pe != null)
    require (pf != null)
    require (pg != null)
    require (ph != null)
    require (pi != null)
    require (pj != null)
    require (pk != null)
    require (pl != null)
    require (pm != null)
    require (pn != null)
    require (po != null)
    require (pp != null)
    require (pq != null)
    require (pr != null)

    new Pickler [(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R)] {

      def p (v: (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R), ctx: PickleContext) {
        pa.p (v._1, ctx)
        pb.p (v._2, ctx)
        pc.p (v._3, ctx)
        pd.p (v._4, ctx)
        pe.p (v._5, ctx)
        pf.p (v._6, ctx)
        pg.p (v._7, ctx)
        ph.p (v._8, ctx)
        pi.p (v._9, ctx)
        pj.p (v._10, ctx)
        pk.p (v._11, ctx)
        pl.p (v._12, ctx)
        pm.p (v._13, ctx)
        pn.p (v._14, ctx)
        po.p (v._15, ctx)
        pp.p (v._16, ctx)
        pq.p (v._17, ctx)
        pr.p (v._18, ctx)
      }

      def u (ctx: UnpickleContext) = {
        (pa.u (ctx),
            pb.u (ctx),
            pc.u (ctx),
            pd.u (ctx),
            pe.u (ctx),
            pf.u (ctx),
            pg.u (ctx),
            ph.u (ctx),
            pi.u (ctx),
            pj.u (ctx),
            pk.u (ctx),
            pl.u (ctx),
            pm.u (ctx),
            pn.u (ctx),
            po.u (ctx),
            pp.u (ctx),
            pq.u (ctx),
            pr.u (ctx))
      }

      override def toString =
        "tuple " + (pa, pb, pc, pd, pe, pf, pg, ph, pi, pj, pk, pl, pm, pn, po, pp, pq, pr)
    }}

  def wrap [A, V] (pickler: Pickler [A]) (build: A => V) (inspect: V => A) (implicit ct: ClassTag [V]):
      Pickler [V] = {

    require (pickler != null)

    new Pickler [V] {

      def p (v: V, ctx: PickleContext) {
        pickler.p (inspect (v), ctx)
      }

      def u (ctx: UnpickleContext) = {
        build (pickler.u (ctx))
      }

      override def toString = ct.runtimeClass.getSimpleName
    }}

  def wrap [A, B, V] (
      pa: Pickler [A],
      pb: Pickler [B])
      (build: (A, B) => V)
      (inspect: V => (A, B))
      (implicit ct: ClassTag [V]):
      Pickler [V] = {

    require (pa != null)
    require (pb != null)

    new Pickler [V] {

      def p (v: V, ctx: PickleContext) {
        val vt = inspect (v)
        pa.p (vt._1, ctx)
        pb.p (vt._2, ctx)
      }

      def u (ctx: UnpickleContext) = {
        build (pa.u (ctx),
            pb.u (ctx))
      }

      override def toString = ct.runtimeClass.getSimpleName
    }}

  def wrap [A, B, C, V] (
      pa: Pickler [A],
      pb: Pickler [B],
      pc: Pickler [C])
      (build: (A, B, C) => V)
      (inspect: V => (A, B, C))
      (implicit ct: ClassTag [V]):
      Pickler [V] = {

    require (pa != null)
    require (pb != null)
    require (pc != null)

    new Pickler [V] {

      def p (v: V, ctx: PickleContext) {
        val vt = inspect (v)
        pa.p (vt._1, ctx)
        pb.p (vt._2, ctx)
        pc.p (vt._3, ctx)
      }

      def u (ctx: UnpickleContext) = {
        build (pa.u (ctx),
            pb.u (ctx),
            pc.u (ctx))
      }

      override def toString = ct.runtimeClass.getSimpleName
    }}

  def wrap [A, B, C, D, V] (
      pa: Pickler [A],
      pb: Pickler [B],
      pc: Pickler [C],
      pd: Pickler [D])
      (build: (A, B, C, D) => V)
      (inspect: V => (A, B, C, D))
      (implicit ct: ClassTag [V]):
      Pickler [V] = {

    require (pa != null)
    require (pb != null)
    require (pc != null)
    require (pd != null)

    new Pickler [V] {

      def p (v: V, ctx: PickleContext) {
        val vt = inspect (v)
        pa.p (vt._1, ctx)
        pb.p (vt._2, ctx)
        pc.p (vt._3, ctx)
        pd.p (vt._4, ctx)
      }

      def u (ctx: UnpickleContext) = {
        build (pa.u (ctx),
            pb.u (ctx),
            pc.u (ctx),
            pd.u (ctx))
      }

      override def toString = ct.runtimeClass.getSimpleName
    }}

  def wrap [A, B, C, D, E, V] (
      pa: Pickler [A],
      pb: Pickler [B],
      pc: Pickler [C],
      pd: Pickler [D],
      pe: Pickler [E])
      (build: (A, B, C, D, E) => V)
      (inspect: V => (A, B, C, D, E))
      (implicit ct: ClassTag [V]):
      Pickler [V] = {

    require (pa != null)
    require (pb != null)
    require (pc != null)
    require (pd != null)
    require (pe != null)

    new Pickler [V] {

      def p (v: V, ctx: PickleContext) {
        val vt = inspect (v)
        pa.p (vt._1, ctx)
        pb.p (vt._2, ctx)
        pc.p (vt._3, ctx)
        pd.p (vt._4, ctx)
        pe.p (vt._5, ctx)
      }

      def u (ctx: UnpickleContext) = {
        build (pa.u (ctx),
            pb.u (ctx),
            pc.u (ctx),
            pd.u (ctx),
            pe.u (ctx))
      }

      override def toString = ct.runtimeClass.getSimpleName
    }}

  def wrap [A, B, C, D, E, F, V] (
      pa: Pickler [A],
      pb: Pickler [B],
      pc: Pickler [C],
      pd: Pickler [D],
      pe: Pickler [E],
      pf: Pickler [F])
      (build: (A, B, C, D, E, F) => V)
      (inspect: V => (A, B, C, D, E, F))
      (implicit ct: ClassTag [V]):
      Pickler [V] = {

    require (pa != null)
    require (pb != null)
    require (pc != null)
    require (pd != null)
    require (pe != null)
    require (pf != null)

    new Pickler [V] {

      def p (v: V, ctx: PickleContext) {
        val vt = inspect (v)
        pa.p (vt._1, ctx)
        pb.p (vt._2, ctx)
        pc.p (vt._3, ctx)
        pd.p (vt._4, ctx)
        pe.p (vt._5, ctx)
        pf.p (vt._6, ctx)
      }

      def u (ctx: UnpickleContext) = {
        build (pa.u (ctx),
            pb.u (ctx),
            pc.u (ctx),
            pd.u (ctx),
            pe.u (ctx),
            pf.u (ctx))
      }

      override def toString = ct.runtimeClass.getSimpleName
    }}

  def wrap [A, B, C, D, E, F, G, V] (
      pa: Pickler [A],
      pb: Pickler [B],
      pc: Pickler [C],
      pd: Pickler [D],
      pe: Pickler [E],
      pf: Pickler [F],
      pg: Pickler [G])
      (build: (A, B, C, D, E, F, G) => V)
      (inspect: V => (A, B, C, D, E, F, G))
      (implicit ct: ClassTag [V]):
      Pickler [V] = {

    require (pa != null)
    require (pb != null)
    require (pc != null)
    require (pd != null)
    require (pe != null)
    require (pf != null)
    require (pg != null)

    new Pickler [V] {

      def p (v: V, ctx: PickleContext) {
        val vt = inspect (v)
        pa.p (vt._1, ctx)
        pb.p (vt._2, ctx)
        pc.p (vt._3, ctx)
        pd.p (vt._4, ctx)
        pe.p (vt._5, ctx)
        pf.p (vt._6, ctx)
        pg.p (vt._7, ctx)
      }

      def u (ctx: UnpickleContext) = {
        build (pa.u (ctx),
            pb.u (ctx),
            pc.u (ctx),
            pd.u (ctx),
            pe.u (ctx),
            pf.u (ctx),
            pg.u (ctx))
      }

      override def toString = ct.runtimeClass.getSimpleName
    }}

  def wrap [A, B, C, D, E, F, G, H, V] (
      pa: Pickler [A],
      pb: Pickler [B],
      pc: Pickler [C],
      pd: Pickler [D],
      pe: Pickler [E],
      pf: Pickler [F],
      pg: Pickler [G],
      ph: Pickler [H])
      (build: (A, B, C, D, E, F, G, H) => V)
      (inspect: V => (A, B, C, D, E, F, G, H))
      (implicit ct: ClassTag [V]):
      Pickler [V] = {

    require (pa != null)
    require (pb != null)
    require (pc != null)
    require (pd != null)
    require (pe != null)
    require (pf != null)
    require (pg != null)
    require (ph != null)

    new Pickler [V] {

      def p (v: V, ctx: PickleContext) {
        val vt = inspect (v)
        pa.p (vt._1, ctx)
        pb.p (vt._2, ctx)
        pc.p (vt._3, ctx)
        pd.p (vt._4, ctx)
        pe.p (vt._5, ctx)
        pf.p (vt._6, ctx)
        pg.p (vt._7, ctx)
        ph.p (vt._8, ctx)
      }

      def u (ctx: UnpickleContext) = {
        build (pa.u (ctx),
            pb.u (ctx),
            pc.u (ctx),
            pd.u (ctx),
            pe.u (ctx),
            pf.u (ctx),
            pg.u (ctx),
            ph.u (ctx))
      }

      override def toString = ct.runtimeClass.getSimpleName
    }}

  def wrap [A, B, C, D, E, F, G, H, I, V] (
      pa: Pickler [A],
      pb: Pickler [B],
      pc: Pickler [C],
      pd: Pickler [D],
      pe: Pickler [E],
      pf: Pickler [F],
      pg: Pickler [G],
      ph: Pickler [H],
      pi: Pickler [I])
      (build: (A, B, C, D, E, F, G, H, I) => V)
      (inspect: V => (A, B, C, D, E, F, G, H, I))
      (implicit ct: ClassTag [V]):
      Pickler [V] = {

    require (pa != null)
    require (pb != null)
    require (pc != null)
    require (pd != null)
    require (pe != null)
    require (pf != null)
    require (pg != null)
    require (ph != null)
    require (pi != null)

    new Pickler [V] {

      def p (v: V, ctx: PickleContext) {
        val vt = inspect (v)
        pa.p (vt._1, ctx)
        pb.p (vt._2, ctx)
        pc.p (vt._3, ctx)
        pd.p (vt._4, ctx)
        pe.p (vt._5, ctx)
        pf.p (vt._6, ctx)
        pg.p (vt._7, ctx)
        ph.p (vt._8, ctx)
        pi.p (vt._9, ctx)
      }

      def u (ctx: UnpickleContext) = {
        build (pa.u (ctx),
            pb.u (ctx),
            pc.u (ctx),
            pd.u (ctx),
            pe.u (ctx),
            pf.u (ctx),
            pg.u (ctx),
            ph.u (ctx),
            pi.u (ctx))
      }

      override def toString = ct.runtimeClass.getSimpleName
    }}

  def wrap [A, B, C, D, E, F, G, H, I, J, V] (
      pa: Pickler [A],
      pb: Pickler [B],
      pc: Pickler [C],
      pd: Pickler [D],
      pe: Pickler [E],
      pf: Pickler [F],
      pg: Pickler [G],
      ph: Pickler [H],
      pi: Pickler [I],
      pj: Pickler [J])
      (build: (A, B, C, D, E, F, G, H, I, J) => V)
      (inspect: V => (A, B, C, D, E, F, G, H, I, J))
      (implicit ct: ClassTag [V]):
      Pickler [V] = {

    require (pa != null)
    require (pb != null)
    require (pc != null)
    require (pd != null)
    require (pe != null)
    require (pf != null)
    require (pg != null)
    require (ph != null)
    require (pi != null)
    require (pj != null)

    new Pickler [V] {

      def p (v: V, ctx: PickleContext) {
        val vt = inspect (v)
        pa.p (vt._1, ctx)
        pb.p (vt._2, ctx)
        pc.p (vt._3, ctx)
        pd.p (vt._4, ctx)
        pe.p (vt._5, ctx)
        pf.p (vt._6, ctx)
        pg.p (vt._7, ctx)
        ph.p (vt._8, ctx)
        pi.p (vt._9, ctx)
        pj.p (vt._10, ctx)
      }

      def u (ctx: UnpickleContext) = {
        build (pa.u (ctx),
            pb.u (ctx),
            pc.u (ctx),
            pd.u (ctx),
            pe.u (ctx),
            pf.u (ctx),
            pg.u (ctx),
            ph.u (ctx),
            pi.u (ctx),
            pj.u (ctx))
      }

      override def toString = ct.runtimeClass.getSimpleName
    }}

  def wrap [A, B, C, D, E, F, G, H, I, J, K, V] (
      pa: Pickler [A],
      pb: Pickler [B],
      pc: Pickler [C],
      pd: Pickler [D],
      pe: Pickler [E],
      pf: Pickler [F],
      pg: Pickler [G],
      ph: Pickler [H],
      pi: Pickler [I],
      pj: Pickler [J],
      pk: Pickler [K])
      (build: (A, B, C, D, E, F, G, H, I, J, K) => V)
      (inspect: V => (A, B, C, D, E, F, G, H, I, J, K))
      (implicit ct: ClassTag [V]):
      Pickler [V] = {

    require (pa != null)
    require (pb != null)
    require (pc != null)
    require (pd != null)
    require (pe != null)
    require (pf != null)
    require (pg != null)
    require (ph != null)
    require (pi != null)
    require (pj != null)
    require (pk != null)

    new Pickler [V] {

      def p (v: V, ctx: PickleContext) {
        val vt = inspect (v)
        pa.p (vt._1, ctx)
        pb.p (vt._2, ctx)
        pc.p (vt._3, ctx)
        pd.p (vt._4, ctx)
        pe.p (vt._5, ctx)
        pf.p (vt._6, ctx)
        pg.p (vt._7, ctx)
        ph.p (vt._8, ctx)
        pi.p (vt._9, ctx)
        pj.p (vt._10, ctx)
        pk.p (vt._11, ctx)
      }

      def u (ctx: UnpickleContext) = {
        build (pa.u (ctx),
            pb.u (ctx),
            pc.u (ctx),
            pd.u (ctx),
            pe.u (ctx),
            pf.u (ctx),
            pg.u (ctx),
            ph.u (ctx),
            pi.u (ctx),
            pj.u (ctx),
            pk.u (ctx))
      }

      override def toString = ct.runtimeClass.getSimpleName
    }}

  def wrap [A, B, C, D, E, F, G, H, I, J, K, L, V] (
      pa: Pickler [A],
      pb: Pickler [B],
      pc: Pickler [C],
      pd: Pickler [D],
      pe: Pickler [E],
      pf: Pickler [F],
      pg: Pickler [G],
      ph: Pickler [H],
      pi: Pickler [I],
      pj: Pickler [J],
      pk: Pickler [K],
      pl: Pickler [L])
      (build: (A, B, C, D, E, F, G, H, I, J, K, L) => V)
      (inspect: V => (A, B, C, D, E, F, G, H, I, J, K, L))
      (implicit ct: ClassTag [V]):
      Pickler [V] = {

    require (pa != null)
    require (pb != null)
    require (pc != null)
    require (pd != null)
    require (pe != null)
    require (pf != null)
    require (pg != null)
    require (ph != null)
    require (pi != null)
    require (pj != null)
    require (pk != null)
    require (pl != null)

    new Pickler [V] {

      def p (v: V, ctx: PickleContext) {
        val vt = inspect (v)
        pa.p (vt._1, ctx)
        pb.p (vt._2, ctx)
        pc.p (vt._3, ctx)
        pd.p (vt._4, ctx)
        pe.p (vt._5, ctx)
        pf.p (vt._6, ctx)
        pg.p (vt._7, ctx)
        ph.p (vt._8, ctx)
        pi.p (vt._9, ctx)
        pj.p (vt._10, ctx)
        pk.p (vt._11, ctx)
        pl.p (vt._12, ctx)
      }

      def u (ctx: UnpickleContext) = {
        build (pa.u (ctx),
            pb.u (ctx),
            pc.u (ctx),
            pd.u (ctx),
            pe.u (ctx),
            pf.u (ctx),
            pg.u (ctx),
            ph.u (ctx),
            pi.u (ctx),
            pj.u (ctx),
            pk.u (ctx),
            pl.u (ctx))
      }

      override def toString = ct.runtimeClass.getSimpleName
    }}

  def wrap [A, B, C, D, E, F, G, H, I, J, K, L, M, V] (
      pa: Pickler [A],
      pb: Pickler [B],
      pc: Pickler [C],
      pd: Pickler [D],
      pe: Pickler [E],
      pf: Pickler [F],
      pg: Pickler [G],
      ph: Pickler [H],
      pi: Pickler [I],
      pj: Pickler [J],
      pk: Pickler [K],
      pl: Pickler [L],
      pm: Pickler [M])
      (build: (A, B, C, D, E, F, G, H, I, J, K, L, M) => V)
      (inspect: V => (A, B, C, D, E, F, G, H, I, J, K, L, M))
      (implicit ct: ClassTag [V]):
      Pickler [V] = {

    require (pa != null)
    require (pb != null)
    require (pc != null)
    require (pd != null)
    require (pe != null)
    require (pf != null)
    require (pg != null)
    require (ph != null)
    require (pi != null)
    require (pj != null)
    require (pk != null)
    require (pl != null)
    require (pm != null)

    new Pickler [V] {

      def p (v: V, ctx: PickleContext) {
        val vt = inspect (v)
        pa.p (vt._1, ctx)
        pb.p (vt._2, ctx)
        pc.p (vt._3, ctx)
        pd.p (vt._4, ctx)
        pe.p (vt._5, ctx)
        pf.p (vt._6, ctx)
        pg.p (vt._7, ctx)
        ph.p (vt._8, ctx)
        pi.p (vt._9, ctx)
        pj.p (vt._10, ctx)
        pk.p (vt._11, ctx)
        pl.p (vt._12, ctx)
        pm.p (vt._13, ctx)
      }

      def u (ctx: UnpickleContext) = {
        build (pa.u (ctx),
            pb.u (ctx),
            pc.u (ctx),
            pd.u (ctx),
            pe.u (ctx),
            pf.u (ctx),
            pg.u (ctx),
            ph.u (ctx),
            pi.u (ctx),
            pj.u (ctx),
            pk.u (ctx),
            pl.u (ctx),
            pm.u (ctx))
      }

      override def toString = ct.runtimeClass.getSimpleName
    }}

  def wrap [A, B, C, D, E, F, G, H, I, J, K, L, M, N, V] (
      pa: Pickler [A],
      pb: Pickler [B],
      pc: Pickler [C],
      pd: Pickler [D],
      pe: Pickler [E],
      pf: Pickler [F],
      pg: Pickler [G],
      ph: Pickler [H],
      pi: Pickler [I],
      pj: Pickler [J],
      pk: Pickler [K],
      pl: Pickler [L],
      pm: Pickler [M],
      pn: Pickler [N])
      (build: (A, B, C, D, E, F, G, H, I, J, K, L, M, N) => V)
      (inspect: V => (A, B, C, D, E, F, G, H, I, J, K, L, M, N))
      (implicit ct: ClassTag [V]):
      Pickler [V] = {

    require (pa != null)
    require (pb != null)
    require (pc != null)
    require (pd != null)
    require (pe != null)
    require (pf != null)
    require (pg != null)
    require (ph != null)
    require (pi != null)
    require (pj != null)
    require (pk != null)
    require (pl != null)
    require (pm != null)
    require (pn != null)

    new Pickler [V] {

      def p (v: V, ctx: PickleContext) {
        val vt = inspect (v)
        pa.p (vt._1, ctx)
        pb.p (vt._2, ctx)
        pc.p (vt._3, ctx)
        pd.p (vt._4, ctx)
        pe.p (vt._5, ctx)
        pf.p (vt._6, ctx)
        pg.p (vt._7, ctx)
        ph.p (vt._8, ctx)
        pi.p (vt._9, ctx)
        pj.p (vt._10, ctx)
        pk.p (vt._11, ctx)
        pl.p (vt._12, ctx)
        pm.p (vt._13, ctx)
        pn.p (vt._14, ctx)
      }

      def u (ctx: UnpickleContext) = {
        build (pa.u (ctx),
            pb.u (ctx),
            pc.u (ctx),
            pd.u (ctx),
            pe.u (ctx),
            pf.u (ctx),
            pg.u (ctx),
            ph.u (ctx),
            pi.u (ctx),
            pj.u (ctx),
            pk.u (ctx),
            pl.u (ctx),
            pm.u (ctx),
            pn.u (ctx))
      }

      override def toString = ct.runtimeClass.getSimpleName
    }}

  def wrap [A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, V] (
      pa: Pickler [A],
      pb: Pickler [B],
      pc: Pickler [C],
      pd: Pickler [D],
      pe: Pickler [E],
      pf: Pickler [F],
      pg: Pickler [G],
      ph: Pickler [H],
      pi: Pickler [I],
      pj: Pickler [J],
      pk: Pickler [K],
      pl: Pickler [L],
      pm: Pickler [M],
      pn: Pickler [N],
      po: Pickler [O])
      (build: (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O) => V)
      (inspect: V => (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O))
      (implicit ct: ClassTag [V]):
      Pickler [V] = {

    require (pa != null)
    require (pb != null)
    require (pc != null)
    require (pd != null)
    require (pe != null)
    require (pf != null)
    require (pg != null)
    require (ph != null)
    require (pi != null)
    require (pj != null)
    require (pk != null)
    require (pl != null)
    require (pm != null)
    require (pn != null)
    require (po != null)

    new Pickler [V] {

      def p (v: V, ctx: PickleContext) {
        val vt = inspect (v)
        pa.p (vt._1, ctx)
        pb.p (vt._2, ctx)
        pc.p (vt._3, ctx)
        pd.p (vt._4, ctx)
        pe.p (vt._5, ctx)
        pf.p (vt._6, ctx)
        pg.p (vt._7, ctx)
        ph.p (vt._8, ctx)
        pi.p (vt._9, ctx)
        pj.p (vt._10, ctx)
        pk.p (vt._11, ctx)
        pl.p (vt._12, ctx)
        pm.p (vt._13, ctx)
        pn.p (vt._14, ctx)
        po.p (vt._15, ctx)
      }

      def u (ctx: UnpickleContext) = {
        build (pa.u (ctx),
            pb.u (ctx),
            pc.u (ctx),
            pd.u (ctx),
            pe.u (ctx),
            pf.u (ctx),
            pg.u (ctx),
            ph.u (ctx),
            pi.u (ctx),
            pj.u (ctx),
            pk.u (ctx),
            pl.u (ctx),
            pm.u (ctx),
            pn.u (ctx),
            po.u (ctx))
      }

      override def toString = ct.runtimeClass.getSimpleName
    }}

  def wrap [A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, V] (
      pa: Pickler [A],
      pb: Pickler [B],
      pc: Pickler [C],
      pd: Pickler [D],
      pe: Pickler [E],
      pf: Pickler [F],
      pg: Pickler [G],
      ph: Pickler [H],
      pi: Pickler [I],
      pj: Pickler [J],
      pk: Pickler [K],
      pl: Pickler [L],
      pm: Pickler [M],
      pn: Pickler [N],
      po: Pickler [O],
      pp: Pickler [P])
      (build: (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P) => V)
      (inspect: V => (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P))
      (implicit ct: ClassTag [V]):
      Pickler [V] = {

    require (pa != null)
    require (pb != null)
    require (pc != null)
    require (pd != null)
    require (pe != null)
    require (pf != null)
    require (pg != null)
    require (ph != null)
    require (pi != null)
    require (pj != null)
    require (pk != null)
    require (pl != null)
    require (pm != null)
    require (pn != null)
    require (po != null)
    require (pp != null)

    new Pickler [V] {

      def p (v: V, ctx: PickleContext) {
        val vt = inspect (v)
        pa.p (vt._1, ctx)
        pb.p (vt._2, ctx)
        pc.p (vt._3, ctx)
        pd.p (vt._4, ctx)
        pe.p (vt._5, ctx)
        pf.p (vt._6, ctx)
        pg.p (vt._7, ctx)
        ph.p (vt._8, ctx)
        pi.p (vt._9, ctx)
        pj.p (vt._10, ctx)
        pk.p (vt._11, ctx)
        pl.p (vt._12, ctx)
        pm.p (vt._13, ctx)
        pn.p (vt._14, ctx)
        po.p (vt._15, ctx)
        pp.p (vt._16, ctx)
      }

      def u (ctx: UnpickleContext) = {
        build (pa.u (ctx),
            pb.u (ctx),
            pc.u (ctx),
            pd.u (ctx),
            pe.u (ctx),
            pf.u (ctx),
            pg.u (ctx),
            ph.u (ctx),
            pi.u (ctx),
            pj.u (ctx),
            pk.u (ctx),
            pl.u (ctx),
            pm.u (ctx),
            pn.u (ctx),
            po.u (ctx),
            pp.u (ctx))
      }

      override def toString = ct.runtimeClass.getSimpleName
    }}

  def wrap [A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, V] (
      pa: Pickler [A],
      pb: Pickler [B],
      pc: Pickler [C],
      pd: Pickler [D],
      pe: Pickler [E],
      pf: Pickler [F],
      pg: Pickler [G],
      ph: Pickler [H],
      pi: Pickler [I],
      pj: Pickler [J],
      pk: Pickler [K],
      pl: Pickler [L],
      pm: Pickler [M],
      pn: Pickler [N],
      po: Pickler [O],
      pp: Pickler [P],
      pq: Pickler [Q])
      (build: (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q) => V)
      (inspect: V => (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q))
      (implicit ct: ClassTag [V]):
      Pickler [V] = {

    require (pa != null)
    require (pb != null)
    require (pc != null)
    require (pd != null)
    require (pe != null)
    require (pf != null)
    require (pg != null)
    require (ph != null)
    require (pi != null)
    require (pj != null)
    require (pk != null)
    require (pl != null)
    require (pm != null)
    require (pn != null)
    require (po != null)
    require (pp != null)
    require (pq != null)

    new Pickler [V] {

      def p (v: V, ctx: PickleContext) {
        val vt = inspect (v)
        pa.p (vt._1, ctx)
        pb.p (vt._2, ctx)
        pc.p (vt._3, ctx)
        pd.p (vt._4, ctx)
        pe.p (vt._5, ctx)
        pf.p (vt._6, ctx)
        pg.p (vt._7, ctx)
        ph.p (vt._8, ctx)
        pi.p (vt._9, ctx)
        pj.p (vt._10, ctx)
        pk.p (vt._11, ctx)
        pl.p (vt._12, ctx)
        pm.p (vt._13, ctx)
        pn.p (vt._14, ctx)
        po.p (vt._15, ctx)
        pp.p (vt._16, ctx)
        pq.p (vt._17, ctx)
      }

      def u (ctx: UnpickleContext) = {
        build (pa.u (ctx),
            pb.u (ctx),
            pc.u (ctx),
            pd.u (ctx),
            pe.u (ctx),
            pf.u (ctx),
            pg.u (ctx),
            ph.u (ctx),
            pi.u (ctx),
            pj.u (ctx),
            pk.u (ctx),
            pl.u (ctx),
            pm.u (ctx),
            pn.u (ctx),
            po.u (ctx),
            pp.u (ctx),
            pq.u (ctx))
      }

      override def toString = ct.runtimeClass.getSimpleName
    }}

  def wrap [A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, V] (
      pa: Pickler [A],
      pb: Pickler [B],
      pc: Pickler [C],
      pd: Pickler [D],
      pe: Pickler [E],
      pf: Pickler [F],
      pg: Pickler [G],
      ph: Pickler [H],
      pi: Pickler [I],
      pj: Pickler [J],
      pk: Pickler [K],
      pl: Pickler [L],
      pm: Pickler [M],
      pn: Pickler [N],
      po: Pickler [O],
      pp: Pickler [P],
      pq: Pickler [Q],
      pr: Pickler [R])
      (build: (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R) => V)
      (inspect: V => (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R))
      (implicit ct: ClassTag [V]):
      Pickler [V] = {

    require (pa != null)
    require (pb != null)
    require (pc != null)
    require (pd != null)
    require (pe != null)
    require (pf != null)
    require (pg != null)
    require (ph != null)
    require (pi != null)
    require (pj != null)
    require (pk != null)
    require (pl != null)
    require (pm != null)
    require (pn != null)
    require (po != null)
    require (pp != null)
    require (pq != null)
    require (pr != null)

    new Pickler [V] {

      def p (v: V, ctx: PickleContext) {
        val vt = inspect (v)
        pa.p (vt._1, ctx)
        pb.p (vt._2, ctx)
        pc.p (vt._3, ctx)
        pd.p (vt._4, ctx)
        pe.p (vt._5, ctx)
        pf.p (vt._6, ctx)
        pg.p (vt._7, ctx)
        ph.p (vt._8, ctx)
        pi.p (vt._9, ctx)
        pj.p (vt._10, ctx)
        pk.p (vt._11, ctx)
        pl.p (vt._12, ctx)
        pm.p (vt._13, ctx)
        pn.p (vt._14, ctx)
        po.p (vt._15, ctx)
        pp.p (vt._16, ctx)
        pq.p (vt._17, ctx)
        pr.p (vt._18, ctx)
      }

      def u (ctx: UnpickleContext) = {
        build (pa.u (ctx),
            pb.u (ctx),
            pc.u (ctx),
            pd.u (ctx),
            pe.u (ctx),
            pf.u (ctx),
            pg.u (ctx),
            ph.u (ctx),
            pi.u (ctx),
            pj.u (ctx),
            pk.u (ctx),
            pl.u (ctx),
            pm.u (ctx),
            pn.u (ctx),
            po.u (ctx),
            pp.u (ctx),
            pq.u (ctx),
            pr.u (ctx))
      }

      override def toString = ct.runtimeClass.getSimpleName
    }}}

object Picklers extends Picklers {

  final case class Tag [A] (tag: Long, clazz: Class [A], pa: Pickler [A]) {

    private [pickle] def read (in: UnpickleContext) = pa.u (in)

    private [pickle] def write (v: Any, ctx: PickleContext) {
      long.p (tag, ctx)
      pa.p (clazz.cast (v).asInstanceOf [A], ctx)
    }}}
