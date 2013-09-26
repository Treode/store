package com.treode.pickle

import java.net.{InetSocketAddress, SocketAddress}
import java.nio.ByteBuffer
import java.nio.charset.{Charset, StandardCharsets}
import scala.collection.generic.GenericCompanion
import scala.collection.mutable
import scala.language.{existentials, higherKinds, implicitConversions}
import scala.reflect.ClassTag

trait Picklers {

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
      def p (v: Int, ctx: PickleContext) = ctx.writeVariableLengthInt (v)
      def u (ctx: UnpickleContext) = ctx.readVariableLengthInt()
      override def toString = "int"
    }

  val long: Pickler [Long] =
    new Pickler [Long] {
      def p (v: Long, ctx: PickleContext) = ctx.writeVariableLengthLong (v)
      def u (ctx: UnpickleContext) = ctx.readVariableLengthLong()
      override def toString = "long"
    }

  val unsignedInt: Pickler [Int] =
    new Pickler [Int] {
      def p (v: Int, ctx: PickleContext) = ctx.writeVariableLengthUnsignedInt (v)
      def u (ctx: UnpickleContext) = ctx.readVariableLengthUnsignedInt()
      override def toString = "int"
    }

  val unsignedLong: Pickler [Long] =
    new Pickler [Long] {
      def p (v: Long, ctx: PickleContext) = ctx.writeVariableLengthUnsignedLong (v)
      def u (ctx: UnpickleContext) = ctx.readVariableLengthUnsignedLong()
      override def toString = "long"
    }

  val fixedShort: Pickler [Short] =
    new Pickler [Short] {
      def p (v: Short, ctx: PickleContext) = ctx.writeShort (v)
      def u (ctx: UnpickleContext) = ctx.readShort()
      override def toString = "fixedShort"
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

  def string (cs: Charset): Pickler [String] =
    new Pickler [String] {
      def p (v: String, ctx: PickleContext) = {
        val bb = cs.encode (v)
        int.p (bb.limit, ctx)
        ctx.writeBytes (bb)
      }
      def u (ctx: UnpickleContext) = {
        val l = int.u (ctx)
        val bb = ByteBuffer.allocate (l)
        ctx.readBytes (bb)
        bb.flip()
        cs.decode (bb).toString
      }
      override def toString = "string"
    }

  val string: Pickler [String] = string (StandardCharsets.UTF_8)

  def const [T] (v: T): Pickler [T] =
    new Pickler [T] {
      def p (v: T, ctx: PickleContext) = ()
      def u (ctx: UnpickleContext) = v
      override def toString = "const (" + v + ")"
    }

  val unit = const (())

  def laze [T] (pa: => Pickler [T]): Pickler [T] =
    new Pickler [T] {
      lazy val p1 = pa
      def p (v: T, ctx: PickleContext) = p1.p (v, ctx)
      def u (ctx: UnpickleContext) = p1.u (ctx)
      override def toString = "laze (" + pa.toString + ")"
    }

  def tuple [A, B] (pa:  Pickler [A], pb: Pickler [B]): Pickler [(A, B)] = {
    require (pa != null); require (pb != null)

    new Pickler [(A, B)] {
      def p (v: (A, B), ctx: PickleContext) = {
        pa.p (v._1, ctx); pb.p (v._2, ctx)
      }
      def u (ctx: UnpickleContext) = {
        (pa.u (ctx), pb.u (ctx))
      }
      override def toString = "tuple " + (pa, pb)
    }}

  def tuple [A, B, C] (pa: Pickler [A], pb: Pickler [B], pc: Pickler [C]): Pickler [(A, B, C)] = {
    require (pa != null); require (pb != null); require (pc != null)

    new Pickler [(A, B, C)] {
      def p (v: (A, B, C), ctx: PickleContext): Unit = {
        pa.p (v._1, ctx); pb.p (v._2, ctx); pc.p (v._3, ctx)
      }
      def u (ctx: UnpickleContext) = {
        (pa.u (ctx), pb.u (ctx), pc.u (ctx))
      }
      override def toString = "tuple " + (pa, pb, pc)
    }}

  def tuple [A, B, C, D] (pa: Pickler [A], pb: Pickler [B], pc: Pickler [C], pd: Pickler [D]):
      Pickler [(A, B, C, D)] = {
    require (pa != null); require (pb != null); require (pc != null); require (pd != null)

    new Pickler [(A, B, C, D)] {
      def p (v: (A, B, C, D), ctx: PickleContext): Unit = {
        pa.p (v._1, ctx); pb.p (v._2, ctx); pc.p (v._3, ctx); pd.p (v._4, ctx)
      }
      def u (ctx: UnpickleContext) = {
        (pa.u (ctx), pb.u (ctx), pc.u (ctx), pd.u (ctx))
      }
      override def toString = "tuple " + (pa, pb, pc, pd)
    }}

  def tuple [A, B, C, D, E] (pa: Pickler [A], pb: Pickler [B], pc: Pickler [C], pd: Pickler [D],
      pe: Pickler [E]): Pickler [(A, B, C, D, E)] = {
    require (pa != null); require (pb != null); require (pc != null); require (pd != null)
    require (pe != null)

    new Pickler [(A, B, C, D, E)] {
      def p (v: (A, B, C, D, E), ctx: PickleContext): Unit = {
        pa.p (v._1, ctx); pb.p (v._2, ctx); pc.p (v._3, ctx); pd.p (v._4, ctx); pe.p (v._5, ctx)
      }
      def u (ctx: UnpickleContext) = {
        (pa.u (ctx), pb.u (ctx), pc.u (ctx), pd.u (ctx), pe.u (ctx))
      }
      override def toString = "tuple " + (pa, pb, pc, pd, pe)
    }}

  def tuple [A, B, C, D, E, F] (pa: Pickler [A], pb: Pickler [B], pc: Pickler [C], pd: Pickler [D],
      pe: Pickler [E], pf: Pickler [F]): Pickler [(A, B, C, D, E, F)] = {
    require (pa != null); require (pb != null); require (pc != null); require (pd != null)
    require (pe != null); require (pf != null)

    new Pickler [(A, B, C, D, E, F)] {
      def p (v: (A, B, C, D, E, F), ctx: PickleContext): Unit = {
        pa.p (v._1, ctx); pb.p (v._2, ctx); pc.p (v._3, ctx); pd.p (v._4, ctx); pe.p (v._5, ctx); pf.p (v._6, ctx)
      }
      def u (ctx: UnpickleContext) = {
        (pa.u (ctx), pb.u (ctx), pc.u (ctx), pd.u (ctx), pe.u (ctx), pf.u (ctx))
      }
      override def toString = "tuple " + (pa, pb, pc, pd, pe, pf)
    }}

  def tuple [A, B, C, D, E, F, G] (pa: Pickler [A], pb: Pickler [B], pc: Pickler [C],
      pd: Pickler [D], pe: Pickler [E], pf: Pickler [F], pg: Pickler [G]):
      Pickler [(A, B, C, D, E, F, G)] = {
    require (pa != null); require (pb != null); require (pc != null); require (pd != null)
    require (pe != null); require (pf != null); require (pg != null)

    new Pickler [(A, B, C, D, E, F, G)] {
      def p (v: (A, B, C, D, E, F, G), ctx: PickleContext): Unit = {
        pa.p (v._1, ctx); pb.p (v._2, ctx); pc.p (v._3, ctx); pd.p (v._4, ctx); pe.p (v._5, ctx)
        pf.p (v._6, ctx); pg.p (v._7, ctx)
      }
      def u (ctx: UnpickleContext) = {
        (pa.u (ctx), pb.u (ctx), pc.u (ctx), pd.u (ctx), pe.u (ctx), pf.u (ctx), pg.u (ctx))
      }
      override def toString = "tuple " + (pa, pb, pc, pd, pe, pf, pg)
    }}

  def tuple [A, B, C, D, E, F, G, H] (pa: Pickler [A], pb: Pickler [B], pc: Pickler [C],
      pd: Pickler [D], pe: Pickler [E], pf: Pickler [F], pg: Pickler [G], ph: Pickler [H]):
      Pickler [(A, B, C, D, E, F, G, H)] = {
    require (pa != null); require (pb != null); require (pc != null); require (pd != null)
    require (pe != null); require (pf != null); require (pg != null); require (ph != null)

    new Pickler [(A, B, C, D, E, F, G, H)] {
      def p (v: (A, B, C, D, E, F, G, H), ctx: PickleContext): Unit = {
        pa.p (v._1, ctx); pb.p (v._2, ctx); pc.p (v._3, ctx); pd.p (v._4, ctx); pe.p (v._5, ctx)
        pf.p (v._6, ctx); pg.p (v._7, ctx); ph.p (v._8, ctx)
      }
      def u (ctx: UnpickleContext) = {
        (pa.u (ctx), pb.u (ctx), pc.u (ctx), pd.u (ctx), pe.u (ctx), pf.u (ctx), pg.u (ctx),
            ph.u (ctx))
      }
      override def toString = "tuple " + (pa, pb, pc, pd, pe, pf, pg, ph)
    }}

  def tuple [A, B, C, D, E, F, G, H, I] (pa: Pickler [A], pb: Pickler [B], pc: Pickler [C],
      pd: Pickler [D], pe: Pickler [E], pf: Pickler [F], pg: Pickler [G], ph: Pickler [H],
      pi: Pickler [I]): Pickler [(A, B, C, D, E, F, G, H, I)] = {
    require (pa != null); require (pb != null); require (pc != null); require (pd != null)
    require (pe != null); require (pf != null); require (pg != null); require (ph != null)
    require (pi != null)

   new Pickler [(A, B, C, D, E, F, G, H, I)] {
      def p (v: (A, B, C, D, E, F, G, H, I), ctx: PickleContext): Unit = {
        pa.p (v._1, ctx); pb.p (v._2, ctx); pc.p (v._3, ctx); pd.p (v._4, ctx); pe.p (v._5, ctx)
        pf.p (v._6, ctx); pg.p (v._7, ctx); ph.p (v._8, ctx); pi.p (v._9, ctx)
      }
      def u (ctx: UnpickleContext) = {
        (pa.u (ctx), pb.u (ctx), pc.u (ctx), pd.u (ctx), pe.u (ctx), pf.u (ctx), pg.u (ctx),
            ph.u (ctx), pi.u (ctx))
      }
      override def toString = "tuple " + (pa, pb, pc, pd, pe, pf, pg, ph, pi)
    }}

  def tuple [A, B, C, D, E, F, G, H, I, J] (pa: Pickler [A], pb: Pickler [B], pc: Pickler [C],
      pd: Pickler [D], pe: Pickler [E], pf: Pickler [F], pg: Pickler [G], ph: Pickler [H],
      pi: Pickler [I], pj: Pickler [J]): Pickler [(A, B, C, D, E, F, G, H, I, J)] = {
    require (pa != null); require (pb != null); require (pc != null); require (pd != null)
    require (pe != null); require (pf != null); require (pg != null); require (ph != null)
    require (pi != null); require (pj != null)

    new Pickler [(A, B, C, D, E, F, G, H, I, J)] {
      def p (v: (A, B, C, D, E, F, G, H, I, J), ctx: PickleContext): Unit = {
        pa.p (v._1, ctx); pb.p (v._2, ctx); pc.p (v._3, ctx); pd.p (v._4, ctx); pe.p (v._5, ctx)
        pf.p (v._6, ctx); pg.p (v._7, ctx); ph.p (v._8, ctx); pi.p (v._9, ctx); pj.p (v._10, ctx)
      }
      def u (ctx: UnpickleContext) = {
        (pa.u (ctx), pb.u (ctx), pc.u (ctx), pd.u (ctx), pe.u (ctx), pf.u (ctx), pg.u (ctx),
            ph.u (ctx), pi.u (ctx), pj.u (ctx))
      }
      override def toString = "tuple " + (pa, pb, pc, pd, pe, pf, pg, ph, pi, pj)
    }}

  def tuple [A, B, C, D, E, F, G, H, I, J, K] (pa: Pickler [A], pb: Pickler [B], pc: Pickler [C],
      pd: Pickler [D], pe: Pickler [E], pf: Pickler [F], pg: Pickler [G], ph: Pickler [H],
      pi: Pickler [I], pj: Pickler [J], pk: Pickler [K]):
      Pickler [(A, B, C, D, E, F, G, H, I, J, K)] = {
    require (pa != null); require (pb != null); require (pc != null); require (pd != null)
    require (pe != null); require (pf != null); require (pg != null); require (ph != null)
    require (pi != null); require (pj != null); require (pk != null)

    new Pickler [(A, B, C, D, E, F, G, H, I, J, K)] {
      def p (v: (A, B, C, D, E, F, G, H, I, J, K), ctx: PickleContext): Unit = {
        pa.p (v._1, ctx); pb.p (v._2, ctx); pc.p (v._3, ctx); pd.p (v._4, ctx); pe.p (v._5, ctx)
        pf.p (v._6, ctx); pg.p (v._7, ctx); ph.p (v._8, ctx); pi.p (v._9, ctx); pj.p (v._10, ctx)
        pk.p (v._11, ctx)
      }
      def u (ctx: UnpickleContext) = {
        (pa.u (ctx), pb.u (ctx), pc.u (ctx), pd.u (ctx), pe.u (ctx), pf.u (ctx), pg.u (ctx),
            ph.u (ctx), pi.u (ctx), pj.u (ctx), pk.u (ctx))
      }
      override def toString = "tuple " + (pa, pb, pc, pd, pe, pf, pg, ph, pi, pj, pk)
    }}

  def tuple [A, B, C, D, E, F, G, H, I, J, K, L] (pa: Pickler [A], pb: Pickler [B],
      pc: Pickler [C], pd: Pickler [D], pe: Pickler [E], pf: Pickler [F], pg: Pickler [G],
      ph: Pickler [H], pi: Pickler [I], pj: Pickler [J], pk: Pickler [K], pl: Pickler [L]):
      Pickler [(A, B, C, D, E, F, G, H, I, J, K, L)] = {
    require (pa != null); require (pb != null); require (pc != null); require (pd != null)
    require (pe != null); require (pf != null); require (pg != null); require (ph != null)
    require (pi != null); require (pj != null); require (pk != null); require (pl != null)

    new Pickler [(A, B, C, D, E, F, G, H, I, J, K, L)] {
      def p (v: (A, B, C, D, E, F, G, H, I, J, K, L), ctx: PickleContext): Unit = {
        pa.p (v._1, ctx); pb.p (v._2, ctx); pc.p (v._3, ctx); pd.p (v._4, ctx); pe.p (v._5, ctx)
        pf.p (v._6, ctx); pg.p (v._7, ctx); ph.p (v._8, ctx); pi.p (v._9, ctx); pj.p (v._10, ctx)
        pk.p (v._11, ctx); pl.p (v._12, ctx)
      }
      def u (ctx: UnpickleContext) = {
        (pa.u (ctx), pb.u (ctx), pc.u (ctx), pd.u (ctx), pe.u (ctx), pf.u (ctx), pg.u (ctx),
            ph.u (ctx), pi.u (ctx), pj.u (ctx), pk.u (ctx), pl.u (ctx))
      }
      override def toString = "tuple " + (pa, pb, pc, pd, pe, pf, pg, ph, pi, pj, pk)
    }}

  def tuple [A, B, C, D, E, F, G, H, I, J, K, L, M] (pa: Pickler [A], pb: Pickler [B],
      pc: Pickler [C], pd: Pickler [D], pe: Pickler [E], pf: Pickler [F], pg: Pickler [G],
      ph: Pickler [H], pi: Pickler [I], pj: Pickler [J], pk: Pickler [K], pl: Pickler [L],
      pm: Pickler [M]): Pickler [(A, B, C, D, E, F, G, H, I, J, K, L, M)] = {
    require (pa != null); require (pb != null); require (pc != null); require (pd != null)
    require (pe != null); require (pf != null); require (pg != null); require (ph != null)
    require (pi != null); require (pj != null); require (pk != null); require (pl != null)
    require (pm != null)

    new Pickler [(A, B, C, D, E, F, G, H, I, J, K, L, M)] {
      def p (v: (A, B, C, D, E, F, G, H, I, J, K, L, M), ctx: PickleContext): Unit = {
        pa.p (v._1, ctx); pb.p (v._2, ctx); pc.p (v._3, ctx); pd.p (v._4, ctx); pe.p (v._5, ctx)
        pf.p (v._6, ctx); pg.p (v._7, ctx); ph.p (v._8, ctx); pi.p (v._9, ctx); pj.p (v._10, ctx)
        pk.p (v._11, ctx); pl.p (v._12, ctx); pm.p (v._13, ctx)
      }
      def u (ctx: UnpickleContext) = {
        (pa.u (ctx), pb.u (ctx), pc.u (ctx), pd.u (ctx), pe.u (ctx), pf.u (ctx), pg.u (ctx),
            ph.u (ctx), pi.u (ctx), pj.u (ctx), pk.u (ctx), pl.u (ctx), pm.u (ctx))
      }
      override def toString = "tuple " + (pa, pb, pc, pd, pe, pf, pg, ph, pi, pj, pk, pl, pm)
    }}

  def tuple [A, B, C, D, E, F, G, H, I, J, K, L, M, N] (pa: Pickler [A], pb: Pickler [B],
      pc: Pickler [C], pd: Pickler [D], pe: Pickler [E], pf: Pickler [F], pg: Pickler [G],
      ph: Pickler [H], pi: Pickler [I], pj: Pickler [J], pk: Pickler [K], pl: Pickler [L],
      pm: Pickler [M], pn: Pickler [N]): Pickler [(A, B, C, D, E, F, G, H, I, J, K, L, M, N)] = {
    require (pa != null); require (pb != null); require (pc != null); require (pd != null)
    require (pe != null); require (pf != null); require (pg != null); require (ph != null)
    require (pi != null); require (pj != null); require (pk != null); require (pl != null)
    require (pm != null); require (pn != null)

    new Pickler [(A, B, C, D, E, F, G, H, I, J, K, L, M, N)] {
      def p (v: (A, B, C, D, E, F, G, H, I, J, K, L, M, N), ctx: PickleContext): Unit = {
        pa.p (v._1, ctx); pb.p (v._2, ctx); pc.p (v._3, ctx); pd.p (v._4, ctx); pe.p (v._5, ctx)
        pf.p (v._6, ctx); pg.p (v._7, ctx); ph.p (v._8, ctx); pi.p (v._9, ctx); pj.p (v._10, ctx)
        pk.p (v._11, ctx); pl.p (v._12, ctx); pm.p (v._13, ctx); pn.p (v._14, ctx)
      }
      def u (ctx: UnpickleContext) = {
        (pa.u (ctx), pb.u (ctx), pc.u (ctx), pd.u (ctx), pe.u (ctx), pf.u (ctx), pg.u (ctx),
            ph.u (ctx), pi.u (ctx), pj.u (ctx), pk.u (ctx), pl.u (ctx), pm.u (ctx), pn.u (ctx))
      }
      override def toString = "tuple " + (pa, pb, pc, pd, pe, pf, pg, ph, pi, pj, pk, pl, pm, pn)
    }}

  def tuple [A, B, C, D, E, F, G, H, I, J, K, L, M, N, O] (pa: Pickler [A], pb: Pickler [B],
      pc: Pickler [C], pd: Pickler [D], pe: Pickler [E], pf: Pickler [F], pg: Pickler [G],
      ph: Pickler [H], pi: Pickler [I], pj: Pickler [J], pk: Pickler [K], pl: Pickler [L],
      pm: Pickler [M], pn: Pickler [N], po: Pickler [O]):
      Pickler [(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O)] = {
    require (pa != null); require (pb != null); require (pc != null); require (pd != null)
    require (pe != null); require (pf != null); require (pg != null); require (ph != null)
    require (pi != null); require (pj != null); require (pk != null); require (pl != null)
    require (pm != null); require (pn != null); require (po != null)

    new Pickler [(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O)] {
      def p (v: (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O), ctx: PickleContext): Unit = {
        pa.p (v._1, ctx); pb.p (v._2, ctx); pc.p (v._3, ctx); pd.p (v._4, ctx); pe.p (v._5, ctx)
        pf.p (v._6, ctx); pg.p (v._7, ctx); ph.p (v._8, ctx); pi.p (v._9, ctx); pj.p (v._10, ctx)
        pk.p (v._11, ctx); pl.p (v._12, ctx); pm.p (v._13, ctx); pn.p (v._14, ctx)
      }
      def u (ctx: UnpickleContext) = {
        (pa.u (ctx), pb.u (ctx), pc.u (ctx), pd.u (ctx), pe.u (ctx), pf.u (ctx), pg.u (ctx),
            ph.u (ctx), pi.u (ctx), pj.u (ctx), pk.u (ctx), pl.u (ctx), pm.u (ctx), pn.u (ctx),
            po.u (ctx))
      }
      override def toString =
        "tuple " + (pa, pb, pc, pd, pe, pf, pg, ph, pi, pj, pk, pl, pm, pn, po)
    }}

  def tuple [A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P] (pa: Pickler [A], pb: Pickler [B],
      pc: Pickler [C], pd: Pickler [D], pe: Pickler [E], pf: Pickler [F], pg: Pickler [G],
      ph: Pickler [H], pi: Pickler [I], pj: Pickler [J], pk: Pickler [K], pl: Pickler [L],
      pm: Pickler [M], pn: Pickler [N], po: Pickler [O], pp: Pickler [P]):
      Pickler [(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P)] = {
    require (pa != null); require (pb != null); require (pc != null); require (pd != null)
    require (pe != null); require (pf != null); require (pg != null); require (ph != null)
    require (pi != null); require (pj != null); require (pk != null); require (pl != null)
    require (pm != null); require (pn != null); require (po != null); require (pp != null)

    new Pickler [(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P)] {
      def p (v: (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P), ctx: PickleContext): Unit = {
        pa.p (v._1, ctx); pb.p (v._2, ctx); pc.p (v._3, ctx); pd.p (v._4, ctx); pe.p (v._5, ctx)
        pf.p (v._6, ctx); pg.p (v._7, ctx); ph.p (v._8, ctx); pi.p (v._9, ctx); pj.p (v._10, ctx)
        pk.p (v._11, ctx); pl.p (v._12, ctx); pm.p (v._13, ctx); pn.p (v._14, ctx)
        po.p (v._15, ctx); pp.p (v._16, ctx)
      }
      def u (ctx: UnpickleContext) = {
        (pa.u (ctx), pb.u (ctx), pc.u (ctx), pd.u (ctx), pe.u (ctx), pf.u (ctx), pg.u (ctx),
            ph.u (ctx), pi.u (ctx), pj.u (ctx), pk.u (ctx), pl.u (ctx), pm.u (ctx), pn.u (ctx),
            po.u (ctx), pp.u (ctx))
      }
      override def toString =
        "tuple " + (pa, pb, pc, pd, pe, pf, pg, ph, pi, pj, pk, pl, pm, pn, po, pp)
    }}

  def tuple [A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q] (pa: Pickler [A], pb: Pickler [B],
      pc: Pickler [C], pd: Pickler [D], pe: Pickler [E], pf: Pickler [F], pg: Pickler [G],
      ph: Pickler [H], pi: Pickler [I], pj: Pickler [J], pk: Pickler [K], pl: Pickler [L],
      pm: Pickler [M], pn: Pickler [N], po: Pickler [O], pp: Pickler [P], pq: Pickler [Q]):
      Pickler [(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q)] = {
    require (pa != null); require (pb != null); require (pc != null); require (pd != null)
    require (pe != null); require (pf != null); require (pg != null); require (ph != null)
    require (pi != null); require (pj != null); require (pk != null); require (pl != null)
    require (pm != null); require (pn != null); require (po != null); require (pp != null)
    require (pq != null)

    new Pickler [(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q)] {
      def p (v: (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q), ctx: PickleContext): Unit = {
        pa.p (v._1, ctx); pb.p (v._2, ctx); pc.p (v._3, ctx); pd.p (v._4, ctx); pe.p (v._5, ctx)
        pf.p (v._6, ctx); pg.p (v._7, ctx); ph.p (v._8, ctx); pi.p (v._9, ctx); pj.p (v._10, ctx)
        pk.p (v._11, ctx); pl.p (v._12, ctx); pm.p (v._13, ctx); pn.p (v._14, ctx)
        po.p (v._15, ctx); pp.p (v._16, ctx); pq.p (v._17, ctx)
      }
      def u (ctx: UnpickleContext) = {
        (pa.u (ctx), pb.u (ctx), pc.u (ctx), pd.u (ctx), pe.u (ctx), pf.u (ctx), pg.u (ctx),
            ph.u (ctx), pi.u (ctx), pj.u (ctx), pk.u (ctx), pl.u (ctx), pm.u (ctx), pn.u (ctx),
            po.u (ctx), pp.u (ctx), pq.u (ctx))
      }
      override def toString =
        "tuple " + (pa, pb, pc, pd, pe, pf, pg, ph, pi, pj, pk, pl, pm, pn, po, pp, pq)
    }}

  def tuple [A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R] (pa: Pickler [A],
      pb: Pickler [B], pc: Pickler [C], pd: Pickler [D], pe: Pickler [E], pf: Pickler [F],
      pg: Pickler [G], ph: Pickler [H], pi: Pickler [I], pj: Pickler [J], pk: Pickler [K],
      pl: Pickler [L], pm: Pickler [M], pn: Pickler [N], po: Pickler [O], pp: Pickler [P],
      pq: Pickler [Q], pr: Pickler [R]):
      Pickler [(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R)] = {
    require (pa != null); require (pb != null); require (pc != null); require (pd != null)
    require (pe != null); require (pf != null); require (pg != null); require (ph != null)
    require (pi != null); require (pj != null); require (pk != null); require (pl != null)
    require (pm != null); require (pn != null); require (po != null); require (pp != null)
    require (pq != null); require (pr != null)

    new Pickler [(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R)] {
      def p (v: (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R), ctx: PickleContext): Unit = {
        pa.p (v._1, ctx); pb.p (v._2, ctx); pc.p (v._3, ctx); pd.p (v._4, ctx); pe.p (v._5, ctx)
        pf.p (v._6, ctx); pg.p (v._7, ctx); ph.p (v._8, ctx); pi.p (v._9, ctx); pj.p (v._10, ctx)
        pk.p (v._11, ctx); pl.p (v._12, ctx); pm.p (v._13, ctx); pn.p (v._14, ctx)
        po.p (v._15, ctx); pp.p (v._16, ctx); pq.p (v._17, ctx); pr.p (v._18, ctx)
      }
      def u (ctx: UnpickleContext) = {
        (pa.u (ctx), pb.u (ctx), pc.u (ctx), pd.u (ctx), pe.u (ctx), pf.u (ctx), pg.u (ctx),
            ph.u (ctx), pi.u (ctx), pj.u (ctx), pk.u (ctx), pl.u (ctx), pm.u (ctx), pn.u (ctx),
            po.u (ctx), pp.u (ctx), pq.u (ctx), pr.u (ctx))
      }
      override def toString =
        "tuple " + (pa, pb, pc, pd, pe, pf, pg, ph, pi, pj, pk, pl, pm, pn, po, pp, pq, pr)
    }}

  def wrap [A, B] (pickle: Pickler [A], build: A => B, inspect: B => A) (
      implicit mb: ClassTag [B]): Pickler [B] = {
    require (pickle != null)

    new Pickler [B] {
      def p (v: B, ctx: PickleContext) = pickle.p (inspect (v), ctx)
      def u (ctx: UnpickleContext) = build (pickle.u (ctx))
      override def toString = mb.runtimeClass.getSimpleName + " (" + pickle + ")"
    }}

  final case class Tag [A] (tag: Long, clazz: Class [_], pickle: Pickler [A]) {

    private [pickle] def read (in: UnpickleContext) = pickle.u (in)

    private [pickle] def write (v: Any, ctx: PickleContext) {
      long.p (tag, ctx)
      pickle.p (clazz.cast (v).asInstanceOf [A], ctx)
    }}

  implicit def intFormatToTag [A] (tf: (Int, Pickler [A])) (implicit mf: ClassTag [A]): Tag [A] =
    Tag [A] (tf._1.toLong, mf.runtimeClass, tf._2)

  implicit def longFormatToTag [A] (tf: (Long, Pickler [A])) (implicit mf: ClassTag [A]): Tag [A] =
    Tag [A] (tf._1, mf.runtimeClass, tf._2)

  /** Write per first matching class. */
  def tagged [A] (tags: Tag [_ <: A]*) (implicit ma: ClassTag [A]): Pickler [A] =
    new Pickler [A] {
      val ps = Map (tags map (tag => (tag.tag, tag)): _*)
      require (ps.size == tags.size, "Tag reused.")

      def u (in: UnpickleContext): A = {
        val n = long.u (in)
        ps.get (n) match {
          case Some (tag) => tag.read (in)
          case None => throw new InvalidTagException (ma.runtimeClass.getSimpleName, n)
        }}

      def p (v: A, ctx: PickleContext): Unit = {
        tags.find (_.clazz.isInstance (v)) match {
          case Some (tag) => tag.write (v, ctx)
          case None => throw new Exception ("No tag type for " + v.getClass.getName)
        }}

      override def toString = "tagged (" + ma.runtimeClass.getSimpleName + ")"
    }

  def array [A] (pa: Pickler [A]) (implicit m: ClassTag [A]): Pickler [Array [A]] = {
    require (pa != null)

    new Pickler [Array [A]] {

      def p (v: Array [A], ctx: PickleContext): Unit = {
        int.p (v.length, ctx)
        var i = 0
        while (i < v.length) {
          pa.p (v (i), ctx)
          i += 1
        }}

      def u (ctx: UnpickleContext) = {
        val n = int.u (ctx)
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

  def arrayByte: Pickler [Array [Byte]] =
    new Pickler [Array [Byte]] {
      def p (v: Array [Byte], ctx: PickleContext) = {
        int.p (v.length, ctx)
        ctx.writeBytes (v)
      }
      def u (ctx: UnpickleContext) = {
        val l = int.u (ctx)
        val v = new Array [Byte] (l)
        ctx.readBytes (v)
        v
      }
      override def toString = "arrayByte"
    }

  def cast [A, B <: A] (pb: Pickler [B]) (implicit mb: ClassTag [B]): Pickler [A] = {
    require (pb != null)

    val cb = mb.runtimeClass.asInstanceOf [Class [B]]
    new Pickler [A] {
      def p (v: A, ctx: PickleContext): Unit = pb.p (cb.cast (v), ctx)
      def u (ctx: UnpickleContext): A = pb.u (ctx)
    }}

  def record [A] (ps: Seq [Pickler [A]]): Pickler [Seq [A]] = {
    require (ps forall (_ != null))

    new Pickler [Seq [A]] {

      def p (vs: Seq [A], ctx: PickleContext): Unit = {
        require (vs.length == ps.length)
        for ((p, v) <- ps zip vs) {
          p.p (v, ctx)
        }}

      def u (ctx: UnpickleContext) =
        ps map (_.u (ctx))

      override def toString = ps map (_.toString) mkString ("record (", ", ", ")")
    }}

  private [this] def useBuilder [A, C [A] <: Iterable [A]] (
      c: GenericCompanion [C], s: String) (pa: Pickler [A]): Pickler [C [A]] = {
    require (pa != null)

    new Pickler [C [A]] {

      def p (v: C [A], ctx: PickleContext): Unit = {
        int.p (v.size, ctx)
        v foreach (pa.p (_, ctx))
      }

      def u (ctx: UnpickleContext) = {
        val n = int.u (ctx)
        val b = c.newBuilder [A]
        b.sizeHint (n)
        (1 to n) foreach (_ => b += pa.u (ctx))
        b.result()
      }

      override def toString = s + " (" + pa + ")"
    }}

  def list [A] (pa: Pickler [A]) = useBuilder (List, "list") (pa)

  def seq [A] (pa: Pickler [A]) = useBuilder (Seq, "seq") (pa)

  def set [A] (pa: Pickler [A]) = useBuilder (Set, "set") (pa)

  def map [K, V] (pk: Pickler [K], pv: Pickler [V]) = {
    require (pk != null)
    require (pv != null)

    wrap (
      pickle = list (tuple (pk, pv)),
      build = { kvs: List [(K, V)] => Map (kvs: _*) },
      inspect = { m: Map [K, V] => m.toList })
  }

  def option [A] (pa: Pickler [A]): Pickler [Option [A]] = {
    require (pa != null)

    new Pickler [Option [A]] {

      def p (v: Option [A], ctx: PickleContext): Unit = {
        if (v.isEmpty) {
          int.p (0, ctx)
        } else {
          int.p (1, ctx)
          pa.p (v.get, ctx)
        }}

      def u (ctx: UnpickleContext) = {
        val i = int.u (ctx)
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
          int.p (0, ctx)
          pa.p (v.left.get, ctx)
        } else {
          int.p (1, ctx)
          pb.p (v.right.get, ctx)
        }}

      def u (ctx: UnpickleContext) = {
        val i = int.u (ctx)
        if (i == 0)
          Left (pa.u (ctx))
        else
          Right (pb.u (ctx))
      }

      override def toString = "either " + (pa, pb)
    }}

  object pmutable {

    def map [K, V] (pk: Pickler [K], pv: Pickler [V]) =
      wrap (
        pickle = list (tuple (pk, pv)),
        build = { kvs: List [(K, V)] => mutable.Map (kvs: _*) },
        inspect = { m: mutable.Map [K, V] => m.toList })
  }

  def share [A] (pa: Pickler [A]): Pickler [A] = {
    require (pa != null)

    new Pickler [A] {
      def p (v: A, ctx: PickleContext) {
        if (ctx contains v) {
          int.p (0, ctx)
          int.p (ctx get v, ctx)
        } else {
          int.p (1, ctx)
          pa.p (v, ctx)
          ctx.put (v)
        }}

      def u (ctx: UnpickleContext) = {
        val x = int.u (ctx)
        if (x == 0) {
          ctx.get [A] (int.u (ctx))
        } else {
          val v = pa.u (ctx)
          ctx.put (v)
          v
        }}

      override def toString = "share (" + pa + ")"
    }}

  def hash32 [A] (pa: Pickler [A]) =
    wrap (
      pickle = tuple (pa, fixedInt),
      build = { v: (A, Int) => if (v._2 == Hash32.hash (pa, 0, v._1)) Some (v._1) else None },
      inspect = { v: Option [A] => (v.get, Hash32.hash (pa, 0, v.get)) })

  def hash64 [A] (pa: Pickler [A]) =
    wrap (
      pickle = tuple (pa, fixedLong),
      build = { v: (A, Long) => if (v._2 == Hash128.hash (pa, 0, v._1) ._1) Some (v._1) else None },
      inspect = { v: Option [A] => (v.get, Hash128.hash (pa, 0, v.get) ._1) })

  def hash128 [A] (pa: Pickler [A]) =
    wrap (
      pickle = tuple (pa, tuple (fixedLong, fixedLong)),
      build = { v: (A, (Long, Long)) => if (v._2 == Hash128.hash (pa, 0, v._1)) Some (v._1) else None },
      inspect = { v: Option [A] => (v.get, Hash128.hash (pa, 0, v.get)) })

  val stackTrace = {
    val elem =
      wrap [(String, String, String, Int), StackTraceElement] (
        pickle = tuple (string, string, string, int),

        build = ((clazz, method, file, line) =>
          new StackTraceElement(clazz, method, file, line)).tupled,

        inspect = { (e: StackTraceElement) =>
          (e.getClassName, e.getMethodName, e.getFileName, e.getLineNumber) })

    array (elem)
  }

  val socketAddress = new Pickler [SocketAddress] {
    def p (addr: SocketAddress, ctx: PickleContext) {
      addr match {
        case inet: InetSocketAddress =>
          string.p (inet.getHostName, ctx)
          int.p (inet.getPort, ctx)
        case _ =>
          throw new IllegalArgumentException ("Cannot pickle socket address " + addr)
      }}

    def u (ctx: UnpickleContext): SocketAddress = {
      val host = string.u (ctx)
      val port = int.u (ctx)
      new InetSocketAddress (host, port)
    }}}

object Picklers extends Picklers
