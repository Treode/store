/*
 * Copyright 2014 Treode, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.treode.pickle

import java.net.{InetSocketAddress, SocketAddress}
import scala.collection.generic.GenericCompanion
import scala.language.{existentials, higherKinds, implicitConversions}
import scala.reflect.ClassTag

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
          case None => throw new Exception ("No tag for " + v.getClass.getName)
        }}

      def u (ctx: UnpickleContext): A = {
        val n = ctx.readVarULong()
        ps.get (n) match {
          case Some (tag) => tag.read (ctx)
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
      (pa: Pickler [A]): Pickler [C [A]] = {
    require (pa != null)

    new Pickler [C [A]] {

      def p (v: C [A], ctx: PickleContext): Unit = {
        ctx.writeVarUInt (v.size)
        val i = v.iterator
        while (i.hasNext)
          pa.p (i.next, ctx)
      }

      def u (ctx: UnpickleContext) = {
        val n = ctx.readVarUInt()
        val b = c.newBuilder [A]
        b.sizeHint (n)
        var i = 0
        while (i < n) {
          b += pa.u (ctx)
          i += 1
        }
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
    wrap (list (tuple (pk, pv)))
    .build (kvs => Map (kvs: _*))
    .inspect (m => m.toList)
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

  val socketAddress =
    wrap (tuple (option (string), uint))
    .build [SocketAddress] {
      case (Some (host), port) => new InetSocketAddress (host, port)
      case (None, port) => new InetSocketAddress (port)
    } .inspect {
      case addr: InetSocketAddress if addr.getAddress.isAnyLocalAddress =>
        (None, addr.getPort)
      case addr: InetSocketAddress =>
        (Some (addr.getHostString), addr.getPort)
      case addr =>
        throw new MatchError (addr)
    }

  object immutable {
    import _root_.scala.collection.immutable.SortedMap

    def sortedMap [K, V] (pk: Pickler [K], pv: Pickler [V]) (implicit ordk: Ordering [K]) = {
      require (pk != null)
      require (pv != null)
      wrap (list (tuple (pk, pv)))
      .build (kvs => SortedMap (kvs: _*))
      .inspect (m => m.toList)
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
          val i = v.iterator
          while (i.hasNext)
            pa.p (i.next, ctx)
        }

        def u (ctx: UnpickleContext) = {
          val n = ctx.readInt
          val v = new util.ArrayList [A] (n)
          var i = 0
          while (i < n) {
            v.add (pa.u (ctx))
            i += 1
          }
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

  private def _wrap [A, B] (
      pickler: Pickler [A],
      build: A => B,
      inspect: B => A,
      tag: ClassTag [B]): Pickler [B] =
    new Pickler [B] {
      def p (v: B, ctx: PickleContext): Unit = pickler.p (inspect (v), ctx)
      def u (ctx: UnpickleContext): B = build (pickler.u (ctx))
      override def toString = tag.runtimeClass.getSimpleName
  }

  class WrapInspect [A, B] (pickler: Pickler [A], build: A => B) {
    def inspect (f: B => A) (implicit t: ClassTag [B]) = _wrap (pickler, build, f, t)
  }

  class WrapBuild [A] (pickler: Pickler [A]) {
    def build [B] (f: A => B) = new WrapInspect (pickler, f)
  }

  trait Wrap [T] {
    def apply [V] (build: T => V, inspect: V => T) (implicit ct: ClassTag [V]): Pickler [V]
  }

  def wrap [A] (
      pa: Pickler [A]) =
        new WrapBuild (pa)

  def wrap [A, B] (
      pa: Pickler [A],
      pb: Pickler [B]) =
        new WrapBuild (tuple (pa, pb))

  def wrap [A, B, C] (
      pa: Pickler [A],
      pb: Pickler [B],
      pc: Pickler [C]) =
        new WrapBuild (tuple (pa, pb, pc))

  def wrap [A, B, C, D] (
      pa: Pickler [A],
      pb: Pickler [B],
      pc: Pickler [C],
      pd: Pickler [D]) =
        new WrapBuild (tuple (pa, pb, pc, pd))

  def wrap [A, B, C, D, E] (
      pa: Pickler [A],
      pb: Pickler [B],
      pc: Pickler [C],
      pd: Pickler [D],
      pe: Pickler [E]) =
        new WrapBuild (tuple (pa, pb, pc, pd, pe))

  def wrap [A, B, C, D, E, F] (
      pa: Pickler [A],
      pb: Pickler [B],
      pc: Pickler [C],
      pd: Pickler [D],
      pe: Pickler [E],
      pf: Pickler [F]) =
        new WrapBuild (tuple (pa, pb, pc, pd, pe, pf))

  def wrap [A, B, C, D, E, F, G] (
      pa: Pickler [A],
      pb: Pickler [B],
      pc: Pickler [C],
      pd: Pickler [D],
      pe: Pickler [E],
      pf: Pickler [F],
      pg: Pickler [G]) =
        new WrapBuild (tuple (pa, pb, pc, pd, pe, pf, pg))

  def wrap [A, B, C, D, E, F, G, H] (
      pa: Pickler [A],
      pb: Pickler [B],
      pc: Pickler [C],
      pd: Pickler [D],
      pe: Pickler [E],
      pf: Pickler [F],
      pg: Pickler [G],
      ph: Pickler [H]) =
        new WrapBuild (tuple (pa, pb, pc, pd, pe, pf, pg, ph))

  def wrap [A, B, C, D, E, F, G, H, I] (
      pa: Pickler [A],
      pb: Pickler [B],
      pc: Pickler [C],
      pd: Pickler [D],
      pe: Pickler [E],
      pf: Pickler [F],
      pg: Pickler [G],
      ph: Pickler [H],
      pi: Pickler [I]) =
        new WrapBuild (tuple (pa, pb, pc, pd, pe, pf, pg, ph, pi))

  def wrap [A, B, C, D, E, F, G, H, I, J] (
      pa: Pickler [A],
      pb: Pickler [B],
      pc: Pickler [C],
      pd: Pickler [D],
      pe: Pickler [E],
      pf: Pickler [F],
      pg: Pickler [G],
      ph: Pickler [H],
      pi: Pickler [I],
      pj: Pickler [J]) =
        new WrapBuild (tuple (pa, pb, pc, pd, pe, pf, pg, ph, pi, pj))

  def wrap [A, B, C, D, E, F, G, H, I, J, K] (
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
      pk: Pickler [K]) =
        new WrapBuild (tuple (pa, pb, pc, pd, pe, pf, pg, ph, pi, pj, pk))

  def wrap [A, B, C, D, E, F, G, H, I, J, K, L] (
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
      pl: Pickler [L]) =
        new WrapBuild (tuple (pa, pb, pc, pd, pe, pf, pg, ph, pi, pj, pk, pl))

  def wrap [A, B, C, D, E, F, G, H, I, J, K, L, M] (
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
      pm: Pickler [M]) =
        new WrapBuild (tuple (pa, pb, pc, pd, pe, pf, pg, ph, pi, pj, pk, pl, pm))

  def wrap [A, B, C, D, E, F, G, H, I, J, K, L, M, N] (
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
      pn: Pickler [N]) =
        new WrapBuild (tuple (pa, pb, pc, pd, pe, pf, pg, ph, pi, pj, pk, pl, pm, pn))

  def wrap [A, B, C, D, E, F, G, H, I, J, K, L, M, N, O] (
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
      po: Pickler [O]) =
        new WrapBuild (tuple (pa, pb, pc, pd, pe, pf, pg, ph, pi, pj, pk, pl, pm, pn, po))

  def wrap [A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P] (
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
      pp: Pickler [P]) =
        new WrapBuild (tuple (pa, pb, pc, pd, pe, pf, pg, ph, pi, pj, pk, pl, pm, pn, po, pp))

  def wrap [A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q] (
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
      pq: Pickler [Q]) =
        new WrapBuild (tuple (pa, pb, pc, pd, pe, pf, pg, ph, pi, pj, pk, pl, pm, pn, po, pp, pq))

  def wrap [A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R] (
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
      pr: Pickler [R]) =
        new WrapBuild (tuple (pa, pb, pc, pd, pe, pf, pg, ph, pi, pj, pk, pl, pm, pn, po, pp, pq, pr))
}

object Picklers extends Picklers {

  final case class Tag [A] (tag: Long, clazz: Class [A], pa: Pickler [A]) {

    private [pickle] def read (ctx: UnpickleContext) = pa.u (ctx)

    private [pickle] def write (v: Any, ctx: PickleContext) {
      ctx.writeVarULong (tag)
      pa.p (clazz.cast (v).asInstanceOf [A], ctx)
    }}}
