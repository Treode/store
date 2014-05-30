package com.treode.store

import scala.language.implicitConversions

import com.google.common.primitives.{Longs, UnsignedLongs}
import com.treode.async.misc.parseLong
import com.treode.pickle.Picklers
import org.joda.time.{DateTime, Instant}

class TxClock private [store] (val time: Long) extends AnyVal with Ordered [TxClock] {

  def + (n: Int): TxClock = new TxClock (time+n)

  def - (n: Int): TxClock = new TxClock (time-n)

  def byteSize: Int = Longs.BYTES

  def compare (that: TxClock): Int =
    UnsignedLongs.compare (this.time, that.time)

  override def toString = "0x%X" format time
}

object TxClock extends Ordering [TxClock] {

  val MinValue = new TxClock (0)

  val MaxValue = new TxClock (-1)

  def now = new TxClock (System.currentTimeMillis * 1000)

  implicit def apply (time: Instant): TxClock =
    new TxClock (time.getMillis * 1000)

  implicit def apply (time: DateTime): TxClock =
    new TxClock (time.getMillis * 1000)

  def parse (s: String): Option [TxClock] =
    parseLong (s) .map (new TxClock (_))

  def compare (x: TxClock, y: TxClock): Int =
    x compare y

  val pickler = {
    import Picklers._
    wrap (ulong) build (new TxClock (_)) inspect (_.time)
  }}
