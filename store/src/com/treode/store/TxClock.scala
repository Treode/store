package com.treode.store

import scala.language.implicitConversions

import com.google.common.primitives.{Longs, UnsignedLongs}
import com.treode.async.misc.parseUnsignedLong
import com.treode.pickle.Picklers
import org.joda.time.{DateTime, Instant}

/** The timestamp of updates; sorts in reverse chronological order.
  *
  * The `time` is approximately the count of microseconds since the Unix epoch.  You may display
  * the timestamp as a formatted date string for the user, but be do not convert that back to a
  * TxClock and loose resolution. In particular, when converting the timestamp to a string for an
  * ETag, use the toString method to get its hexstring.  Do not use a formatted date string for the
  * ETag, as that may loose resolution and yield a value that works improperly as a condition time
  * on a subsequent write.
  */
class TxClock private [store] (val time: Long) extends AnyVal with Ordered [TxClock] {

  def + (n: Int): TxClock = new TxClock (time+n)

  def - (n: Int): TxClock = new TxClock (time-n)

  /** WARNING: `TxClock(this.toInstant) != this` */
  def toInstant: Instant =
    new Instant (time / 1000)

  /** WARNING: `TxClock(this.toDateTime) != this` */
  def toDateTime: DateTime =
    new DateTime (time / 1000)

  def byteSize: Int = Longs.BYTES

  def compare (that: TxClock): Int =
    UnsignedLongs.compare (this.time, that.time)

  /** The TxClock as `0x<hex>`. */
  override def toString = f"0x${time}%X"
}

object TxClock extends Ordering [TxClock] {

  val MinValue = new TxClock (0)

  val MaxValue = new TxClock (-1)

  def now = new TxClock (System.currentTimeMillis * 1000)

  implicit def apply (time: Instant): TxClock =
    new TxClock (time.getMillis * 1000)

  implicit def apply (time: DateTime): TxClock =
    new TxClock (time.getMillis * 1000)

  /** Parses decimal (no leading zero), octal (leading zero) or hexadecimal (leading `0x` or `#`).
    */
  def parse (s: String): Option [TxClock] =
    parseUnsignedLong (s) .map (new TxClock (_))

  def compare (x: TxClock, y: TxClock): Int =
    x compare y

  val pickler = {
    import Picklers._
    wrap (ulong) build (new TxClock (_)) inspect (_.time)
  }}
