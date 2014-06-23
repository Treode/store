package com.treode.store

import com.treode.async.AsyncIterator

/** A window of time for scanning a table.
  *
  * TreodeDB retains past values for rows, and windows permit the scanning of just one recent
  * value for the row or its changes over time.  The case classes are nested in the
  * [[Window$ companion object]].
  *
  * <img src="/img/windows.png"/>
  */
sealed abstract class Window {

  def later: Bound [TxClock]

  def filter (iter: AsyncIterator [Cell]): AsyncIterator [Cell]
}

/** A window of time for scanning a table.
  *
  * TreodeDB retains past values for rows, and windows permit the scanning of just one recent
  * value for the row or its changes over time.
  *
  * <img src="/img/windows.png"/>
  */
object Window {

  /** Choose only the most recent value as of `later` so long as that was set after `earlier`. */
  case class Recent (later: Bound [TxClock], earlier: Bound [TxClock]) extends Window {

    def filter (iter: AsyncIterator [Cell]): AsyncIterator [Cell] = {
      var key = Option.empty [Bytes]
      iter.filter { cell =>
        if (later >* cell.time && earlier <* cell.time && (key.isEmpty || cell.key != key.get)) {
          key = Some (cell.key)
          true
        } else {
          false
        }}}}

  object Recent {

    def now = Recent (TxClock.now, true)

    def apply (later: TxClock, linc: Boolean, earlier: TxClock, einc: Boolean): Recent =
      Recent (Bound (later, linc), Bound (earlier, einc))

    def apply (later: TxClock, inclusive: Boolean): Recent =
      Recent (Bound (later, inclusive), Bound (TxClock.MinValue, true))
  }

  /** Choose all changes between `later` and `earlier`. */
  case class Between (later: Bound [TxClock], earlier: Bound [TxClock]) extends Window {

    def filter (iter: AsyncIterator [Cell]): AsyncIterator [Cell] =
      iter.filter (cell => later >* cell.time && earlier <* cell.time)
  }

  object Between {

    def apply (later: TxClock, linc: Boolean, earlier: TxClock, einc: Boolean): Between =
      Between (Bound (later, linc), Bound (earlier, einc))
  }

  /** Choose all changes between `later` and `earlier` and the most recent change as of 
    * `earlier`.
    */
  case class Through (later: Bound [TxClock], earlier: TxClock) extends Window {

    def filter (iter: AsyncIterator [Cell]): AsyncIterator [Cell] = {
      var key = Option.empty [Bytes]
      iter.filter { cell =>
        if (later >* cell.time && earlier < cell.time) {
          key = None
          true
        } else if (earlier >= cell.time && (key.isEmpty || cell.key != key.get)) {
          key = Some (cell.key)
          true
        } else {
          false
        }}}}

  object Through {

    def apply (later: TxClock, linc: Boolean, earlier: TxClock): Through =
      Through (Bound (later, linc), earlier)
  }

  val pickler = {
    import StorePicklers._
    tagged [Window] (
      0x1 -> wrap (tuple (bound (txClock), bound (txClock)))
          .build (v => new Recent (v._1, v._2))
          .inspect (v => (v.later, v.earlier)),
      0x2 -> wrap (tuple (bound (txClock), bound (txClock)))
          .build (v => (new Between (v._1, v._2)))
          .inspect (v => (v.later, v.earlier)),
      0x3 -> wrap (tuple (bound (txClock), txClock))
          .build (v => (new Through (v._1, v._2)))
          .inspect (v => (v.later, v.earlier)))
  }}
