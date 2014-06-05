package com.treode.store

import com.treode.async.AsyncIterator

sealed abstract class Window {

  def filter (iter: AsyncIterator [Cell]): AsyncIterator [Cell]
}

object Window {

  case class Recent (time: Bound [TxClock]) extends Window {

    def filter (iter: AsyncIterator [Cell]): AsyncIterator [Cell] = {
      var key = Option.empty [Bytes]
      iter.filter { cell =>
        if (time >* cell.time && (key.isEmpty || cell.key != key.get)) {
          key = Some (cell.key)
          true
        } else {
          false
        }}}}

  object Recent {

    def apply (time: TxClock, inclusive: Boolean): Recent =
      Recent (Bound (time, inclusive))
  }

  case class Between (later: Bound [TxClock], earlier: Bound [TxClock]) extends Window {

    def filter (iter: AsyncIterator [Cell]): AsyncIterator [Cell] =
      iter.filter (cell => later >* cell.time && earlier <* cell.time)
  }

  object Between {

    def apply (later: TxClock, linc: Boolean, earlier: TxClock, einc: Boolean): Between =
      Between (Bound (later, linc), Bound (earlier, einc))
  }

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

    def apply (later: TxClock, inclusive: Boolean, earlier: TxClock): Through =
      Through (Bound (later, inclusive), earlier)
  }

  val pickler = {
    import StorePicklers._
    tagged [Window] (
      0x1 -> wrap (bound (txClock)) .build (new Recent (_)) .inspect (_.time),
      0x2 -> wrap (tuple (bound (txClock), bound (txClock)))
          .build (v => (new Between (v._1, v._2)))
          .inspect (v => (v.later, v.earlier)),
      0x3 -> wrap (tuple (bound (txClock), txClock))
          .build (v => (new Through (v._1, v._2)))
          .inspect (v => (v.later, v.earlier)))
  }}
