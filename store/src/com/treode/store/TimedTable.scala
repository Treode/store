package com.treode.store

import java.io.Closeable
import com.treode.async.Callback

trait TimedTable extends Closeable {

  /** Get the most recent value before the read time. */
  def get (key: Bytes, time: TxClock, cb: Callback [TimedCell])

  /** Set the value as of the write time. */
  def put (key: Bytes, time: TxClock, value: Option [Bytes], cb: Callback [Unit])

  /** Read the most recent value before the read time. */
  def read (key: Bytes, n: Int, reader: TimedReader) {
    val cb =  new Callback [TimedCell] {
      def pass (c: TimedCell) = reader.got (n, c)
      def fail (t: Throwable) = reader.fail (t)
    }
    cb.defer (get (key, reader.rt, cb))
  }

  /** Prepare a create. */
  def create (key: Bytes, n: Int, writer: TimedWriter) {
    val cb = new Callback [TimedCell] {
      def pass (c: TimedCell) {
        if (c == null)
          writer.advance (TxClock.zero)
        else if (c.value.isEmpty)
          writer.advance (c.time)
        else
          writer.conflict (n)
      }
      def fail (t: Throwable) = writer.fail (t)
    }
    cb.defer (get (key, TxClock.max, cb))
  }

  /** Prepare a hold, update or delete. */
  def prepare (key: Bytes, writer: TimedWriter) {
    val cb = new Callback [TimedCell] {
      def pass (c: TimedCell) {
        if (c == null)
          writer.advance (TxClock.zero)
        else if (c.time <= writer.ct)
          writer.advance (c.time)
        else
          writer.advance (c.time)
      }
      def fail (t: Throwable) = writer.fail (t)
    }
    cb.defer (get (key, TxClock.max, cb))
  }

  /** Commit a create. */
  def create (key: Bytes, value: Bytes, wt: TxClock, cb: Callback [Unit]): Unit =
    cb.defer (put (key, wt, Some (value), cb))

  /** Commit an update. */
  def update (key: Bytes, value: Bytes, wt: TxClock, cb: Callback [Unit]): Unit =
    cb.defer (put (key, wt, Some (value), cb))

  /** Commit a delete. */
  def delete (key: Bytes, wt: TxClock, cb: Callback [Unit]): Unit =
    put (key, wt, None, cb)
}
