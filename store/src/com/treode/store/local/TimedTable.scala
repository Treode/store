package com.treode.store.local

import java.io.Closeable

import com.treode.store.Bytes

trait TimedTable extends Closeable {

  def read (key: Bytes, n: Int, reader: TimedReader)

  /** Prepare a create. */
  def create (key: Bytes, value: Bytes, n: Int, writer: TimedWriter)

  /** Prepare a hold. */
  def hold (key: Bytes, writer: TimedWriter)

  /** Prepare an update. */
  def update (key: Bytes, value: Bytes, writer: TimedWriter)

  /** Prepare a delete. */
  def delete (key: Bytes, writer: TimedWriter)
}
