package com.treode.store.local

import com.treode.store.Bytes

trait Table {

  def read (key: Bytes, n: Int, reader: Reader)

  /** Prepare a create. */
  def create (key: Bytes, value: Bytes, n: Int, writer: Writer)

  /** Prepare a hold. */
  def hold (key: Bytes, writer: Writer)

  /** Prepare an update. */
  def update (key: Bytes, value: Bytes, writer: Writer)

  /** Prepare a delete. */
  def delete (key: Bytes, writer: Writer)
}
