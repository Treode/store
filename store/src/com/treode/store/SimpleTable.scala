package com.treode.store

import java.io.Closeable

import com.treode.concurrent.Callback

private trait SimpleTable extends Closeable {

  def get (key: Bytes, cb: Callback [Option [Bytes]])

  def put (key: Bytes, value: Bytes, cb: Callback [Unit])

  def del (key: Bytes, cb: Callback [Unit])
}
