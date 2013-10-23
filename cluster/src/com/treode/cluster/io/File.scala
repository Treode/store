package com.treode.cluster.io

import java.nio.channels.AsynchronousFileChannel
import java.nio.ByteBuffer

import com.treode.concurrent.Callback

/** Something that can be mocked in tests. */
class File (file: AsynchronousFileChannel) {

  /** For testing mocks only. */
  def this() = this (null)

  def write (buf: ByteBuffer, pos: Long, cb: Callback [Int]): Unit =
    Callback.guard (cb) (file.write (buf, pos, cb, Callback.IntHandler))

  def read (buf: ByteBuffer, pos: Long, cb: Callback [Int]): Unit =
    Callback.guard (cb) (file.read (buf, pos, cb, Callback.IntHandler))
}
