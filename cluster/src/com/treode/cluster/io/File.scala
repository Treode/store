package com.treode.cluster.io

import java.nio.channels.AsynchronousFileChannel
import java.nio.ByteBuffer

import com.esotericsoftware.kryo.io.{Input, Output}
import com.treode.concurrent.Callback

/** A file that has useful behavior (flush/fill) and that can be mocked. */
class File (file: AsynchronousFileChannel) {

  /** For testing mocks only. */
  def this() = this (null)

  private class FileFiller (pos: Long, input: Input, length: Int, cb: Callback [Unit])
  extends Callback [Int] {

    private [this] val buffer = KryoPool.wrap (input)
    private [this] var _pos = pos

    def fill() {
      if (length <= buffer.position - input.position)
        cb()
      else
        file.read (buffer, _pos, this, Callback.IntHandler)
    }

    def pass (result: Int) {
      if (result < 0) {
        cb.fail (new Exception ("End of file reached."))
      } else {
        input.setLimit (input.limit + result)
        _pos += result
        fill()
      }}

    def fail (t: Throwable) = cb.fail (t)
  }

  def fill (pos: Long, input: Input, length: Int, cb: Callback [Unit]): Unit =
    Callback.guard (cb) {
      if (length <= input.limit - input.position) {
        cb()
      } else {
        KryoPool.ensure (input, length)
        new FileFiller (pos, input, length, cb) .fill()
      }}

  private class FileFlusher (output: Output, pos: Long, cb: Callback [Unit])
  extends Callback [Int] {

    private [this] val buffer = KryoPool.wrap (output)
    private [this] var _pos = pos

    def flush() {
      if (buffer.remaining == 0) {
        cb()
      } else {
        file.write (buffer, _pos, this, Callback.IntHandler)
      }}

    def pass (result: Int) {
      if (result < 0) {
        cb.fail (new Exception ("File write failed."))
      } else {
        _pos += result
        flush()
      }}

    def fail (t: Throwable) = cb.fail (t)
  }

  def flush (output: Output, pos: Long, cb: Callback [Unit]): Unit =
    Callback.guard (cb) (new FileFlusher (output, pos, cb) .flush())
}
