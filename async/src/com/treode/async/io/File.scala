package com.treode.async.io

import java.nio.channels.AsynchronousFileChannel
import java.nio.ByteBuffer

import com.treode.async.Callback
import com.treode.pickle.Buffer

/** A file that has useful behavior (flush/fill) and that can be mocked. */
class File (file: AsynchronousFileChannel) {

  /** For testing mocks only. */
  def this() = this (null)

  private class FileFiller (input: Buffer, pos: Long, length: Int, cb: Callback [Unit])
  extends Callback [Int] {

    private [this] var bytebuf = input.buffer (input.writePos, input.writeableBytes)
    private [this] var _pos = pos

    def fill() {
      if (length <= input.readableBytes)
        cb()
      else {
        if (bytebuf.remaining == 0)
          bytebuf = input.buffer (input.writePos, input.writeableBytes)
        file.read (bytebuf, _pos, this, Callback.IntHandler)
      }}

    def pass (result: Int) {
      if (result < 0) {
        cb.fail (new Exception ("End of file reached."))
      } else {
        input.writePos = input.writePos + result
        _pos += result
        fill()
      }}

    def fail (t: Throwable) = cb.fail (t)
  }

  def fill (input: Buffer, pos: Long, length: Int, cb: Callback [Unit]): Unit =
    try {
      if (length <= input.readableBytes) {
        cb()
      } else {
        input.capacity (input.readPos + length)
        new FileFiller (input, pos, length, cb) .fill()
      }
    } catch {
      case t: Throwable => cb.fail (t)
    }

  private class FileFlusher (output: Buffer, pos: Long, cb: Callback [Unit])
  extends Callback [Int] {

    private [this] var bytebuf = output.buffer (output.readPos, output.readableBytes)
    private [this] var _pos = pos

    def flush() {
      if (output.readableBytes == 0) {
        //buffer.release()
        cb()
      } else {
        if (bytebuf.remaining == 0)
          bytebuf = output.buffer (output.readPos, output.readableBytes)
        file.write (bytebuf, _pos, this, Callback.IntHandler)
      }}

    def pass (result: Int) {
      if (result < 0) {
        cb.fail (new Exception ("File write failed."))
      } else {
        output.readPos = output.readPos + result
        _pos += result
        flush()
      }}

    def fail (t: Throwable) = cb.fail (t)
  }


  def flush (output: Buffer, pos: Long, cb: Callback [Unit]): Unit =
    try {
      new FileFlusher (output, pos, cb) .flush()
    } catch {
      case t: Throwable => cb.fail (t)
    }
}
