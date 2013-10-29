package com.treode.cluster

import com.esotericsoftware.kryo.io.{Input, Output}
import com.treode.concurrent.Callback

package object io {

  private class SocketFiller (socket: Socket, input: Input, length: Int, cb: Callback [Unit])
  extends Callback [Int] {

    private [this] val buffer = KryoPool.wrap (input)

    def fill() {
      if (length <= buffer.position - input.position)
        cb()
      else
        socket.read (buffer, this)
    }

    def pass (result: Int) {
      if (result < 0) {
        socket.close()
      } else {
        input.setLimit (input.limit + result)
        fill()
      }}

    def fail (t: Throwable) = cb.fail (t)
  }

  def fill (socket: Socket, input: Input, length: Int, cb: Callback [Unit]): Unit =
    Callback.guard (cb) {
      if (length <= input.limit - input.position) {
        cb()
      } else {
        KryoPool.ensure (input, length)
        new SocketFiller (socket, input, length, cb) .fill()
      }}

  private class SocketFlusher (socket: Socket, output: Output, cb: Callback [Unit])
  extends Callback [Int] {

    private [this] val buffer = KryoPool.wrap (output)

    def flush() {
      if (buffer.remaining == 0) {
        output.setPosition (0)
        cb()
      } else {
        socket.write (buffer, this)
      }}

    def pass (result: Int) {
      if (result < 0)
        socket.close()
      else
        flush()
    }

    def fail (t: Throwable) = cb.fail (t)
  }

  def flush (socket: Socket, output: Output, cb: Callback [Unit]): Unit =
    Callback.guard (cb) (new SocketFlusher (socket, output, cb) .flush())
}
