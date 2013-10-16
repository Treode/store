package com.treode.cluster

import com.esotericsoftware.kryo.io.{Input, Output}
import com.treode.cluster.concurrent.Callback
import com.treode.cluster.io.Socket
import com.treode.cluster.misc.KryoPool

package object messenger {

  private class Filler (socket: Socket, input: Input, length: Int, cb: Callback [Unit])
  extends Callback [Int] {

    private [this] val buffer = KryoPool.wrap (input)

    def fill() {
      if (length <= buffer.position - input.position)
        cb()
      else
        socket.read (buffer, this)
    }

    def apply (result: Int) {
      if (result < 0) {
        socket.close()
      } else {
        input.setLimit (input.limit + result)
        fill()
      }}

    def fail (t: Throwable) = cb.fail (t)
  }

  private [messenger] def fill (socket: Socket, input: Input, length: Int, cb: Callback [Unit]) {
    if (length <= input.limit - input.position) {
      cb()
    } else {
      KryoPool.ensure (input, length)
      new Filler (socket, input, length, cb) .fill()
    }}

  private class Flusher (socket: Socket, output: Output, cb: Callback [Unit])
  extends Callback [Int] {

    private [this] val buffer = KryoPool.wrap (output)

    def flush() {
      if (buffer.remaining == 0) {
        output.setPosition (0)
        cb()
      } else {
        socket.write (buffer, this)
      }}

    def apply (result: Int) {
      if (result < 0)
        socket.close()
      else
        flush()
    }

    def fail (t: Throwable) = cb.fail (t)
  }

  private [messenger] def flush (socket: Socket, output: Output, cb: Callback [Unit]): Unit =
    new Flusher (socket, output, cb) .flush()
}
