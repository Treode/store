package com.treode.cluster

import scala.language.reflectiveCalls

import com.esotericsoftware.kryo.io.{Input, Output}
import com.treode.cluster.concurrent.Callback
import com.treode.cluster.misc.KryoPool

package object messenger {

  private [messenger] def fill (socket: Socket, input: Input, length: Int, cb: Callback [Unit]) {

    if (length <= input.limit - input.position) {
      cb()

    } else {

      KryoPool.ensure (input, length)

      val loop = new Callback [Int] {

        val buffer = KryoPool.wrap (input)

        def ensure() {
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
            ensure()
          }}

        def fail (t: Throwable) = cb.fail (t)
      }

      loop.ensure()
    }}

  private [messenger] def flush (socket: Socket, output: Output, cb: Callback [Unit]) {

    val loop = new Callback [Int] {

      val buffer = KryoPool.wrap (output)

      def flush() {
        if (buffer.remaining == 0) {
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

    loop.flush()
  }}
