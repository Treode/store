package com.treode.cluster

import java.nio.ByteBuffer
import java.util.concurrent.TimeUnit._
import scala.language.reflectiveCalls

import com.esotericsoftware.kryo.io.{Input, Output}
import com.treode.cluster.concurrent.Callback
import com.treode.cluster.misc.KryoPool

package object messenger {

  private def size (desired: Int): Int = {
    var i = 1
    while (i < desired)
      i <<= 1
    i
  }

  /** Ensure readable + writable >= length */
  private def ensure (input: Input, length: Int) {
    val capacity = input.getBuffer.length
    val position = input.position
    val limit = input.limit
    if (length <= capacity - position) {
      if (position == limit && position != 0) {
        input.setPosition (0)
        input.setLimit (0)
      }
      return
    }
    val available = limit - position
    if (length < capacity - available) {
      // Plenty of space in the buffer, compact and fill from the socket.
      val buffer = input.getBuffer
      System.arraycopy (buffer, input.position, buffer, 0, available)
      input.setPosition (0)
      input.setLimit (available)
    } else {
      // Not enough space in the buffer, grow and fill from the socket.
      val src = input.getBuffer
      val dst = new Array [Byte] (size (length))
      System.arraycopy (src, input.position, dst, 0, available)
      input.setBuffer (dst, 0, available)
   }}

  private [messenger] def fill (socket: Socket, input: Input, length: Int, cb: Callback [Unit]) {

    if (length <= input.limit - input.position) {
      cb()

    } else {

      ensure (input, length)

      val loop = new Callback [Int] {

        val buffer = ByteBuffer.wrap (input.getBuffer, input.limit, input.getBuffer.length - input.limit)

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

      val buffer = ByteBuffer.wrap (output.getBuffer, 0, output.position)

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
