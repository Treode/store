package com.treode.cluster.io

import java.nio.ByteBuffer

import com.esotericsoftware.kryo.io.Input
import com.treode.cluster.concurrent.Callback
import org.scalamock.CallHandler2
import org.scalamock.scalatest.MockFactory
import org.scalatest.Suite

trait IoMockFactory extends MockFactory {
  this: Suite =>

  class MockSocket {

    val socket = mock [Socket]

    private var callback: Callback [Int] = null

    private def wrap (buf: ByteBuffer, cb: Callback [Int]) {
      callback = new Callback [Int] {
        def apply (v: Int) {
          if (v > 0)
            buf.position (buf.position + v)
          cb (v)
        }
        def fail (t: Throwable) = cb.fail (t)
      }}

    def completeLast (v: Int) = callback (v)

    def failLast (t: Throwable) = callback.fail (t)

    def expectRead (f: Input => Any): CallHandler2 [ByteBuffer, Callback [Int], Unit] =
      (socket.read _) .expects (where { case (buf, cb) =>
        wrap (buf, cb)
        f (new Input (buf.array, buf.position, buf.limit - buf.position))
        true
      })

    def expectWrite (f: Input => Any): CallHandler2 [ByteBuffer, Callback [Int], Unit] =
      (socket.write _) .expects (where { case (buf, cb) =>
        wrap (buf, cb)
        f (new Input (buf.array, buf.position, buf.limit - buf.position))
        true
      })

    def expectClose() =
      (socket.close _) .expects()
  }}
