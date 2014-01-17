package com.treode.async.io

import scala.util.Random

import com.treode.async.Callback
import com.treode.buffer.PagedBuffer
import org.scalamock.scalatest.MockFactory
import org.scalatest.{FlatSpec, PropSpec, Specs}
import org.scalatest.prop.PropertyChecks

class SocketSpec extends Specs (SocketBehaviors, SocketProperties)

object SocketBehaviors extends FlatSpec with MockFactory {

  def mkSocket = {
    val async = new AsyncSocketMock
    val socket = new Socket (async, ExecutorMock)
    val buffer = PagedBuffer (5)
    (async, socket, buffer)
  }

  "The flush method" should "handle an empty buffer" in {
    val (async, socket, buffer) = mkSocket
    val cb = mock [Callback [Unit]]
    (cb.apply _) .expects() .once()
    socket.flush (buffer, cb)
  }

  it should "flush an int" in {
    val (async, socket, buffer) = mkSocket
    buffer.writeInt (0)
    async.expectWrite (0, 4)
    val cb = mock [Callback [Unit]]
    socket.flush (buffer, cb)
    (cb.apply _) .expects() .once()
    async.completeLast (4)
  }

  it should "loop to flush an int" in {
    val (async, socket, output) = mkSocket
    output.writeInt (0)
    val cb = mock [Callback [Unit]]
    var _pos = 0
    async.expectWrite (0, 4)
    async.expectWrite (2, 4)
    socket.flush (output, cb)
    async.completeLast (2)
    (cb.apply _) .expects() .once()
    async.completeLast (2)
  }

  it should "handle socket close" in {
    val (async, socket, output) = mkSocket
    output.writeInt (0)
    val cb = mock [Callback [Unit]]
    async.expectWrite (0, 4)
    socket.flush (output, cb)
    (cb.fail _) .expects (*) .once()
    async.completeLast (-1)
  }

  "The fill method for a socket" should "handle a request for 0 bytes" in {
    val (async, socket, input) = mkSocket
    val cb = mock [Callback [Unit]]
    (cb.apply _) .expects() .once()
    socket.fill (input, 0, cb)
  }

  it should "handle a request for bytes available at the beginning" in {
    val (async, socket, input) = mkSocket
    input.writePos = 4
    val cb = mock [Callback [Unit]]
    (cb.apply _) .expects() .once()
    socket.fill (input, 4, cb)
  }

  it should "fill needed bytes with an empty buffer" in {
    val (async, socket, input) = mkSocket
    val cb = mock [Callback [Unit]]
    async.expectRead (0, 32)
    socket.fill (input, 4, cb)
    (cb.apply _) .expects() .once()
    async.completeLast (4)
  }

  it should "loop to fill needed bytes within a page" in {
    val (async, socket, input) = mkSocket
    val cb = mock [Callback [Unit]]
    async.expectRead (0, 32)
    async.expectRead (2, 32)
    socket.fill (input, 4, cb)
    async.completeLast (2)
    (cb.apply _) .expects() .once()
    async.completeLast (2)
  }

  it should "fill needed bytes with some at the beginning" in {
    val (async, socket, input) = mkSocket
    input.writePos = 2
    val cb = mock [Callback [Unit]]
    async.expectRead (2, 32)
    socket.fill (input, 4, cb)
    (cb.apply _) .expects() .once()
    async.completeLast (2)
  }

  it should "handle a request for bytes available in the middle" in {
    val (async, socket, input) = mkSocket
    input.writePos = 4
    input.readPos = 4
    val cb = mock [Callback [Unit]]
    async.expectRead (4, 32)
    (cb.apply _) .expects() .once()
    socket.fill (input, 4, cb)
    async.completeLast (4)
  }

  it should "fill needed bytes with some in the middle and space after" in {
    val (async, socket, input) = mkSocket
    input.writePos = 6
    input.readPos = 4
    val cb = mock [Callback [Unit]]
    async.expectRead (6, 32)
    socket.fill (input, 4, cb)
    (cb.apply _) .expects() .once()
    async.completeLast (2)
  }

  it should "repeat to fill needed bytes across pages" in {
    val (async, socket, input) = mkSocket
    input.writePos = 30
    input.readPos = 26
    val cb = mock [Callback [Unit]]
    async.expectRead (30, 32)
    async.expectRead (0, 32)
    socket.fill (input, 8, cb)
    async.completeLast (2)
    (cb.apply _) .expects() .once()
    async.completeLast (6)
  }

  it should "handle socket close" in {
    val (async, socket, input) = mkSocket
    val cb = mock [Callback [Unit]]
    async.expectRead (0, 32)
    socket.fill (input, 4, cb)
    (cb.fail _) .expects (*) .once()
    async.completeLast (-1)
  }}

object SocketProperties extends PropSpec with PropertyChecks {

  property ("It should work") {
    forAll ("seed") { seed: Int =>
      val random = new Random (seed)
      val socket = new Socket (new AsyncSocketStub (random), ExecutorMock)
      val data = Array.fill (100) (random.nextInt)
      val buffer = PagedBuffer (5)
      for (i <- data)
        buffer.writeVarInt (i)
      val length = buffer.writePos
      socket.flush (buffer, Callback.ignore)
      buffer.clear()
      socket.fill (buffer, length, Callback.ignore)
      for (i <- data)
        expectResult (i) (buffer.readVarInt())
    }}}
