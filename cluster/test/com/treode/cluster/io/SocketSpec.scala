package com.treode.cluster.io

import com.esotericsoftware.kryo.io.{Input, Output}
import com.treode.concurrent.Callback
import org.scalamock.scalatest.MockFactory
import org.scalatest.FlatSpec

class SocketSpec extends FlatSpec with MockFactory {

  "The flush method for a socket" should "handle an empty buffer" in {
    val stub = new AsyncSocketStub
    val socket = new Socket (stub)
    val output = new Output (256)
    val cb = mock [Callback [Unit]]
    (cb.apply _) .expects() .once()
    socket.flush (output, cb)
  }

  it should "flush an int" in {
    val stub = new AsyncSocketStub
    val socket = new Socket (stub)
    val output = new Output (256)
    output.writeInt (0)
    stub.expectWrite (0, 4)
    val cb = mock [Callback [Unit]]
    socket.flush (output, cb)
    (cb.apply _) .expects() .once()
    stub.completeLast (4)
  }

  it should "loop to flush an int" in {
    val stub = new AsyncSocketStub
    val socket = new Socket (stub)
    val output = new Output (256)
    output.writeInt (0)
    val cb = mock [Callback [Unit]]
    stub.expectWrite (0, 4)
    stub.expectWrite (2, 4)
    socket.flush (output, cb)
    stub.completeLast (2)
    (cb.apply _) .expects() .once()
    stub.completeLast (2)
  }

  it should "handle socket close" in {
    val stub = new AsyncSocketStub
    val socket = new Socket (stub)
    val output = new Output (256)
    output.writeInt (0)
    val cb = mock [Callback [Unit]]
    stub.expectWrite (0, 4)
    socket.flush (output, cb)
    stub.expectClose()
    stub.completeLast (-1)
  }

  "The fill method for a socket" should "handle a request for 0 bytes" in {
    val stub = new AsyncSocketStub
    val socket = new Socket (stub)
    val input = new Input (256)
    val cb = mock [Callback [Unit]]
    (cb.apply _) .expects() .once()
    socket.fill (input, 0, cb)
  }

  it should "handle a request for bytes available at the beginning" in {
    val stub = new AsyncSocketStub
    val socket = new Socket (stub)
    val input = new Input (256)
    input.setLimit (4)
    val cb = mock [Callback [Unit]]
    (cb.apply _) .expects() .once()
    socket.fill (input, 4, cb)
  }

  it should "fill needed bytes with an empty buffer" in {
    val stub = new AsyncSocketStub
    val socket = new Socket (stub)
    val input = new Input (256)
    val cb = mock [Callback [Unit]]
    stub.expectRead (0, 256)
    socket.fill (input, 4, cb)
    (cb.apply _) .expects() .once()
    stub.completeLast (4)
  }

  it should "loop to fill needed bytes with an empty buffer" in {
    val stub = new AsyncSocketStub
    val socket = new Socket (stub)
    val input = new Input (256)
    val cb = mock [Callback [Unit]]
    stub.expectRead (0, 256)
    stub.expectRead (2, 256)
    socket.fill (input, 4, cb)
    stub.completeLast (2)
    (cb.apply _) .expects() .once()
    stub.completeLast (2)
  }

  it should "fill needed bytes with some at the beginning" in {
    val stub = new AsyncSocketStub
    val socket = new Socket (stub)
    val input = new Input (256)
    input.setLimit (2)
    val cb = mock [Callback [Unit]]
    stub.expectRead (2, 256)
    socket.fill (input, 4, cb)
    (cb.apply _) .expects() .once()
    stub.completeLast (2)
  }

  it should "handle a request for bytes available in the middle" in {
    val stub = new AsyncSocketStub
    val socket = new Socket (stub)
    val input = new Input (256)
    input.setPosition (4)
    input.setLimit (8)
    val cb = mock [Callback [Unit]]
    (cb.apply _) .expects() .once()
    socket.fill (input, 4, cb)
  }

  it should "fill needed bytes with some in the middle and space after" in {
    val stub = new AsyncSocketStub
    val socket = new Socket (stub)
    val input = new Input (256)
    input.setPosition (4)
    input.setLimit (6)
    val cb = mock [Callback [Unit]]
    stub.expectRead (6, 256)
    socket.fill (input, 4, cb)
    (cb.apply _) .expects() .once()
    stub.completeLast (2)
  }

  it should "clear the buffer when position==limit" in {
    val stub = new AsyncSocketStub
    val socket = new Socket (stub)
    val input = new Input (256)
    input.setPosition (6)
    input.setLimit (6)
    val cb = mock [Callback [Unit]]
    stub.expectRead (0, 256)
    socket.fill (input, 4, cb)
    (cb.apply _) .expects() .once()
    stub.completeLast (4)
  }

  it should "compact the buffer when bytes in the middle and space before" in {
    val stub = new AsyncSocketStub
    val socket = new Socket (stub)
    val input = new Input (256)
    input.setPosition (250)
    input.setLimit (254)
    val cb = mock [Callback [Unit]]
    stub.expectRead (4, 256)
    socket.fill (input, 8, cb)
    (cb.apply _) .expects() .once()
    stub.completeLast (4)
  }

  it should "grow the buffer when it is empty but too small" in {
    val stub = new AsyncSocketStub
    val socket = new Socket (stub)
    val input = new Input (256)
    val cb = mock [Callback [Unit]]
    stub.expectRead (0, 1024)
    socket.fill (input, 1024, cb)
    (cb.apply _) .expects() .once()
    stub.completeLast (1024)
  }

  it should "grow the buffer when it is non-empty at the beginning and too small" in {
    val stub = new AsyncSocketStub
    val socket = new Socket (stub)
    val input = new Input (256)
    input.setLimit (16)
    val cb = mock [Callback [Unit]]
    stub.expectRead (16, 1024)
    socket.fill (input, 1024, cb)
    (cb.apply _) .expects() .once()
    stub.completeLast (1008)
  }

  it should "grow and compact the buffer when it is non-empty in the middle and too small" in {
    val stub = new AsyncSocketStub
    val socket = new Socket (stub)
    val input = new Input (256)
    input.setPosition (64)
    input.setLimit (128)
    val cb = mock [Callback [Unit]]
    stub.expectRead (64, 1024)
    socket.fill (input, 1024, cb)
    (cb.apply _) .expects() .once()
    stub.completeLast (960)
  }

  it should "handle socket close" in {
    val stub = new AsyncSocketStub
    val socket = new Socket (stub)
    val input = new Input (256)
    val cb = mock [Callback [Unit]]
    stub.expectRead (0, 256)
    socket.fill (input, 4, cb)
    stub.expectClose()
    stub.completeLast(-1)
  }}
