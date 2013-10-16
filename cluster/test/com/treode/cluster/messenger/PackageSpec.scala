package com.treode.cluster.messenger

import com.esotericsoftware.kryo.io.{Input, Output}
import com.treode.cluster.Socket
import com.treode.cluster.concurrent.Callback
import org.scalamock.scalatest.MockFactory
import org.scalatest.FlatSpec

class PackageSpec extends FlatSpec with MockFactory {

  "The flush method" should "handle an empty buffer" in {
    val socket = mock [Socket]
    val output = new Output (256)
    val cb = mock [Callback [Unit]]
    (cb.apply _) .expects() .once()
    flush (socket, output, cb)
  }

  it should "flush an int" in {
    val socket = mock [Socket]
    val output = new Output (256)
    output.writeInt (0)
    (socket.write _) expects (where { case (buf, cb) =>
      expectResult ((0, 4)) (buf.position, buf.limit)
      buf.position (4)
      cb (4)
      true
    })
    val cb = mock [Callback [Unit]]
    (cb.apply _) .expects() .once()
    flush (socket, output, cb)
  }

  it should "loop to flush an int" in {
    val socket = mock [Socket]
    val output = new Output (256)
    output.writeInt (0)
    val cb = mock [Callback [Unit]]
    var pos = 0
    (socket.write _) .expects (where { case (buf, cb) =>
      expectResult ((pos, 4)) (buf.position, buf.limit)
      pos += 2
      buf.position (pos)
      cb (2)
      true
    }) .twice()
    (cb.apply _) .expects() .once()
    flush (socket, output, cb)
  }

  it should "handle socket close" in {
    val socket = mock [Socket]
    val output = new Output (256)
    output.writeInt (0)
    val cb = mock [Callback [Unit]]
    (socket.write _) .expects (where { case (buf, cb) =>
      cb (-1)
      true
    }) .once()
    (socket.close _) .expects() .once()
    flush (socket, output, cb)
  }

  "The fill method" should "handle a request for 0 bytes" in {
    val socket = mock [Socket]
    val input = new Input (256)
    val cb = mock [Callback [Unit]]
    (cb.apply _) .expects() .once()
    fill (socket, input, 0, cb)
  }

  it should "handle a request for bytes available at the beginning" in {
    val socket = mock [Socket]
    val input = new Input (256)
    input.setLimit (4)
    val cb = mock [Callback [Unit]]
    (cb.apply _) .expects() .once()
    fill (socket, input, 4, cb)
  }

  it should "fill needed bytes with an empty buffer" in {
    val socket = mock [Socket]
    val input = new Input (256)
    val cb = mock [Callback [Unit]]
    (socket.read _) .expects (where { case (buf, cb) =>
      expectResult ((0, 256)) (buf.position, buf.limit)
      buf.position (4)
      cb (4)
      true
    }) .once()
    (cb.apply _) .expects() .once()
    fill (socket, input, 4, cb)
  }

  it should "loop to fill needed bytes with an empty buffer" in {
    val socket = mock [Socket]
    val input = new Input (256)
    val cb = mock [Callback [Unit]]
    var pos = 0
    (socket.read _) .expects (where { case (buf, cb) =>
      expectResult ((pos, 256)) (buf.position, buf.limit)
      pos += 2
      buf.position (pos)
      cb (2)
      true
    }) .twice()
    (cb.apply _) .expects() .once()
    fill (socket, input, 4, cb)
  }

  it should "fill needed bytes with some at the beginning" in {
    val socket = mock [Socket]
    val input = new Input (256)
    input.setLimit (2)
    val cb = mock [Callback [Unit]]
    (socket.read _) .expects (where { case (buf, cb) =>
      expectResult ((2, 256)) (buf.position, buf.limit)
      buf.position (4)
      cb (2)
      true
    }) .once()
    (cb.apply _) .expects() .once()
    fill (socket, input, 4, cb)
  }

  it should "handle a request for bytes available in the middle" in {
    val socket = mock [Socket]
    val input = new Input (256)
    input.setPosition (4)
    input.setLimit (8)
    val cb = mock [Callback [Unit]]
    (cb.apply _) .expects() .once()
    fill (socket, input, 4, cb)
  }

  it should "fill needed bytes with some in the middle and space after" in {
    val socket = mock [Socket]
    val input = new Input (256)
    input.setPosition (4)
    input.setLimit (6)
    val cb = mock [Callback [Unit]]
    (socket.read _) .expects (where { case (buf, cb) =>
      expectResult ((6, 256)) (buf.position, buf.limit)
      buf.position (8)
      cb (2)
      true
    }) .once()
    (cb.apply _) .expects() .once()
    fill (socket, input, 4, cb)
  }

  it should "clear the buffer when position==limit" in {
    val socket = mock [Socket]
    val input = new Input (256)
    input.setPosition (6)
    input.setLimit (6)
    val cb = mock [Callback [Unit]]
    (socket.read _) .expects (where { case (buf, cb) =>
      expectResult ((0, 256)) (buf.position, buf.limit)
      buf.position (4)
      cb (4)
      true
    }) .once()
    (cb.apply _) .expects() .once()
    fill (socket, input, 4, cb)
  }

  it should "compact the buffer when bytes in the middle and space before" in {
    val socket = mock [Socket]
    val input = new Input (256)
    input.setPosition (250)
    input.setLimit (254)
    val cb = mock [Callback [Unit]]
    (socket.read _) .expects (where { case (buf, cb) =>
      expectResult ((4, 256)) (buf.position, buf.limit)
      buf.position (8)
      cb (4)
      true
    }) .once()
    (cb.apply _) .expects() .once()
    fill (socket, input, 8, cb)
  }

  it should "grow the buffer when it is empty but too small" in {
    val socket = mock [Socket]
    val input = new Input (256)
    val cb = mock [Callback [Unit]]
    (socket.read _) .expects (where { case (buf, cb) =>
      expectResult ((0, 1024)) (buf.position, buf.limit)
      buf.position (1024)
      cb (1024)
      true
    }) .once()
    (cb.apply _) .expects() .once()
    fill (socket, input, 1024, cb)
  }

  it should "grow the buffer when it is non-empty at the beginning and too small" in {
    val socket = mock [Socket]
    val input = new Input (256)
    input.setLimit (16)
    val cb = mock [Callback [Unit]]
    (socket.read _) .expects (where { case (buf, cb) =>
      expectResult ((16, 1024)) (buf.position, buf.limit)
      buf.position (1024)
      cb (1008)
      true
    }) .once()
    (cb.apply _) .expects() .once()
    fill (socket, input, 1024, cb)
  }

  it should "grow and compact the buffer when it is non-empty in the middle and too small" in {
    val socket = mock [Socket]
    val input = new Input (256)
    input.setPosition (64)
    input.setLimit (128)
    val cb = mock [Callback [Unit]]
    (socket.read _) .expects (where { case (buf, cb) =>
      expectResult ((64, 1024)) (buf.position, buf.limit)
      buf.position (1024)
      cb (960)
      true
    }) .once()
    (cb.apply _) .expects() .once()
    fill (socket, input, 1024, cb)
  }

  it should "handle socket close" in {
    val socket = mock [Socket]
    val input = new Input (256)
    val cb = mock [Callback [Unit]]
    (socket.read _) .expects (where { case (buf, cb) =>
      cb (-1)
      true
    }) .once()
    (socket.close _) .expects() .once()
    fill (socket, input, 4, cb)
  }}
