package com.treode.cluster.io

import com.esotericsoftware.kryo.io.{Input, Output}
import com.treode.cluster.concurrent.Callback
import org.scalatest.FlatSpec

class PackageSpec extends FlatSpec with IoMockFactory {

  "The flush method for a file" should "handle an empty buffer" in {
    val file = new MockFile
    val output = new Output (256)
    val cb = mock [Callback [Unit]]
    (cb.apply _) .expects() .once()
    flush (file.file, output, 0, cb)
  }

  it should "flush an int" in {
    val file = new MockFile
    val output = new Output (256)
    output.writeInt (0)
    file.expectWrite { (buf, pos) =>
      expectResult ((0, 4)) (buf.position, buf.limit)
    }
    val cb = mock [Callback [Unit]]
    flush (file.file, output, 0, cb)
    (cb.apply _) .expects() .once()
    file.completeLast (4)
  }

  it should "loop to flush an int" in {
    val file = new MockFile
    val output = new Output (256)
    output.writeInt (0)
    val cb = mock [Callback [Unit]]
    var _pos = 0
    file.expectWrite { (buf, pos) =>
      expectResult ((_pos, 4)) (buf.position, buf.limit)
      expectResult (_pos) (pos)
      _pos += 2
    } .twice()
    flush (file.file, output, 0, cb)
    file.completeLast (2)
    (cb.apply _) .expects() .once()
    file.completeLast (2)
  }

  it should "handle file close" in {
    val file = new MockFile
    val output = new Output (256)
    output.writeInt (0)
    val cb = mock [Callback [Unit]]
    file.expectWrite ((buf, pos) => ())
    flush (file.file, output, 0, cb)
    (cb.fail _) .expects (*) .once()
    file.completeLast (-1)
  }

  "The fill method for a file" should "handle a request for 0 bytes" in {
    val file = new MockFile
    val input = new Input (256)
    val cb = mock [Callback [Unit]]
    (cb.apply _) .expects() .once()
    fill (file.file, 0, input, 0, cb)
  }

  it should "handle a request for bytes available at the beginning" in {
    val file = new MockFile
    val input = new Input (256)
    input.setLimit (4)
    val cb = mock [Callback [Unit]]
    (cb.apply _) .expects() .once()
    fill (file.file, 0, input, 4, cb)
  }

  it should "fill needed bytes with an empty buffer" in {
    val file = new MockFile
    val input = new Input (256)
    val cb = mock [Callback [Unit]]
    file.expectRead { (buf, pos) =>
      expectResult ((0, 256)) (buf.position, buf.limit)
    }
    fill (file.file, 0, input, 4, cb)
    (cb.apply _) .expects() .once()
    file.completeLast (4)
  }

  it should "loop to fill needed bytes with an empty buffer" in {
    val file = new MockFile
    val input = new Input (256)
    val cb = mock [Callback [Unit]]
    var _pos = 0
    file.expectRead { (buf, pos) =>
      expectResult ((_pos, 256)) (buf.position, buf.limit)
      expectResult (_pos) (pos)
      _pos += 2
    } .twice()
    fill (file.file, 0, input, 4, cb)
    file.completeLast (2)
    (cb.apply _) .expects() .once()
    file.completeLast (2)
  }

  it should "fill needed bytes with some at the beginning" in {
    val file = new MockFile
    val input = new Input (256)
    input.setLimit (2)
    val cb = mock [Callback [Unit]]
    file.expectRead { (buf, pos) =>
      expectResult ((2, 256)) (buf.position, buf.limit)
    }
    fill (file.file, 0, input, 4, cb)
    (cb.apply _) .expects() .once()
    file.completeLast (2)
  }

  it should "handle a request for bytes available in the middle" in {
    val file = new MockFile
    val input = new Input (256)
    input.setPosition (4)
    input.setLimit (8)
    val cb = mock [Callback [Unit]]
    (cb.apply _) .expects() .once()
    fill (file.file, 0, input, 4, cb)
  }

  it should "fill needed bytes with some in the middle and space after" in {
    val file = new MockFile
    val input = new Input (256)
    input.setPosition (4)
    input.setLimit (6)
    val cb = mock [Callback [Unit]]
    file.expectRead { (buf, pos) =>
      expectResult ((6, 256)) (buf.position, buf.limit)
    }
    fill (file.file, 0, input, 4, cb)
    (cb.apply _) .expects() .once()
    file.completeLast (2)
  }

  it should "clear the buffer when position==limit" in {
    val file = new MockFile
    val input = new Input (256)
    input.setPosition (6)
    input.setLimit (6)
    val cb = mock [Callback [Unit]]
    file.expectRead { (buf, pos) =>
      expectResult ((0, 256)) (buf.position, buf.limit)
    }
    fill (file.file, 0, input, 4, cb)
    (cb.apply _) .expects() .once()
    file.completeLast (4)
  }

  it should "compact the buffer when bytes in the middle and space before" in {
    val file = new MockFile
    val input = new Input (256)
    input.setPosition (250)
    input.setLimit (254)
    val cb = mock [Callback [Unit]]
    file.expectRead { (buf, pos) =>
      expectResult ((4, 256)) (buf.position, buf.limit)
    }
    fill (file.file, 0, input, 8, cb)
    (cb.apply _) .expects() .once()
    file.completeLast (4)
  }

  it should "grow the buffer when it is empty but too small" in {
    val file = new MockFile
    val input = new Input (256)
    val cb = mock [Callback [Unit]]
    file.expectRead { (buf, pos) =>
      expectResult ((0, 1024)) (buf.position, buf.limit)
    }
    fill (file.file, 0, input, 1024, cb)
    (cb.apply _) .expects() .once()
    file.completeLast (1024)
  }

  it should "grow the buffer when it is non-empty at the beginning and too small" in {
    val file = new MockFile
    val input = new Input (256)
    input.setLimit (16)
    val cb = mock [Callback [Unit]]
    file.expectRead { (buf, pos) =>
      expectResult ((16, 1024)) (buf.position, buf.limit)
    }
    fill (file.file, 0, input, 1024, cb)
    (cb.apply _) .expects() .once()
    file.completeLast (1008)
  }

  it should "grow and compact the buffer when it is non-empty in the middle and too small" in {
    val file = new MockFile
    val input = new Input (256)
    input.setPosition (64)
    input.setLimit (128)
    val cb = mock [Callback [Unit]]
    file.expectRead { (buf, pos) =>
      expectResult ((64, 1024)) (buf.position, buf.limit)
    }
    fill (file.file, 0, input, 1024, cb)
    (cb.apply _) .expects() .once()
    file.completeLast (960)
  }

  it should "handle file close" in {
    val file = new MockFile
    val input = new Input (256)
    val cb = mock [Callback [Unit]]
    file.expectRead ((buf, pos) => ())
    fill (file.file, 0, input, 4, cb)
    (cb.fail _) .expects (*) .once()
    file.completeLast(-1)
  }

  "The flush method for a socket" should "handle an empty buffer" in {
    val socket = new MockSocket
    val output = new Output (256)
    val cb = mock [Callback [Unit]]
    (cb.apply _) .expects() .once()
    flush (socket.socket, output, cb)
  }

  it should "flush an int" in {
    val socket = new MockSocket
    val output = new Output (256)
    output.writeInt (0)
    socket.expectWrite { buf =>
      expectResult ((0, 4)) (buf.position, buf.limit)
    }
    val cb = mock [Callback [Unit]]
    flush (socket.socket, output, cb)
    (cb.apply _) .expects() .once()
    socket.completeLast (4)
  }

  it should "loop to flush an int" in {
    val socket = new MockSocket
    val output = new Output (256)
    output.writeInt (0)
    val cb = mock [Callback [Unit]]
    var pos = 0
    socket.expectWrite { buf =>
      expectResult ((pos, 4)) (buf.position, buf.limit)
      pos += 2
    } .twice()
    flush (socket.socket, output, cb)
    socket.completeLast (2)
    (cb.apply _) .expects() .once()
    socket.completeLast (2)
  }

  it should "handle socket close" in {
    val socket = new MockSocket
    val output = new Output (256)
    output.writeInt (0)
    val cb = mock [Callback [Unit]]
    socket.expectWrite (_ => ())
    flush (socket.socket, output, cb)
    socket.expectClose()
    socket.completeLast (-1)
  }

  "The fill method for a socket" should "handle a request for 0 bytes" in {
    val socket = new MockSocket
    val input = new Input (256)
    val cb = mock [Callback [Unit]]
    (cb.apply _) .expects() .once()
    fill (socket.socket, input, 0, cb)
  }

  it should "handle a request for bytes available at the beginning" in {
    val socket = new MockSocket
    val input = new Input (256)
    input.setLimit (4)
    val cb = mock [Callback [Unit]]
    (cb.apply _) .expects() .once()
    fill (socket.socket, input, 4, cb)
  }

  it should "fill needed bytes with an empty buffer" in {
    val socket = new MockSocket
    val input = new Input (256)
    val cb = mock [Callback [Unit]]
    socket.expectRead { buf =>
      expectResult ((0, 256)) (buf.position, buf.limit)
    }
    fill (socket.socket, input, 4, cb)
    (cb.apply _) .expects() .once()
    socket.completeLast (4)
  }

  it should "loop to fill needed bytes with an empty buffer" in {
    val socket = new MockSocket
    val input = new Input (256)
    val cb = mock [Callback [Unit]]
    var pos = 0
    socket.expectRead { buf =>
      expectResult ((pos, 256)) (buf.position, buf.limit)
      pos += 2
    } .twice()
    fill (socket.socket, input, 4, cb)
    socket.completeLast (2)
    (cb.apply _) .expects() .once()
    socket.completeLast (2)
  }

  it should "fill needed bytes with some at the beginning" in {
    val socket = new MockSocket
    val input = new Input (256)
    input.setLimit (2)
    val cb = mock [Callback [Unit]]
    socket.expectRead { buf =>
      expectResult ((2, 256)) (buf.position, buf.limit)
    }
    fill (socket.socket, input, 4, cb)
    (cb.apply _) .expects() .once()
    socket.completeLast (2)
  }

  it should "handle a request for bytes available in the middle" in {
    val socket = new MockSocket
    val input = new Input (256)
    input.setPosition (4)
    input.setLimit (8)
    val cb = mock [Callback [Unit]]
    (cb.apply _) .expects() .once()
    fill (socket.socket, input, 4, cb)
  }

  it should "fill needed bytes with some in the middle and space after" in {
    val socket = new MockSocket
    val input = new Input (256)
    input.setPosition (4)
    input.setLimit (6)
    val cb = mock [Callback [Unit]]
    socket.expectRead { buf =>
      expectResult ((6, 256)) (buf.position, buf.limit)
    }
    fill (socket.socket, input, 4, cb)
    (cb.apply _) .expects() .once()
    socket.completeLast (2)
  }

  it should "clear the buffer when position==limit" in {
    val socket = new MockSocket
    val input = new Input (256)
    input.setPosition (6)
    input.setLimit (6)
    val cb = mock [Callback [Unit]]
    socket.expectRead { buf =>
      expectResult ((0, 256)) (buf.position, buf.limit)
    }
    fill (socket.socket, input, 4, cb)
    (cb.apply _) .expects() .once()
    socket.completeLast (4)
  }

  it should "compact the buffer when bytes in the middle and space before" in {
    val socket = new MockSocket
    val input = new Input (256)
    input.setPosition (250)
    input.setLimit (254)
    val cb = mock [Callback [Unit]]
    socket.expectRead { buf =>
      expectResult ((4, 256)) (buf.position, buf.limit)
    }
    fill (socket.socket, input, 8, cb)
    (cb.apply _) .expects() .once()
    socket.completeLast (4)
  }

  it should "grow the buffer when it is empty but too small" in {
    val socket = new MockSocket
    val input = new Input (256)
    val cb = mock [Callback [Unit]]
    socket.expectRead { buf =>
      expectResult ((0, 1024)) (buf.position, buf.limit)
    }
    fill (socket.socket, input, 1024, cb)
    (cb.apply _) .expects() .once()
    socket.completeLast (1024)
  }

  it should "grow the buffer when it is non-empty at the beginning and too small" in {
    val socket = new MockSocket
    val input = new Input (256)
    input.setLimit (16)
    val cb = mock [Callback [Unit]]
    socket.expectRead { buf =>
      expectResult ((16, 1024)) (buf.position, buf.limit)
    }
    fill (socket.socket, input, 1024, cb)
    (cb.apply _) .expects() .once()
    socket.completeLast (1008)
  }

  it should "grow and compact the buffer when it is non-empty in the middle and too small" in {
    val socket = new MockSocket
    val input = new Input (256)
    input.setPosition (64)
    input.setLimit (128)
    val cb = mock [Callback [Unit]]
    socket.expectRead { buf =>
      expectResult ((64, 1024)) (buf.position, buf.limit)
    }
    fill (socket.socket, input, 1024, cb)
    (cb.apply _) .expects() .once()
    socket.completeLast (960)
  }

  it should "handle socket close" in {
    val socket = new MockSocket
    val input = new Input (256)
    val cb = mock [Callback [Unit]]
    socket.expectRead (_ => ())
    fill (socket.socket, input, 4, cb)
    socket.expectClose()
    socket.completeLast(-1)
  }}
