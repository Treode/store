package com.treode.async.io

import java.util.concurrent.ConcurrentHashMap
import com.treode.async.Callback
import com.treode.buffer.PagedBuffer
import com.treode.pickle.{Pickler, pickle, size, unpickle}

class Framer [ID, H, T] (strategy: Framer.Strategy [ID, H]) {
  import Framer.Unpickler

  type FileFrame = (Int, H, Option [T])
  type SocketFrame = (H, Option [T])

  private val framers = new ConcurrentHashMap [ID, Unpickler [T]]

  def register [P] (p: Pickler [P], id: ID) (read: P => T) {
    val h = Unpickler (p, id, read)
    val h0 = framers.putIfAbsent (id, h)
    require (h0 == null, s"$id already registered")
  }

  def register [P] (p: Pickler [P]) (read: P => T): ID = {
    var id = strategy.newEphemeralId
    while (framers.putIfAbsent (id, Unpickler (p, id, read)) != null)
      id = strategy.newEphemeralId
    id
  }

  def unregister [P] (id: ID): Unit =
    framers.remove (id)

  private def read (len: Int, buf: PagedBuffer): SocketFrame = {
    val end = buf.readPos + len
    val (id, hdr) = strategy.readHeader (buf)
    if (id.isEmpty) {
      buf.readPos = end
      buf.discard (buf.readPos)
      return (hdr, None)
    }
    val frm = framers.get (id.get)
    if (frm != null) {
      val v = frm.read (buf)
      if (buf.readPos == end) {
        buf.discard (buf.readPos)
        (hdr, Some (v))
      } else {
        buf.readPos = end
        buf.discard (buf.readPos)
        throw new FrameBoundsException
      }
    } else {
      buf.readPos = end
      buf.discard (buf.readPos)
      if (!strategy.isEphemeralId (id.get))
        throw new FrameNotRecognizedException (id)
      (hdr, None)
    }}

  def read [P] (p: Pickler [P], hdr: H, v: P): SocketFrame = {
    val buf = new PagedBuffer (12)
    strategy.writeHeader (hdr, buf)
    pickle (p, v, buf)
    read (buf.writePos, buf)
  }

  def read (buf: PagedBuffer): SocketFrame = {
    val len = buf.readInt()
    read (len, buf)
  }

  def read (file: File, pos: Long, buf: PagedBuffer, cb: Callback [FileFrame]) {

    def bodyRead (len: Int, buf: PagedBuffer, cb: Callback [FileFrame]) =

      new Callback [Unit] {

        def pass (v: Unit) {
          val (hdr, v) = try {
            read (len, buf)
          } catch {
            case e: Throwable =>
              cb.fail (e)
              return
          }
          cb (len, hdr, v)
        }

        def fail (t: Throwable) = cb.fail (t)
      }

    def headerRead() = new Callback [Unit] {

      def pass (v: Unit) {
        try {
          val len = buf.readInt()
          file.fill (buf, pos + 4, len, bodyRead (len, buf, cb))
        } catch {
          case e: Throwable => cb.fail (e)
        }}

      def fail (t: Throwable) = cb.fail (t)
    }

    file.fill (buf, pos, 4, headerRead())
  }

  def read (socket: Socket, buf: PagedBuffer, cb: Callback [SocketFrame]) {

    def bodyRead (len: Int, buf: PagedBuffer, cb: Callback [SocketFrame]) =

      new Callback [Unit] {

        def pass (v: Unit) {
          val (hdr, v) = try {
            read (len, buf)
          } catch {
            case e: Throwable =>
              cb.fail (e)
              return
          }
          cb (hdr, v)
        }

        def fail (t: Throwable) = cb.fail (t)
      }

    def headerRead() = new Callback [Unit] {

      def pass (v: Unit) {
        try {
          val len = buf.readInt()
          socket.fill (buf, len, bodyRead (len, buf, cb))
        } catch {
          case e: Throwable => cb.fail (e)
        }}

      def fail (t: Throwable) = cb.fail (t)
    }

    socket.fill (buf, 4, headerRead())
  }}

object Framer {

  private trait Unpickler [T] {

    def read (buf: PagedBuffer): T
  }

  private object Unpickler {

    def apply [ID, P, T] (p: Pickler [P], id: ID, reader: P => T): Unpickler [T] =
      new Unpickler [T] {
        def read (buf: PagedBuffer): T = reader (unpickle (p, buf))
        override def toString = s"Unpickler($id)"
    }}

  trait Strategy [ID, H] {

    def newEphemeralId: ID
    def isEphemeralId (id: ID): Boolean
    def readHeader (buf: PagedBuffer): (Option [ID], H)
    def writeHeader (hdr: H, buf: PagedBuffer)

    def write (hdr: H, buf: PagedBuffer) {
      val preamble = buf.writePos
      buf.writePos = preamble + 4
      writeHeader (hdr, buf)
      val end = buf.writePos
      buf.writePos = preamble
      buf.writeInt (end - preamble - 4)
      buf.writePos = end
    }

    def write [P] (p: Pickler [P], hdr: H, v: P, buf: PagedBuffer) {
      val preamble = buf.writePos
      buf.writePos = preamble + 4
      writeHeader (hdr, buf)
      pickle (p, v, buf)
      val end = buf.writePos
      buf.writePos = preamble
      buf.writeInt (end - preamble - 4)
      buf.writePos = end
    }}}
