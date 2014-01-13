package com.treode.async.io

import java.util.concurrent.ConcurrentHashMap
import com.treode.async.Callback
import com.treode.buffer.PagedBuffer
import com.treode.pickle.{Pickler, unpickle, pickle}

class Framer [ID, T] (strategy: Framer.Strategy [ID]) {
  import Framer.Frame

  private val headerByteSize = strategy.idByteSize + 4

  private val framers = new ConcurrentHashMap [ID, Frame [T]]

  def register [P] (p: Pickler [P], id: ID, read: P => T) {
    val h = Frame (p, id, read)
    val h0 = framers.putIfAbsent (id, h)
    require (h0 == null, s"$id already registered")
  }

  def register [P] (p: Pickler [P], read: P => T): ID = {
    var id = strategy.newEphemeralId
    while (framers.putIfAbsent (id, Frame (p, id, read)) != null)
      id = strategy.newEphemeralId
    id
  }

  def unregister [P] (id: ID): Unit =
    framers.remove (id)

  def send [P] (p: Pickler [P], id: ID, v: P, handle: T => Any) {
    val buf = new PagedBuffer (12)
    pickle (p, v, buf)
    val frm = framers.get (id)
    require (frm != null || strategy.isEphemeralId (id), s"$id not recognized.")
    if (frm != null) {
      val x = frm.read (buf)
      require (buf.readPos == buf.writePos, s"Frame overflow on $id.")
      handle (x)
    }}

  def read (pos: Long, file: File, buf: PagedBuffer, handle: T => Any, cb: Callback [Unit]) {

    def bodyRead (len: Int, id: ID) = new Callback [Unit] {

      def pass (v: Unit) {
        val end = buf.readPos + len
        try {
          val frm = framers.get (id)
          require (frm != null || strategy.isEphemeralId (id), s"$id not recognized.")
          if (frm != null) {
            val x = frm.read (buf)
            require (buf.readPos == end, s"Frame overflow on $id.")
            handle (x)
          } else {
            buf.readPos = end
          }
        } catch {
          case e: Throwable =>
            buf.readPos = end
            cb.fail (e)
        }
        cb()
      }

      def fail (t: Throwable) = cb.fail (t)
    }

    def headerRead() = new Callback [Unit] {

      def pass (v: Unit) {
        try {
          val len = buf.readInt()
          val id = strategy.readId (buf)
          file.fill (buf, pos + headerByteSize, len, bodyRead (len, id))
        } catch {
          case e: Throwable => cb.fail (e)
        }}

      def fail (t: Throwable) = cb.fail (t)
    }

    file.fill (buf, pos, headerByteSize, headerRead())
  }

  def read (socket: Socket, buf: PagedBuffer, handle: T => Any, cb: Callback [Unit]) {

    def bodyRead (len: Int, id: ID) = new Callback [Unit] {

      def pass (v: Unit) {
        val end = buf.readPos + len
        try {
          val frm = framers.get (id)
          require (frm != null || strategy.isEphemeralId (id), s"$id not recognized.")
          if (frm != null) {
            val x = frm.read (buf)
            require (buf.readPos == end, s"Frame overflow on $id.")
            handle (x)
          } else {
            buf.readPos = end
          }
        } catch {
          case e: Throwable =>
            buf.readPos = end
            cb.fail (e)
        }
        cb()
      }

      def fail (t: Throwable) = cb.fail (t)
    }

    def headerRead() = new Callback [Unit] {

      def pass (v: Unit) {
        try {
          val len = buf.readInt()
          val id = strategy.readId (buf)
          socket.fill (buf, len, bodyRead (len, id))
        } catch {
          case e: Throwable => cb.fail (e)
        }}

      def fail (t: Throwable) = cb.fail (t)
    }

    socket.fill (buf, headerByteSize, headerRead())
  }}

object Framer {

  private trait Frame [T] {

    def read (buf: PagedBuffer): T
  }

  private object Frame {

    def apply [ID, P, T] (p: Pickler [P], id: ID, reader: P => T): Frame [T] =
      new Frame [T] {
        def read (buf: PagedBuffer): T = reader (unpickle (p, buf))
        override def toString = s"Framer($id)"
    }}

  trait Strategy [ID] {

    def idByteSize: Int
    def newEphemeralId: ID
    def isEphemeralId (id: ID): Boolean
    def readId (buf: PagedBuffer): ID
    def writeId (id: ID, buf: PagedBuffer)

    def write [P] (p: Pickler [P], id: ID, v: P, buf: PagedBuffer) {
      val header = buf.writePos
      buf.writeInt (0)
      writeId (id, buf)
      val body = buf.writePos
      pickle (p, v, buf)
      val end = buf.writePos
      buf.writePos = header
      buf.writeInt (end - body)
      buf.writePos = end
    }}}
