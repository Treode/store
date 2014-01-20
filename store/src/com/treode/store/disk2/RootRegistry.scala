package com.treode.store.disk2

import java.util.ArrayList
import scala.collection.JavaConversions._

import com.treode.async.{Callback, callback, delay}
import com.treode.async.io.Framer
import com.treode.buffer.PagedBuffer
import com.treode.pickle.{Pickler, Picklers}

class RootRegistry (disks: DisksKit, pages: PageDispatcher) {

  private val checkpoints = new ArrayList [Callback [PickledRoot] => Unit]

  private val recoveries = new Framer [TypeId, TypeId, Any] (RootRegistry.framer)

  def checkpoint [B] (pblk: Pickler [B], id: TypeId) (f: Callback [B] => Any): Unit =
    synchronized {
      checkpoints.add { cb =>
        f (callback (cb) { root =>
          PickledRoot (pblk, id, root)
        })
      }}

  def recover [B] (pblk: Pickler [B], id: TypeId) (f: B => Any): Unit =
    recoveries.register (pblk, id) (f)

  def checkpoint (gen: Int, cb: Callback [RootRegistry.Meta]) = synchronized {
    val count = checkpoints.size

    val rootsPageWritten = callback (cb) { pos: Position =>
      RootRegistry.Meta (count, pos)
    }

    val rootsWritten = Callback.collect (count, delay (cb) { roots: Seq [PickledRoot] =>
      val byteSize = roots .map (_.byteSize) .sum
      val write = {buf: PagedBuffer => roots foreach (_.write (buf))}
      pages.write (byteSize, write, rootsPageWritten)
    })

    for (cp <- checkpoints)
      cp (rootsWritten)
  }

  def recover (meta: RootRegistry.Meta, cb: Callback [Unit]) {
    val buf = PagedBuffer (12)
    disks.fill (buf, meta.pos, callback (cb) { _ =>
      for (i <- 0 until meta.count) {
        recoveries.read (buf)
      }})
  }
}

object RootRegistry {

  case class Meta (count: Int, pos: Position)

  object Meta {

    val empty = Meta (0, Position (0, 0, 0))

    val pickle = {
      import Picklers._
      val pos = Position.pickle
      wrap (int, pos)
      .build ((Meta.apply _).tupled)
      .inspect (v => (v.count, v.pos))
    }}

  val framer: Framer.Strategy [TypeId, TypeId] =
    new Framer.Strategy [TypeId, TypeId] {

      def newEphemeralId = ???

      def isEphemeralId (id: TypeId) = false

      def readHeader (buf: PagedBuffer): (Option [TypeId], TypeId) = {
        val id =TypeId (readInt())
        (Some (id), id)
      }

      def writeHeader (hdr: TypeId, buf: PagedBuffer) {
        buf.writeInt (hdr.id)
      }}}
