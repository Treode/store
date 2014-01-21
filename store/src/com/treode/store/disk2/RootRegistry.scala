package com.treode.store.disk2

import java.util.ArrayList
import scala.collection.JavaConversions._

import com.treode.async.{Callback, callback, delay}
import com.treode.buffer.{Input, PagedBuffer, Output}
import com.treode.pickle.{Pickler, Picklers, TagRegistry, pickle, unpickle}

import TagRegistry.Tagger

class RootRegistry (disks: DisksKit, pages: PageDispatcher) {

  private val checkpoints = new ArrayList [Callback [Tagger] => Unit]

  private val recoveries = new TagRegistry [Any]

  def checkpoint [B] (pblk: Pickler [B], id: TypeId) (f: Callback [B] => Any): Unit =
    synchronized {
      checkpoints.add { cb =>
        f (callback (cb) { root =>
          TagRegistry.tagger (pblk, id.id, root)
        })
      }}

  def recover [B] (pblk: Pickler [B], id: TypeId) (f: B => Any): Unit =
    recoveries.register (pblk, id.id) (f)

  def checkpoint (gen: Int, cb: Callback [RootRegistry.Meta]) = synchronized {
    val count = checkpoints.size

    val rootsPageWritten = callback (cb) { pos: Position =>
      RootRegistry.Meta (count, pos)
    }

    val rootsWritten = Callback.collect (count, delay (cb) { roots: Seq [Tagger] =>
      pages.write (RootRegistry.page, 0, roots, rootsPageWritten)
    })

    for (cp <- checkpoints)
      cp (rootsWritten)
  }

  def recover (meta: RootRegistry.Meta, cb: Callback [Unit]) {
    val buf = PagedBuffer (12)
    disks.fill (buf, meta.pos, callback (cb) { _ =>
      unpickle (Picklers.seq (recoveries.unpickler), buf)
    })
  }}

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


  val page = {
    import Picklers._
    new PageDescriptor (0x6EC7584D, const (0), seq (TagRegistry.pickler))
  }}
