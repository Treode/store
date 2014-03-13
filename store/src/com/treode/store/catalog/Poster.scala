package com.treode.store.catalog

import java.util.ArrayDeque
import scala.util.{Failure, Success}

import com.treode.async.{Async, Fiber, Callback, Scheduler}
import com.treode.disk.{Disks, PageDescriptor, Position, RecordDescriptor}
import com.treode.store.{Bytes, CatalogDescriptor, CatalogId}

import Async.guard
import Poster.Meta

private trait Poster {

  def post (update: Update, bytes: Bytes)
  def checkpoint (version: Int, bytes: Bytes, patches: Seq [Bytes]): Async [Meta]
  def checkpoint (meta: Meta): Async [Unit]
}

private object Poster {

  case class Meta (version: Int, pos: Position)

  object Meta {

    val pickler = {
      import CatalogPicklers._
      wrap (uint, pos)
      .build ((Meta.apply _).tupled)
      .inspect (v => (v.version, v.pos))
    }}

  case class Post (update: Update, bytes: Bytes)

  val update = {
    import CatalogPicklers._
    RecordDescriptor (0x7F5551148920906EL, tuple (catId, Update.pickler))
  }

  val checkpoint = {
    import CatalogPicklers._
    RecordDescriptor (0x79C3FDABEE2C9FDFL, tuple (catId, Meta.pickler))
  }

  val pager = {
    import CatalogPicklers._
    PageDescriptor (0x8407E7035A50C6CFL, uint, tuple (uint, bytes, seq (bytes)))
  }

  abstract class AbstractPoster (id: CatalogId) (implicit scheduler: Scheduler, disks: Disks)
  extends Poster {

    val fiber = new Fiber (scheduler)
    val posts = new ArrayDeque [Post]

    def dispatch (bytes: Bytes): Unit

    private val _posted: Callback [Unit] = {
      case Success (v) => posted()
      case Failure (t) => throw t
    }

    private def engage() {
      val post = posts.peek()
      update.record (id, post.update) .run (_posted)
    }

    private def posted(): Unit = fiber.execute {
      val post = posts.remove()
      if (posts.isEmpty)
        dispatch (post.bytes)
      else
        engage()
    }

    def post (update: Update, bytes: Bytes): Unit = fiber.execute {
      val empty = posts.isEmpty
      posts.add (Post (update, bytes))
      if (empty)
        engage()
    }

    def checkpoint (version: Int, bytes: Bytes, history: Seq [Bytes]): Async [Meta] =
      guard {
        for {
          pos <- pager.write (id.id, version, (version, bytes, history))
          meta = Meta (version, pos)
          _ <- Poster.checkpoint.record (id, meta)
        } yield {
          meta
        }}


    def checkpoint (meta: Meta): Async [Unit] =
      Poster.checkpoint.record (id, meta)
  }

  def apply [C] (
      desc: CatalogDescriptor [C],
      handler: C => Any
  ) (implicit
      scheduler: Scheduler,
      disks: Disks
  ): Poster = {
    new AbstractPoster (desc.id) {

      def dispatch (bytes: Bytes): Unit =
        scheduler.execute (handler (bytes.unpickle (desc.pcat)))

      override def toString = s"Poster(${desc.id}, ${desc.pcat})"
    }}

  def apply (id: CatalogId) (implicit scheduler: Scheduler, disks: Disks): Poster =
    new AbstractPoster (id) {

      def dispatch (bytes: Bytes): Unit = ()

      override def toString = s"Poster($id, unknown)"
    }}
