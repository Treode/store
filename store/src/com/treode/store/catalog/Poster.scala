package com.treode.store.catalog

import java.util.ArrayDeque

import com.treode.async.{Async, Fiber, Callback, Scheduler}
import com.treode.disk.{Disks, PageDescriptor, Position, RecordDescriptor}
import com.treode.store.{Bytes, CatalogDescriptor, CatalogId}

import Async.guard
import Callback.callback

private trait Poster {

  def post (update: Update, bytes: Bytes)

  def checkpoint (version: Int, bytes: Bytes, patches: Seq [Bytes]): Async [(CatalogId, Position)]
}

private object Poster {

  val update = {
    import CatalogPicklers._
    RecordDescriptor (0x7F5551148920906EL, tuple (catId, CatalogPicklers.update))
  }

  val pager = {
    import CatalogPicklers._
    PageDescriptor (0x8407E7035A50C6CFL, int, tuple (int, bytes, seq (bytes)))
  }

  case class Post (update: Update, bytes: Bytes)

  abstract class AbstractPoster (id: CatalogId) (implicit scheduler: Scheduler, disks: Disks)
  extends Poster {

    val fiber = new Fiber (scheduler)
    val posts = new ArrayDeque [Post]

    def dispatch (bytes: Bytes): Unit

    private val _posted = callback [Unit] (_ => posted()) (throw _)

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

    def checkpoint (version: Int, bytes: Bytes, history: Seq [Bytes]): Async [(CatalogId, Position)] =
      guard {
        for {
          pos <- pager.write (0, (version, bytes, history))
        } yield (id, pos)
      }}

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

      override def toString = s"Poster(${desc.id},${desc.pcat})"
    }}

  def apply (id: CatalogId) (implicit scheduler: Scheduler, disks: Disks): Poster =
    new AbstractPoster (id) {

      def dispatch (bytes: Bytes): Unit = ()

      override def toString = s"Poster($id,unknown)"
    }}
