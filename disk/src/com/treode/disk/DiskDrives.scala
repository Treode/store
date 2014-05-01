package com.treode.disk

import java.nio.file.Path
import java.util.concurrent.ExecutorService
import scala.collection.immutable.Queue
import scala.language.postfixOps
import scala.util.{Failure, Success}

import com.treode.async.{Async, AsyncQueue, Callback, Fiber}
import com.treode.async.implicits._
import com.treode.async.io.File

import Async.{async, guard}
import Callback.ignore

private class DiskDrives (kit: DisksKit) {
  import kit.{checkpointer, compactor, scheduler}
  import kit.config.cell

  type AttachItem = (Path, File, DiskGeometry)
  type AttachRequest = (Seq [AttachItem], Callback [Unit])
  type DrainRequest = (Seq [Path], Callback [Unit])
  type DetachRequest = DiskDrive
  type CheckpointRequest = (Map [Int, Long], Callback [Unit])

  val fiber = new Fiber (scheduler)

  var disks = Map.empty [Int, DiskDrive]
  var attachreqs = Queue.empty [AttachRequest]
  var detachreqs = List.empty [DetachRequest]
  var drainreqs = Queue.empty [DrainRequest]
  var checkreqs = Queue.empty [CheckpointRequest]

  var bootgen = 0
  var number = 0

  val queue = AsyncQueue (fiber) {
    if (!attachreqs.isEmpty) {
      val ((items, cb), rest) = attachreqs.dequeue
      attachreqs = rest
      _attach (items, cb)
    } else if (!detachreqs.isEmpty) {
      val all = detachreqs
      detachreqs = List.empty
      _detach (all)
    } else if (!drainreqs.isEmpty) {
      val ((items, cb), rest) = drainreqs.dequeue
      drainreqs = rest
      _drain (items, cb)
    } else if (!checkreqs.isEmpty) {
      val ((marks, cb), rest) = checkreqs.dequeue
      checkreqs = rest
      _checkpoint (marks, cb)
    } else {
      None
    }}

  def launch(): Async [Unit] =
    for {
      segs <- disks.values.filter (_.draining) .latch.casual foreach (_.drain())
    } yield {
      compactor.drain (segs.iterator.flatten)
      queue.launch()
    }

  private def _attach (items: Seq [AttachItem], cb: Callback [Unit]): Option [Runnable] =
    queue.run (cb) {

      val priorDisks = disks.values
      val priorPaths = priorDisks.setBy (_.path)
      val newPaths = items.setBy (_._1)
      val bootgen = this.bootgen + 1
      val number = this.number + items.size
      val attached = priorPaths ++ newPaths
      val newBoot = BootBlock (cell, bootgen, number, attached)

      if (newPaths exists (priorPaths contains _)) {
        val already = (newPaths -- priorPaths).toSeq.sorted
        throw new AlreadyAttachedException (already)
      }

      for {
        newDisks <-
          for (((path, file, geometry), i) <- items.zipWithIndex.latch.seq)
            DiskDrive.init (this.number + i + 1, path, file, geometry, newBoot, kit)
        _ <- priorDisks.latch.unit foreach (_.checkpoint (newBoot, None))
      } yield {
        disks ++= newDisks.mapBy (_.id)
        this.bootgen = bootgen
        this.number = number
        newDisks foreach (_.added())
      }}

  def attach (items: Seq [(Path, File, DiskGeometry)]): Async [Unit] = {
    queue.async { cb =>
      val attaching = items.map (_._1) .toSet
      require (!items.isEmpty, "Must list at least one file or device to attach.")
      require (attaching.size == items.size, "Cannot attach a path multiple times.")
      attachreqs = attachreqs.enqueue (items, cb)
    }}

  def attach (items: Seq [(Path, DiskGeometry)], exec: ExecutorService): Async [Unit] =
    guard {
      val files = items map (openFile (_, exec))
      attach (files)
    }

  private def _detach (items: List [DiskDrive]): Option [Runnable] =
    queue.run (ignore) {

      val paths = items map (_.path)
      val disks = this.disks -- (items map (_.id))
      val bootgen = this.bootgen + 1
      val attached = disks.values.setBy (_.path)
      val newboot = BootBlock (cell, bootgen, number, attached)

      for {
        _ <- disks.values.latch.unit foreach (_.checkpoint (newboot, None))
      } yield {
        this.disks = disks
        this.bootgen = bootgen
        items foreach (_.detach())
        // TODO: add detach hooks
      }}

  def detach (disk: DiskDrive): Unit =
    queue.execute {
      detachreqs ::= disk
    }

  private def _drain (items: Seq [Path], cb: Callback [Unit]): Option [Runnable] =
    queue.run (cb) {

      val byPath = disks.values.mapBy (_.path)
      if (!(items forall (byPath contains _))) {
        val unattached = (items.toSet -- byPath.keySet).toSeq.sorted
        throw new NotAttachedException (unattached)
      }

      if (items.size == disks.values.filterNot (_.draining) .size) {
        throw new CannotDrainAllException
      }

      val draining = items map (byPath.apply _)
      for {
        segs <- draining.latch.casual foreach (_.drain())
      } yield {
        checkpointer.checkpoint()
            .ensure (compactor.drain (segs.iterator.flatten))
            .run (ignore)
      }}

  def drain (items: Seq [Path]): Async [Unit] =
    queue.async { cb =>
      require (!items.isEmpty, "Must list at least one file or device to attach.")
      drainreqs = drainreqs.enqueue (items, cb)
    }

  private def _checkpoint (marks: Map [Int, Long], cb: Callback [Unit]): Option [Runnable] =
    queue.run (cb) {
      val bootgen = this.bootgen + 1
      val attached = disks.values.map (_.path) .toSet
      val newBoot = BootBlock (cell, bootgen, number, attached)
      for {
        _ <- disks.values.latch.unit foreach (disk => disk.checkpoint (newBoot, marks get disk.id))
      } yield {
        this.bootgen = bootgen
      }}

  def checkpoint (marks: Map [Int, Long]): Async [Unit] =
    queue.async { cb =>
      checkreqs = checkreqs.enqueue (marks, cb)
    }

  def add (disks: Seq [DiskDrive]): Async [Unit] =
    fiber.supply {
      this.disks ++= disks.mapBy (_.id)
      disks foreach (_.added())
    }

  def mark(): Async [Map [Int, Long]] =
    guard {
      disks.values.latch.map foreach (_.mark())
    }

  def cleanable(): Async [Iterator [SegmentPointer]] =  {
    guard {
      for {
        segs <- disks.values.filterNot (_.draining) .latch.casual.foreach (_.cleanable())
      } yield segs.iterator.flatten
    }}

  def fetch [P] (desc: PageDescriptor [_, P], pos: Position): Async [P] =
    guard (DiskDrive.read (disks (pos.disk) .file, desc, pos))
}
