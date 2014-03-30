package com.treode.disk

import java.nio.file.Path
import java.util.concurrent.ExecutorService
import scala.collection.immutable.Queue
import scala.language.postfixOps
import scala.util.{Failure, Success}

import com.treode.async.{Async, AsyncImplicits, Callback, Fiber, Latch, Scheduler}
import com.treode.async.io.File

import Async.{async, guard}
import AsyncImplicits._
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
  var engaged = true

  var bootgen = 0
  var number = 0

  private def reengage() {
    if (!attachreqs.isEmpty) {
      val (first, rest) = attachreqs.dequeue
      attachreqs = rest
      _attach (first)
    } else if (!detachreqs.isEmpty) {
      val all = detachreqs
      detachreqs = List.empty
      _detach (all)
    } else if (!drainreqs.isEmpty) {
      val (first, rest) = drainreqs.dequeue
      drainreqs = rest
      _drain (first)
    } else if (!checkreqs.isEmpty) {
      val (first, rest) = checkreqs.dequeue
      checkreqs = rest
      _checkpoint (first)
    } else {
      engaged = false
    }}

  def launch(): Async [Unit] =
    fiber.supply {
      for {
        segs <- disks.values.filter (_.draining) .latch.seq foreach (_.drain())
      } yield {
        compactor.drain (segs.iterator.flatten)
      }
      reengage()
    }

  private def leave [A] (cb: Callback [A]): Callback [A] = {
    case Success (v) =>
      fiber.execute (reengage())
      scheduler.pass (cb, v)
    case Failure (t) =>
      fiber.execute (reengage())
      scheduler.fail (cb, t)
  }

  private def _attach (req: AttachRequest) {
    val items = req._1
    val cb = leave (req._2)
    engaged = true
    fiber.guard {

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
          for (((path, file, geometry), i) <- items.zipWithIndex.latch.indexed)
            DiskDrive.init (this.number + i + 1, path, file, geometry, newBoot, kit)
        _ <- priorDisks.latch.unit foreach (_.checkpoint (newBoot, None))
      } yield fiber.execute {
        disks ++= newDisks.mapBy (_.id)
        this.bootgen = bootgen
        this.number = number
        newDisks foreach (_.added())
      }
    } run (cb)
  }

  def attach (items: Seq [(Path, File, DiskGeometry)]): Async [Unit] =
    fiber.async { cb =>

      val attaching = items.map (_._1) .toSet
      require (!items.isEmpty, "Must list at least one file or device to attach.")
      require (attaching.size == items.size, "Cannot attach a path multiple times.")

      if (engaged)
        attachreqs = attachreqs.enqueue (items, cb)
      else
        _attach (items, cb)
    }

  def attach (items: Seq [(Path, DiskGeometry)], exec: ExecutorService): Async [Unit] =
    guard {
      val files = items map (openFile (_, exec))
      attach (files)
    }

  private def _detach (items: List [DiskDrive]) {
    engaged = true
    fiber.guard {

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
      }
    } run (leave (ignore))
  }

  def detach (disk: DiskDrive) {
    fiber.execute {
      if (engaged)
        detachreqs ::= disk
      else
        _detach (List (disk))
    }}

  private def _drain (req: DrainRequest) {
    val (items, _cb) = req
    val cb = leave (_cb)
    engaged = true
    fiber.guard {

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
        segs <- draining.latch.seq foreach (_.drain())
      } yield {
        checkpointer.checkpoint()
            .leave (compactor.drain (segs.iterator.flatten))
            .run (ignore)
      }
    } run (cb)
  }

  def drain (items: Seq [Path]): Async [Unit] =
    fiber.async { cb =>
      require (!items.isEmpty, "Must list at least one file or device to attach.")
      if (engaged)
        drainreqs = drainreqs.enqueue (items, cb)
      else
        _drain (items, cb)
    }

  private def _checkpoint (req: CheckpointRequest) {
    val (marks, _cb) = req
    val cb = leave (_cb)
    engaged = true
    fiber.guard {
      val bootgen = this.bootgen + 1
      val attached = disks.values.map (_.path) .toSet
      val newBoot = BootBlock (cell, bootgen, number, attached)
      for {
        _ <- disks.values.latch.unit foreach (disk => disk.checkpoint (newBoot, marks get disk.id))
      } yield {
        this.bootgen = bootgen
      }
    } run (cb)
  }

  def checkpoint (marks: Map [Int, Long]): Async [Unit] =
    fiber.async { cb =>
      if (engaged)
        checkreqs = checkreqs.enqueue (marks, cb)
      else
        _checkpoint (marks, cb)
    }

  def add (disks: Seq [DiskDrive]): Async [Unit] =
    fiber.supply {
      this.disks ++= disks.mapBy (_.id)
      disks foreach (_.added())
    }

  def mark(): Async [Map [Int, Long]] =
    fiber.guard {
      disks.values.latch.map foreach (_.mark())
    }

  def cleanable(): Async [Iterator [SegmentPointer]] =  {
    fiber.guard {
      for {
        segs <- disks.values.filterNot (_.draining) .latch.seq.foreach (_.cleanable())
      } yield segs.iterator.flatten
    }}

  def fetch [P] (desc: PageDescriptor [_, P], pos: Position): Async [P] =
    guard (DiskDrive.read (disks (pos.disk) .file, desc, pos))
}
