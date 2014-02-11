package com.treode.disk

import java.nio.file.Path
import java.util.concurrent.ExecutorService
import scala.collection.immutable.Queue

import com.treode.async.{Callback, Fiber, Scheduler, callback, continue, defer}
import com.treode.async.io.File

private class DiskDrives (implicit val scheduler: Scheduler, val config: DisksConfig)
extends Disks {

  type AttachItem = (Path, File, DiskGeometry)
  type AttachPending = (Seq [AttachItem], Callback [Unit])

  val fiber = new Fiber (scheduler)
  val cache = new PageCache (this)
  val logd = new Dispatcher [PickledRecord] (scheduler)
  val paged = new Dispatcher [PickledPage] (scheduler)
  val releaser = new SegmentReleaser (this)
  var disks = Map.empty [Int, DiskDrive]
  var generation = 0
  var number = 0
  var roots = Position (0, 0, 0)
  var loggedBytes = 0
  var loggedEntries = 0
  var allocedSegments = 0
  var attachreqs = Queue.empty [AttachPending]
  var checkreqs = false
  var cleanreqs = false
  var state: State = new Launching

  trait State {
    def launch (checkreqs: CheckpointRegistry, pages: PageRegistry)
    def attach (items: Seq [AttachItem], cb: Callback [Unit])
    def checkpoint()
    def clean()
    def panic (t: Throwable)
  }

  class Launching extends State {

    def launch (checkpoints: CheckpointRegistry, pages: PageRegistry) {
      state = new Launched (checkpoints, pages)
    }

    def attach (items: Seq [AttachItem], cb: Callback [Unit]): Unit =
      defer (cb) {
        attachreqs = attachreqs.enqueue (items, cb)
      }

    def checkpoint(): Unit =
      checkreqs = true

    def clean(): Unit =
      cleanreqs = true

    def panic (t: Throwable): Unit =
      state = new Panicked (t)

    override def toString = "DiskDrives.Launching"
  }

  class Launched (checkpoints: CheckpointRegistry, pages: PageRegistry) extends State {

    var attaching = false

    if (!attachreqs.isEmpty) {
      val (first, rest) = attachreqs.dequeue
      attaching = true
      attachreqs = rest
      attach (first)
    }

    def ready() {
      if (!attachreqs.isEmpty) {
        val (first, rest) = attachreqs.dequeue
        attaching = true
        attachreqs = rest
        attach (first)
      } else if (checkreqs) {
        checkpoint()
      } else if (config.checkpoint (loggedBytes, loggedEntries)) {
        checkreqs = true
        checkpoint()
      }}

    def attach (req: AttachPending) {
      val (items, cb) = req
      defer (cb) {

        val priorPaths = disks.values.map (_.path) .toSet
        val newPaths = items.map (_._1) .toSet
        val newBoot = BootBlock (generation, number+items.size, priorPaths++newPaths, roots)

        val newDisksPrimed = new Callback [Seq [DiskDrive]] {
          def pass (newDisks: Seq [DiskDrive]): Unit = fiber.execute {
            generation = newBoot.gen
            number = newBoot.num
            disks ++= newDisks.mapBy (_.id)
            attaching = false
            checkpoint()
          }
          def fail (t: Throwable): Unit = fiber.execute {
            attaching = false
            ready()
            scheduler.fail (cb, t)
          }}

        if (newPaths exists (priorPaths contains _)) {
          val already = (newPaths -- priorPaths).toSeq.sorted
          ready()
          scheduler.fail (cb, new AlreadyAttachedException (already))
        } else {
          DiskDrive.init (items, number, newBoot, DiskDrives.this, newDisksPrimed)
        }}}

    def _checkpoint() {

      val newgen = generation+1
      loggedBytes = 0
      loggedEntries = 0

      def rootPageWritten (newboot: BootBlock, newroots: Position) =
        Callback.latch (disks.size, new Callback [Unit] {
          def pass (v: Unit) = fiber.execute {
            generation = newboot.gen
            roots = newroots
            checkreqs = false
            ready()
          }
          def fail (t: Throwable) = fiber.execute (panic (t))
        })

      val rootsWritten =
        new Callback [Position] {
          def pass (newroots: Position) = fiber.execute {
            val attached = disks.values.map (_.path) .toSet
            val newboot = BootBlock (newgen, number, attached, newroots)
            val latch = rootPageWritten (newboot, newroots)
            for (disk <- disks.values)
              disk.checkpoint (newboot, latch)
          }
          def fail (t: Throwable) = fiber.execute (panic (t))
      }

      val oneLogMarked = Callback.latch (disks.size, new Callback [Unit] {
        def pass (v: Unit) {
          checkpoints.checkpoint (newgen, rootsWritten)
        }
        def fail (t: Throwable) = fiber.execute (panic (t))
      })

      disks.values foreach (_.mark (oneLogMarked))
    }

    def _cleanable (cb: Callback [Iterator [SegmentPointer]]) {
      val latch = Callback.map (disks.size, callback (cb) { disks: Map [DiskDrive, IntSet] =>
        for ((disk, segs) <- disks.iterator; seg <- segs.iterator)
          yield SegmentPointer (disk, seg)
      })
      disks.values foreach (_.cleanable (latch))
    }

    def _clean() {
      val cleaner = new SegmentCleaner (DiskDrives.this, pages)
      val loop = new Callback [Boolean] {
        def pass (v: Boolean) {
          if (v || allocedSegments >= config.cleaningFrequency) {
            allocedSegments = 0
            _cleanable (continue (this) { iter =>
              cleaner.clean (iter, this)
            })
          } else {
            cleanreqs = false
          }}
        def fail (t: Throwable): Unit = fiber.execute {
          panic (t)
        }}
      loop (true)
    }

    def launch (checkpoints: CheckpointRegistry, pages: PageRegistry): Unit =
      throw new IllegalStateException

    def attach (items: Seq [AttachItem], cb: Callback [Unit]): Unit =
      defer (cb) {
        if (attaching || checkreqs)
          attachreqs = attachreqs.enqueue (items, cb)
        else {
          attaching
          attach ((items, cb))
        }}

    def checkpoint() {
      val now = !attaching && !checkreqs
      checkreqs = true
      if (now) _checkpoint()
    }

    def clean() {
      val now = !cleanreqs
      cleanreqs = true
      if (now) _clean()
    }

    def panic (t: Throwable) {
      state = new Panicked (t)
      for (attach <- attachreqs)
        scheduler.fail (attach._2, new PanickedException (t))
      attachreqs = Queue.empty
      checkreqs = false
    }

    override def toString = s"DiskDrives.Launched(attaching=$attaching)"
  }

  class Panicked (t: Throwable) extends State {

    def launch (checkpoints: CheckpointRegistry, pages: PageRegistry): Unit = ()

    def attach (items: Seq [AttachItem], cb: Callback [Unit]): Unit =
      scheduler.fail (cb, new PanickedException (t))

    def checkpoint(): Unit = ()

    def clean(): Unit = ()

    def panic (t: Throwable): Unit = ()

    override def toString = s"DiskDrives.Panicked(${t})"
  }

  def launch (checkpoints: CheckpointRegistry, pages: PageRegistry): Unit =
    fiber.execute (state.launch (checkpoints, pages))

  def checkpoint(): Unit =
    fiber.execute (state.checkpoint())

  def clean(): Unit =
    fiber.execute (state.clean())

  def panic (t: Throwable): Unit =
    fiber.execute (state.panic (t))

  def add (drives: Seq [DiskDrive]): Unit = fiber.execute {
    disks ++= drives .mapBy (_.id)
  }

  def fetch [P] (desc: PageDescriptor [_, P], pos: Position, cb: Callback [P]): Unit =
    disks (pos.disk) .read (desc, pos, cb)

  def free (segs: Seq [SegmentPointer]): Unit = fiber.execute {
    val byDisk = segs.groupBy (_.disk) .mapValues (_.map (_.num))
    for ((disk, nums) <- byDisk)
      disk.free (nums)
  }

  def tallyLog (bytes: Int, entries: Int): Unit = fiber.execute {
    loggedBytes += bytes
    loggedEntries += entries
    if (config.checkpoint (loggedBytes, loggedEntries) && !checkreqs)
      state.checkpoint()
  }

  def tallyPages (segments: Int): Unit = fiber.execute {
    allocedSegments += 1
    if (allocedSegments >= config.cleaningFrequency && !cleanreqs)
      state.clean()
  }

  def attach (items: Seq [(Path, File, DiskGeometry)], cb: Callback [Unit]): Unit =
    defer (cb) {
      require (!items.isEmpty, "Must list at least one file or device to attach.")
      fiber.execute (state.attach (items, cb))
    }

  def attach (items: Seq [(Path, DiskGeometry)], exec: ExecutorService, cb: Callback [Unit]): Unit =
    defer (cb) {
      val files = items map (openFile (_, exec))
      attach (files, cb)
    }

  def record (disk: Int, entry: RecordHeader, cb: Callback [Unit]): Unit =
    logd.send (PickledRecord (disk, entry, cb))

  def record [R] (desc: RecordDescriptor [R], entry: R, cb: Callback [Unit]): Unit =
    logd.send (PickledRecord (desc, entry, cb))

  def read [P] (desc: PageDescriptor [_, P], pos: Position, cb: Callback [P]): Unit =
    cache.read (desc, pos, cb)

  def write [G, P] (desc: PageDescriptor [G, P], group: G, page: P, cb: Callback [Position]): Unit =
    paged.send (PickledPage (desc, group, page, cb))

  def join [A] (cb: Callback [A]): Callback [A] =
    releaser.join (cb)

  override def toString = state.toString
}
