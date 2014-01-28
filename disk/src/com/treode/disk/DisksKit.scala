package com.treode.disk

import java.nio.file.{Path, StandardOpenOption}
import java.util.concurrent.ExecutorService
import scala.collection.immutable.Queue
import scala.language.postfixOps

import com.treode.async._
import com.treode.async.io.File
import com.treode.buffer.PagedBuffer

private class DisksKit (implicit scheduler: Scheduler) extends Disks {

  case class SuperBlocks (path: Path, file: File, sb1: Option [SuperBlock], sb2: Option [SuperBlock])

  type ReattachItem = (Path, File)
  type AttachItem = (Path, File, DiskDriveConfig)
  type AttachPending = (Seq [AttachItem], Callback [Unit])
  type AttachesPending = Queue [AttachPending]
  type CheckpointsPending = List [Callback [Unit]]

  val fiber = new Fiber (scheduler)
  val log = new LogDispatcher (scheduler)
  val pages = new PageDispatcher (scheduler)
  val cache = new PageCache (scheduler)
  val recovery = new RecoveryKit (scheduler, this)
  val roots = new RootRegistry (pages)
  val cleaner = new SegmentCleaner
  var disks = Map.empty [Int, DiskDrive]
  var generation = 0
  var number = 0
  var meta = RootRegistry.Meta.empty
  var state: State = new Opening

  private def readSuperBlocks (path: Path, file: File, cb: Callback [SuperBlocks]): Unit =
    guard (cb) {

      val buffer = PagedBuffer (SuperBlockBits+1)

      def unpickleSuperBlock (pos: Int): Option [SuperBlock] =
        try {
          buffer.readPos = pos
          Some (SuperBlock.pickler.unpickle (buffer))
        } catch {
          case e: Throwable => None
        }

      def unpickleSuperBlocks() {
        val sb1 = unpickleSuperBlock (0)
        val sb2 = unpickleSuperBlock (SuperBlockBytes)
        cb (SuperBlocks (path, file, sb1, sb2))
      }

      file.fill (buffer, 0, DiskLeadBytes, new Callback [Unit] {
        def pass (v: Unit) = unpickleSuperBlocks()
        def fail (t: Throwable) = unpickleSuperBlocks()
      })
    }

  trait State {
    def reattach (items: Seq [ReattachItem], cb: Callback [Unit]): Unit
    def attach (items: Seq [AttachItem], cb: Callback [Unit])
    def checkpoint (cb: Callback [Unit])
  }

  trait QueueAttachAndCheckpoint {

    var attaches: AttachesPending
    var checkpoints: CheckpointsPending

    def attach (items: Seq [AttachItem], cb: Callback [Unit]): Unit =
      guard (cb) {
        attaches = attaches.enqueue (items, cb)
      }

    def checkpoint (cb: Callback [Unit]): Unit =
      checkpoints ::= cb

    def ready (checkpoint: Boolean) {
      if (checkpoint) {
        for (checkpoint <- checkpoints)
          scheduler.execute (checkpoint())
        checkpoints = List.empty
      }
      if (!attaches.isEmpty) {
        val (first, rest) = attaches.dequeue
        state = new Attaching (first._1, first._2, false, rest, checkpoints)
      } else if (!checkpoints.isEmpty) {
        state = new Checkpointing (attaches, checkpoints)
      } else if (!disks.isEmpty) {
        state = Ready
      } else {
        state = new Opening
      }}

    def panic (t: Throwable) {
      state = new Panicked (t)
      for (attach <- attaches)
        scheduler.fail (attach._2, new PanickedException (t))
      for (cp <- checkpoints)
        scheduler.fail (cp, new PanickedException (t))
    }}

  trait ReattachPending {

    def reattach (items: Seq [(Path, File)], cb: Callback [Unit]): Unit =
      scheduler.fail (cb, new ReattachmentPendingException)
  }

  trait ReattachCompleted {

    def reattach (items: Seq [ReattachItem], cb: Callback [Unit]): Unit =
      scheduler.fail (cb, new RecoveryCompletedException)
  }

  class Opening extends State {

    var checkpoints = List.empty [Callback [Unit]]

    def reattach (items: Seq [ReattachItem], cb: Callback [Unit]): Unit =
      state = new Reattaching (items, cb, Queue.empty, checkpoints)

    def attach (items: Seq [AttachItem], cb: Callback [Unit]): Unit =
      guard (cb) {
        state = new Attaching (items, cb, true, Queue.empty, checkpoints)
      }

    def checkpoint (cb: Callback [Unit]): Unit =
      checkpoints ::= cb

    override def toString = "Opening"
  }

  class Reattaching (
      items: Seq [ReattachItem],
      cb: Callback [Unit],
      var attaches: AttachesPending,
      var checkpoints: CheckpointsPending
  ) extends State with QueueAttachAndCheckpoint with ReattachPending {

    def superBlocksRead (reads: Seq [SuperBlocks]) {

      val sb1 = reads.map (_.sb1) .flatten
      val sb2 = reads.map (_.sb2) .flatten
      if (sb1.size == 0 && sb2.size == 0) {
        panic (new NoSuperBlocksException)
        scheduler.fail (cb, new NoSuperBlocksException)
        return
      }

      val gen1 = if (sb1.isEmpty) -1 else sb1.map (_.boot.gen) .max
      val count1 = sb1 count (_.boot.gen == gen1)
      val gen2 = if (sb2.isEmpty) -1 else sb2.map (_.boot.gen) .max
      val count2 = sb2 count (_.boot.gen == gen2)
      if (count1 != items.size && count2 != items.size) {
        panic (new InconsistentSuperBlocksException)
        scheduler.fail (cb, new InconsistentSuperBlocksException)
        return
      }

      val useGen1 = (count1 == items.size) && (gen1 > gen2 || count2 != items.size)
      val boot = if (useGen1) reads.head.sb1.get.boot else reads.head.sb2.get.boot

      val reattaching = items .map (_._1) .toSet
      val attached = boot.disks.toSet
      if (!(attached forall (reattaching contains _))) {
        val missing = (attached -- reattaching).toSeq.sorted
        panic (new MissingDisksException (missing))
        scheduler.fail (cb, new MissingDisksException (missing))
        return
      }
      if (!(reattaching forall (attached contains _))) {
        val extra = (reattaching -- attached).toSeq.sorted
        panic (new ExtraDisksException (extra))
        scheduler.fail (cb, new ExtraDisksException (extra))
        return
      }

      number = boot.num
      generation = boot.gen
      meta = boot.roots

      val latch = Callback.latch (reads.size, new Callback [Unit] {
        def pass (v: Unit) {
          state = new Recovering (cb, attaches, checkpoints)
        }
        def fail (t: Throwable) {
          panic (t)
          scheduler.fail (cb, t)
        }})

      for (read <- reads) {
        val superblock = if (useGen1) read.sb1.get else read.sb2.get
        val disk = new DiskDrive (superblock.id, read.path, read.file, superblock.config,
            scheduler, log, pages)
        disks += disk.id -> disk
        disk.recover (superblock, latch)
      }}

    val _superBlocksRead = Callback.collect (items.size, new Callback [Seq [SuperBlocks]] {
      def pass (reads: Seq [SuperBlocks]) = fiber.execute {
        superBlocksRead (reads)
      }
      def fail (t: Throwable) = fiber.execute {
        panic (t)
        scheduler.fail (cb, t)
      }})

    for ((path, file) <- items)
      readSuperBlocks (path, file, _superBlocksRead)

    override def toString = "Reattaching"
  }

  class Recovering (
      cb: Callback [Unit],
      var attaches: AttachesPending,
      var checkpoints: CheckpointsPending
  ) extends State with QueueAttachAndCheckpoint with ReattachPending {

    def recovered = new Callback [Unit] {
      def pass (v: Unit) = fiber.execute {
        disks.values foreach (_.engage())
        ready (false)
        cb()
      }
      def fail (t: Throwable) {
        panic (t)
        scheduler.fail (cb, t)
      }}

    recovery.recover (meta, recovered)

    override def toString = "Recovering"
  }

  class Attaching (
      _items: Seq [AttachItem],
      cb: Callback [Unit],
      opening: Boolean,
      var attaches: AttachesPending,
      var checkpoints: CheckpointsPending)
  extends State with QueueAttachAndCheckpoint with ReattachCompleted {

    val items =
      for (((path, file, config), i) <- _items zipWithIndex) yield
        new DiskDrive (number + i, path, file, config, scheduler, log, pages)

    val attached = disks.values.map (_.path) .toSet
    val attaching = items.map (_.path) .toSet
    val newboot = BootBlock (generation + 1, number + items.size, attached ++ attaching, meta)

    def latch4 = Callback.latch (disks.size, new Callback [Unit] {
      def pass (v: Unit) = fiber.execute (ready (false))
      def fail (t: Throwable) = fiber.execute (panic (t))
    })

    def latch3 = Callback.latch (disks.size, new Callback [Unit] {
      def pass (v: Unit): Unit = fiber.execute {
        generation = newboot.gen
        number = newboot.num
        disks ++= (for (item <- items) yield item.id -> item)
        items foreach (_.engage())
        if (opening)
          recovery.recover()
        ready (false)
        scheduler.execute (cb, ())
      }
      def fail (t: Throwable): Unit = fiber.execute {
        val oldboot = BootBlock (generation, number, attached, meta)
        val latch = latch4
        disks.values foreach (_.checkpoint (oldboot, latch))
        scheduler.fail (cb, t)
      }})

    def latch2 = Callback.latch (items.size, new Callback [Unit] {
      def pass (v: Unit): Unit = fiber.execute {
        val latch = latch3
        disks.values foreach (_.checkpoint (newboot, latch))
      }
      def fail (t: Throwable): Unit = fiber.execute {
        ready (false)
        scheduler.fail (cb, t)
      }})

    def latch1 = Callback.latch (items.size, new Callback [Unit] {
      def pass (v: Unit): Unit = fiber.execute {
        val latch = latch2
        items foreach (_.checkpoint (newboot, latch))
      }
      def fail (t: Throwable): Unit = fiber.execute {
        ready (false)
        scheduler.fail (cb, t)
      }
    })

    if (attaching exists (attached contains _)) {
      val already = (attaching -- attached).toSeq.sorted
      ready (false)
      scheduler.fail (cb, new AlreadyAttachedException (already))
    } else {
      val latch = latch1
      guard (latch) (items foreach (_.init (latch)))
    }

    override def toString = "Attaching"
  }

  class Checkpointing (var attaches: AttachesPending, var checkpoints: CheckpointsPending)
  extends State with QueueAttachAndCheckpoint with ReattachCompleted {

    val attached = disks.values.map (_.path) .toSet

    def rootPageWritten (newboot: BootBlock, roots: RootRegistry.Meta) =
      Callback.latch (disks.size, new Callback [Unit] {
        def pass (v: Unit) = fiber.execute {
          generation = newboot.gen
          meta = roots
          ready (true)
        }
        def fail (t: Throwable) = fiber.execute (panic (t))
      })

    val rootsWritten =
      new Callback [RootRegistry.Meta] {
        def pass (roots: RootRegistry.Meta) = fiber.execute {
          val newboot = BootBlock (generation + 1, number, attached, roots)
          val latch = rootPageWritten (newboot, roots)
          disks.values foreach (_.checkpoint (newboot, latch))
        }
        def fail (t: Throwable) = fiber.execute (panic (t))
    }

    roots.checkpoint (generation+1, rootsWritten)

    override def toString = "Checkpointing"
  }

  class Panicked (t: Throwable) extends State {

    def reattach (items: Seq [ReattachItem], cb: Callback [Unit]): Unit =
      scheduler.fail (cb, new PanickedException (t))

    def attach (items: Seq [AttachItem], cb: Callback [Unit]): Unit =
      scheduler.fail (cb, new PanickedException (t))

    def checkpoint (cb: Callback [Unit]): Unit =
      scheduler.fail (cb, new PanickedException (t))

    override def toString = s"Panicked(${t})"
  }

  object Ready extends State with ReattachCompleted {

    def attach (items: Seq [AttachItem], cb: Callback [Unit]): Unit =
      guard (cb) {
        state = new Attaching (items, cb, false, Queue.empty, List.empty)
      }

    def checkpoint (cb: Callback [Unit]): Unit =
      guard (cb) {
        state = new Checkpointing (Queue.empty, List (cb))
      }

    override def toString = "Ready"
  }

  private def reopenFile (path: Path, exec: ExecutorService) = {
    import StandardOpenOption.{READ, WRITE}
    (path, File.open (path, exec, READ, WRITE))
  }

  def reattach (items: Seq [(Path, File)], cb: Callback [Unit]): Unit =
    guard (cb) {
      require (!items.isEmpty, "Must list at least one file to reaattach.")
      fiber.execute (state.reattach (items, cb))
    }

  def reattach (items: Seq [Path], executor: ExecutorService, cb: Callback [Unit]): Unit =
    guard (cb) {
      val files = items map (reopenFile (_, executor))
      fiber.execute (state.reattach (files, cb))
    }

  private def openFile (item: (Path, DiskDriveConfig), exec: ExecutorService) = {
    val (path, config) = item
    import StandardOpenOption.{CREATE, READ, WRITE}
    val file = File.open (path, exec, CREATE, READ, WRITE)
    (path, file, config)
  }

  def attach (items: Seq [(Path, File, DiskDriveConfig)], cb: Callback [Unit]): Unit =
    guard (cb) {
      require (!items.isEmpty, "Must list at least one file to attach.")
      fiber.execute (state.attach (items, cb))
    }

  def attach (items: Seq [(Path, DiskDriveConfig)], exec: ExecutorService, cb: Callback [Unit]): Unit =
    guard (cb) {
      require (!items.isEmpty, "Must llist at least one path to attach.")
      val files = items map (openFile (_, exec))
      fiber.execute (state.attach (files, cb))
    }

  def checkpoint (cb: Callback [Unit]): Unit =
    fiber.execute (state.checkpoint (cb))

  def replayIterator (records: RecordRegistry, cb: Callback [ReplayIterator]): Unit =
    LogIterator.merge (disks.values, records, cb)

  def open (f: Recovery => Any): Unit =
    recovery.open (f)

  def checkpoint [B] (desc: RootDescriptor [B]) (f: Callback [B] => Any): Unit =
    roots.checkpoint (desc) (f)

  def handle [G, P] (desc: PageDescriptor [G, P], handler: PageHandler [G]): Unit =
    cleaner.handle (desc, handler)

  def record [R] (desc: RecordDescriptor [R], entry: R, cb: Callback [Unit]): Unit =
    log.record (desc, entry, cb)

  def fill (buf: PagedBuffer, pos: Position, cb: Callback [Unit]): Unit =
    disks (pos.disk) .fill (buf, pos.offset, pos.length, cb)

  def read [G, P] (desc: PageDescriptor [G, P], pos: Position, cb: Callback [P]): Unit =
    cache.read (desc, disks, pos, cb)

  def write [G, P] (desc: PageDescriptor [G, P], group: G, page: P, cb: Callback [Position]): Unit =
    pages.write (desc, group, page, cb)

  override def toString = state.toString
}
