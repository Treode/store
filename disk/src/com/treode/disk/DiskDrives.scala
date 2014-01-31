package com.treode.disk

import java.nio.file.{Path, StandardOpenOption}
import java.util.concurrent.ExecutorService
import scala.collection.immutable.Queue
import scala.language.postfixOps

import com.treode.async.{Callback, Fiber, Scheduler, callback, delay, guard}
import com.treode.async.io.File
import com.treode.buffer.PagedBuffer

private class DiskDrives (
    logd: LogDispatcher,
    paged: PageDispatcher,
    var disks: Map [Int, DiskDrive]) (
        implicit scheduler: Scheduler) extends Disks {

  type AttachItem = (Path, File, DiskDriveConfig)
  type AttachPending = (Seq [AttachItem], Callback [Unit])
  type AttachesPending = Queue [AttachPending]
  type CheckpointsPending = List [Callback [Unit]]

  val fiber = new Fiber (scheduler)
  val cache = new PageCache (this)
  var generation = 0
  var number = 0
  var roots = Position (0, 0, 0)
  var state: State = new Launching

  trait State {
    def launch (checkreqs: CheckpointRegistry, pages: PageRegistry)
    def attach (items: Seq [AttachItem], cb: Callback [Unit])
    def checkpoint (cb: Callback [Unit])
  }

  class Launching extends State {

    var attachreqs = Queue.empty [AttachPending]
    var checkreqs = List.empty [Callback [Unit]]

    def launch (checkpoints: CheckpointRegistry, pages: PageRegistry): Unit =
      state = new Launched (checkpoints, pages, attachreqs, checkreqs)

    def attach (items: Seq [AttachItem], cb: Callback [Unit]): Unit =
      guard (cb) {
        attachreqs = attachreqs.enqueue (items, cb)
      }

    def checkpoint (cb: Callback [Unit]): Unit =
      guard (cb) {
        checkreqs ::= cb
      }

    override def toString = "DiskDrives.Launching"
  }

  class Launched (
      checkpoints: CheckpointRegistry,
      pages: PageRegistry,
      var attachreqs: AttachesPending,
      var checkreqs: CheckpointsPending) extends State {

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
      } else if (!checkreqs.isEmpty) {
        checkpoint()
      }}

    def panic (t: Throwable) {
      state = new Panicked (t)
      for (attach <- attachreqs)
        scheduler.fail (attach._2, new PanickedException (t))
      for (cp <- checkreqs)
        scheduler.fail (cp, new PanickedException (t))
    }

    def attach (req: AttachPending) {
      val (items, cb) = req

      val priorPaths = disks.values.map (_.path) .toSet
      val newPaths = items.map (_._1) .toSet
      val newBoot = BootBlock (generation + 1, number + items.size, priorPaths ++ newPaths, roots)

      def priorDisksRepaired = new Callback [Unit] {
        def pass (v: Unit) = fiber.execute {
          attaching = false
          ready()
        }
        def fail (t: Throwable) = fiber.execute (panic (t))
      }

      def priorDisksUpdated (newDisks: Seq [DiskDrive]) = new Callback [Unit] {

        def pass (v: Unit): Unit = fiber.execute {
          generation = newBoot.gen
          number = newBoot.num
          disks ++= DiskDrives.mapBy (newDisks) (_.id)
          attaching = false
          newDisks foreach (_.engage())
          ready()
          scheduler.execute (cb, ())
        }

        def fail (t: Throwable): Unit = fiber.execute {
          val oldboot = BootBlock (generation, number, priorPaths, roots)
          val latch = Callback.latch (disks.size, priorDisksRepaired)
          disks.values foreach (_.checkpoint (oldboot, latch))
          scheduler.fail (cb, t)
        }}

      val newDisksPrimed = new Callback [Seq [DiskDrive]] {
        def pass (newDisks: Seq [DiskDrive]): Unit = fiber.execute {
          val latch = Callback.latch (disks.size, priorDisksUpdated (newDisks))
          disks.values foreach (_.checkpoint (newBoot, latch))
        }
        def fail (t: Throwable): Unit = fiber.execute {
          ready()
          scheduler.fail (cb, t)
        }}

      if (newPaths exists (priorPaths contains _)) {
        val already = (newPaths -- priorPaths).toSeq.sorted
        ready()
        scheduler.fail (cb, new AlreadyAttachedException (already))
      } else {
        DiskDrives.primeDisks (items, number, newBoot, logd, paged, newDisksPrimed)
      }}

    def checkpoint() {

      val attached = disks.values.map (_.path) .toSet

      def rootPageWritten (newboot: BootBlock, newroots: Position) =
        Callback.latch (disks.size, new Callback [Unit] {
          def pass (v: Unit) = fiber.execute {
            generation = newboot.gen
            roots = newroots
            for (cp <- checkreqs)
              scheduler.execute (cp, ())
            checkreqs = List.empty
            ready()
          }
          def fail (t: Throwable) = fiber.execute (panic (t))
        })

      val rootsWritten =
        new Callback [Position] {
          def pass (newroots: Position) = fiber.execute {
            val newboot = BootBlock (generation+1, number, attached, newroots)
            val latch = rootPageWritten (newboot, newroots)
            disks.values foreach (_.checkpoint (newboot, latch))
          }
          def fail (t: Throwable) = fiber.execute (panic (t))
      }

      checkpoints.checkpoint (generation+1, rootsWritten)
    }

    def launch (checkpoints: CheckpointRegistry, pages: PageRegistry): Unit =
      throw new IllegalStateException

    def attach (items: Seq [AttachItem], cb: Callback [Unit]): Unit =
      guard (cb) {
        if (attaching || !checkreqs.isEmpty)
          attachreqs = attachreqs.enqueue (items, cb)
        else {
          attaching
          attach ((items, cb))
        }
      }

    def checkpoint (cb: Callback [Unit]): Unit =
      guard (cb) {
        val now = !attaching || checkreqs.isEmpty
        checkreqs ::= cb
        if (now) checkpoint()
      }

    override def toString = s"DiskDrives.Launched(attaching=$attaching)"
  }

  class Panicked (t: Throwable) extends State {

    def launch (checkpoints: CheckpointRegistry, pages: PageRegistry): Unit = ()

    def attach (items: Seq [AttachItem], cb: Callback [Unit]): Unit =
      scheduler.fail (cb, new PanickedException (t))

    def checkpoint (cb: Callback [Unit]): Unit =
      scheduler.fail (cb, new PanickedException (t))

    override def toString = s"DiskDrives.Panicked(${t})"
  }

  def replay (records: RecordRegistry, cb: Callback [Unit]): Unit =
    LogIterator.replay (disks.values, records, cb)

  def fetch [G, P] (desc: PageDescriptor [G, P], pos: Position, cb: Callback [P]): Unit =
    disks (pos.disk) .read (desc, pos, cb)

  def launch (checkpoints: CheckpointRegistry, pages: PageRegistry): Unit =
    fiber.execute (state.launch (checkpoints, pages))

  def checkpoint (cb: Callback [Unit]): Unit =
    fiber.execute (state.checkpoint (cb))

  def attach (items: Seq [(Path, File, DiskDriveConfig)], cb: Callback [Unit]): Unit =
    guard (cb) {
      require (!items.isEmpty, "Must list at least one file to attach.")
      fiber.execute (state.attach (items, cb))
    }

  def attach (items: Seq [(Path, DiskDriveConfig)], exec: ExecutorService, cb: Callback [Unit]): Unit =
    guard (cb) {
      val files = items map (DiskDrives.openFile (_, exec))
      attach (files, cb)
    }

  def record [R] (desc: RecordDescriptor [R], entry: R, cb: Callback [Unit]): Unit =
    logd.record (desc, entry, cb)

  def read [G, P] (desc: PageDescriptor [G, P], pos: Position, cb: Callback [P]): Unit =
    cache.read (desc, pos, cb)

  def write [G, P] (desc: PageDescriptor [G, P], group: G, page: P, cb: Callback [Position]): Unit =
    paged.write (desc, group, page, cb)

  override def toString = state.toString
}

private object DiskDrives {

  case class SuperBlocks (path: Path, file: File, sb1: Option [SuperBlock], sb2: Option [SuperBlock])

  def chooseSuperBlock (reads: Seq [SuperBlocks]): Boolean = {

    val sb1 = reads.map (_.sb1) .flatten
    val sb2 = reads.map (_.sb2) .flatten
    if (sb1.size == 0 && sb2.size == 0)
      throw new NoSuperBlocksException

    val gen1 = if (sb1.isEmpty) -1 else sb1.map (_.boot.gen) .max
    val n1 = sb1 count (_.boot.gen == gen1)
    val gen2 = if (sb2.isEmpty) -1 else sb2.map (_.boot.gen) .max
    val n2 = sb2 count (_.boot.gen == gen2)
    if (n1 != reads.size && n2 != reads.size)
      throw new InconsistentSuperBlocksException

    (n1 == reads.size) && (gen1 > gen2 || n2 != reads.size)
  }

  def verifyReattachment (booted: Set [Path], reattaching: Set [Path]) {
    if (!(booted forall (reattaching contains _))) {
      val missing = (booted -- reattaching).toSeq.sorted
      throw new MissingDisksException (missing)
    }
    if (!(reattaching forall (booted contains _))) {
      val extra = (reattaching -- booted).toSeq.sorted
      new ExtraDisksException (extra)
    }}

  def makeDiskDrive (useGen1: Boolean, read: SuperBlocks, logd: LogDispatcher,
      paged: PageDispatcher) (implicit scheduler: Scheduler): DiskDrive = {
    val superb = if (useGen1) read.sb1.get else read.sb2.get
    val disk = new DiskDrive (superb.id, read.path, read.file, superb.config, logd, paged)
    disk.recover (superb)
    disk
  }

  def mapBy [K, V] (i: Iterable [V]) (f: V => K): Map [K, V] = {
    val b = Map.newBuilder [K, V]
    i foreach (v => b += (f (v) -> v))
    b.result
  }

  def superBlocksRead (reads: Seq [SuperBlocks], recovery: RecoveryAgent) {
    import recovery.scheduler

    val useGen1 = chooseSuperBlock (reads)
    val boot = if (useGen1) reads.head.sb1.get.boot else reads.head.sb2.get.boot
    verifyReattachment (boot.disks.toSet, reads .map (_.path) .toSet)

    val logd = new LogDispatcher
    val paged = new PageDispatcher
    val drives = reads map (makeDiskDrive (useGen1, _, logd, paged))
    drives.foreach (_.engage())
    val disks = new DiskDrives (logd, paged, mapBy (drives) (_.id))

    recovery.recover (boot.roots, disks)
  }

  def readSuperBlocks (path: Path, file: File, cb: Callback [SuperBlocks]): Unit =
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

  def reattach (items: Seq [(Path, File)], recovery: RecoveryAgent): Unit =
    guard (recovery.cb) {
      require (!items.isEmpty, "Must list at least one file to reaattach.")
      val allRead = delay (recovery.cb) { reads: Seq [SuperBlocks] =>
        superBlocksRead (reads, recovery)
      }
      val oneRead = Callback.seq (items.size, allRead)
      for ((path, file) <- items)
        readSuperBlocks (path, file, oneRead)
    }

  def reopenFile (path: Path, exec: ExecutorService) = {
    import StandardOpenOption.{READ, WRITE}
    (path, File.open (path, exec, READ, WRITE))
  }

  def reattach (items: Seq [Path], exec: ExecutorService, recovery: RecoveryAgent): Unit =
    guard (recovery.cb) {
      reattach (items map (reopenFile (_, exec)), recovery)
    }

  def primeDisks (items: Seq [(Path, File, DiskDriveConfig)], base: Int, boot: BootBlock,
      logd: LogDispatcher, paged: PageDispatcher, cb: Callback [Seq [DiskDrive]]) (
          implicit scheduler: Scheduler) {

    val latch = Callback.seq (items.size, cb)
    for (((path, file, config), i) <- items zipWithIndex) {
      val disk = new DiskDrive (base+i, path, file, config, logd, paged)
      disk.init (delay (latch) { _ =>
        disk.checkpoint (boot, callback (latch) { _ =>
          disk
        })
      })
    }}

  def attach (items: Seq [(Path, File, DiskDriveConfig)], recovery: RecoveryAgent): Unit =
    guard (recovery.cb) {
      import recovery.scheduler

      val logd = new LogDispatcher
      val paged = new PageDispatcher

      val disksPrimed = delay (recovery.cb) { drives: Seq [DiskDrive] =>
        drives foreach (_.engage())
        val disks = new DiskDrives (logd, paged, mapBy (drives) (_.id))
        recovery.launch (disks)
      }

      val attaching = items.map (_._1) .toSet
      val roots = Position (0, 0, 0)
      val latch = Callback.seq (items.size, disksPrimed)
      val boot = BootBlock.apply (0, items.size, attaching, roots)
      primeDisks (items, 0, boot, logd, paged, disksPrimed)
  }

  def openFile (item: (Path, DiskDriveConfig), exec: ExecutorService) = {
    val (path, config) = item
    import StandardOpenOption.{CREATE, READ, WRITE}
    val file = File.open (path, exec, CREATE, READ, WRITE)
    (path, file, config)
  }

  def attach (items: Seq [(Path, DiskDriveConfig)], exec: ExecutorService, recovery: RecoveryAgent) (
      implicit scheduler: Scheduler): Unit =
    guard (recovery.cb) {
      val files = items map (openFile (_, exec))
      attach (files, recovery)
    }}
