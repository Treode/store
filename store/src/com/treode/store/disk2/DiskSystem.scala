package com.treode.store.disk2

import java.nio.file.{Path, StandardOpenOption}
import java.util.concurrent.ExecutorService
import scala.collection.JavaConversions._
import scala.collection.immutable.Queue

import com.treode.async._
import com.treode.async.io.File
import com.treode.buffer.PagedBuffer
import com.treode.pickle.unpickle

private class DiskSystem (scheduler: Scheduler) {

  case class SuperBlocks (path: Path, file: File, sb1: Option [SuperBlock], sb2: Option [SuperBlock])
  case class Attachment (disks: Seq [DiskDrive], cb: Callback [Unit])

  type Attachments = Queue [Attachment]
  type Checkpoints = List [Callback [Unit]]

  val fiber = new Fiber (scheduler)
  val log = new LogDispatcher (scheduler)
  var disks = Set.empty [DiskDrive]
  var generation = 0
  var state: State = new Opening

  private def readSuperBlocks (path: Path, file: File, cb: Callback [SuperBlocks]): Unit =
    Callback.guard (cb) {

      val buffer = PagedBuffer (SuperBlockBits+1)

      def unpickleSuperBlock (pos: Int): Option [SuperBlock] =
        try {
          buffer.readPos = pos
          Some (unpickle (SuperBlock.pickle, buffer))
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
    def reattach (items: Seq [(Path, File)], cb: Callback [Unit]): Unit
    def attach (items: Seq [DiskDrive], cb: Callback [Unit])
    def checkpoint (cb: Callback [Unit])
  }

  trait QueueAttachAndCheckpoint {

    var attachments: Attachments
    var checkpoints: Checkpoints

    def attach (items: Seq [DiskDrive], cb: Callback [Unit]): Unit =
      Callback.guard (cb) {
        attachments = attachments.enqueue (Attachment (items, cb))
      }

    def checkpoint (cb: Callback [Unit]): Unit =
      checkpoints ::= cb

    def ready (checkpoint: Boolean) {
      if (checkpoint) {
        for (checkpoint <- checkpoints)
          scheduler.execute (checkpoint())
        checkpoints = List.empty
      }
      if (attachments.isEmpty && checkpoints.isEmpty) {
        state = if (disks.size == 0) new Opening else Ready
      } else if (attachments.isEmpty) {
        state = new Checkpointing (checkpoints)
      } else {
        val (first, rest) = attachments.dequeue
        state = new Attaching (first.disks, first.cb, rest, checkpoints)
      }}

    def panic (t: Throwable) {
      state = new Panicked (t)
      for (attachment <- attachments)
        scheduler.execute (attachment.cb.fail (new PanickedException (t)))
      for (checkpoint <- checkpoints)
        scheduler.execute (checkpoint.fail (new PanickedException (t)))
    }}

  trait ReattachCompleted {

    def reattach (items: Seq [(Path, File)], cb: Callback [Unit]): Unit =
      cb.fail (new RecoveryCompletedException)
  }

  class Opening extends State {

    var checkpoints = List.empty [Callback [Unit]]

    def reattach (items: Seq [(Path, File)], cb: Callback [Unit]): Unit =
      state = new Recovering (items, cb, Queue.empty, checkpoints)

    def attach (items: Seq [DiskDrive], cb: Callback [Unit]): Unit =
      Callback.guard (cb) {
        state = new Attaching (items, cb, Queue.empty, checkpoints)
      }

    def checkpoint (cb: Callback [Unit]): Unit =
      checkpoints ::= cb

    override def toString = "Opening"
  }

  class Recovering (
      items: Seq [(Path, File)],
      cb: Callback [Unit],
      var attachments: Attachments,
      var checkpoints: Checkpoints)
  extends State with QueueAttachAndCheckpoint {

    var reads = List.empty [SuperBlocks]
    var thrown = List.empty [Throwable]

    def _superBlocksRead() {
      if (reads.size + thrown.size < items.size) {
        return
      }

      if (thrown.size > 0) {
        panic (MultiException.fit (thrown))
        cb.fail (MultiException.fit (thrown))
        return
      }

      val sb1 = reads.map (_.sb1) .flatten
      val sb2 = reads.map (_.sb2) .flatten
      if (sb1.size == 0 && sb2.size == 0) {
        panic (new NoSuperBlocksException)
        cb.fail (new NoSuperBlocksException)
        return
      }

      val gen1 = if (sb1.isEmpty) -1 else sb1.map (_.boot.gen) .max
      val count1 = sb1 count (_.boot.gen == gen1)
      val gen2 = if (sb2.isEmpty) -1 else sb2.map (_.boot.gen) .max
      val count2 = sb2 count (_.boot.gen == gen2)
      if (count1 != items.size && count2 != items.size) {
        panic (new InconsistentSuperBlocksException)
        cb.fail (new InconsistentSuperBlocksException)
        return
      }

      val useGen1 = (count1 == items.size) && (gen1 > gen2 || count2 != items.size)
      val boot = if (useGen1) reads.head.sb1.get.boot else reads.head.sb2.get.boot

      val reattaching = items .map (_._1) .toSet
      val attached = boot.disks.toSet
      if (!(attached forall (reattaching contains _))) {
        val missing = (attached -- reattaching).toSeq.sorted
        panic (new MissingDisksException (missing))
        cb.fail (new MissingDisksException (missing))
        return
      }
      if (!(reattaching forall (attached contains _))) {
        val extra = (reattaching -- attached).toSeq.sorted
        panic (new ExtraDisksException (extra))
        cb.fail (new ExtraDisksException (extra))
        return
      }

      for (read <- reads) {
        val superblock = if (useGen1) read.sb1.get else read.sb2.get
        val disk = new DiskDrive (read.path, read.file, superblock.config, scheduler, log)
        disk.recover (superblock)
        disks += disk
      }

      generation = boot.gen

      disks foreach (_.engage())
      ready (false)
      cb()
    }

    val superBlocksRead = new Callback [SuperBlocks] {
      def pass (v: SuperBlocks): Unit = fiber.execute {
        reads ::= v
        _superBlocksRead()
      }
      def fail (t: Throwable): Unit = fiber.execute {
        thrown ::= t
        _superBlocksRead()
      }}

    for ((path, file) <- items)
      readSuperBlocks (path, file, superBlocksRead)

    def reattach (items: Seq [(Path, File)], cb: Callback [Unit]): Unit =
      cb.fail (new ReattachmentPendingException)

    override def toString = "Recovering"
  }

  class Attaching (
      items: Seq [DiskDrive],
      cb: Callback [Unit],
      var attachments: Attachments,
      var checkpoints: Checkpoints)
  extends State with QueueAttachAndCheckpoint with ReattachCompleted {

    val attached = disks .map (_.path)
    val attaching = items .map (_.path) .toSet
    val boot = BootBlock (generation+1, attached ++ attaching)

    def latch4 = Callback.latch (disks.size, new Callback [Unit] {
      def pass (v: Unit) = fiber.execute (ready (true))
      def fail (t: Throwable) = fiber.execute (panic (t))
    })

    def latch3 = Callback.latch (disks.size, new Callback [Unit] {
      def pass (v: Unit): Unit = fiber.execute {
        require (generation == boot.gen-1)
        generation = boot.gen
        disks ++= items
        items foreach (_.engage())
        ready (true)
        cb()
      }
      def fail (t: Throwable): Unit = fiber.execute {
        val reboot = BootBlock (generation, attached)
        val latch = latch4
        disks foreach (_.checkpoint (reboot, latch))
        cb.fail (t)
      }})

    def latch2 = Callback.latch (items.size, new Callback [Unit] {
      def pass (v: Unit): Unit = fiber.execute {
        val latch = latch3
        disks foreach (_.checkpoint (boot, latch))
      }
      def fail (t: Throwable): Unit = fiber.execute {
        ready (false)
        cb.fail (t)
      }})

    def latch1 = Callback.latch (items.size, new Callback [Unit] {
      def pass (v: Unit): Unit = fiber.execute {
        val latch = latch2
        items foreach (_.checkpoint (boot, latch))
      }
      def fail (t: Throwable): Unit = fiber.execute {
        ready (false)
        cb.fail (t)
      }
    })

    if (attaching exists (attached contains _)) {
      val already = (attaching -- attached).toSeq.sorted
      ready (false)
      cb.fail (new AlreadyAttachedException (already))
    } else {
      val latch = latch1
      items foreach (_.init (latch))
    }

    override def toString = "Attaching"
  }

  class Checkpointing (var checkpoints: Checkpoints)
  extends State with QueueAttachAndCheckpoint with ReattachCompleted {

    var attachments = Queue.empty [Attachment]

    val latch = Callback.latch (disks.size, new Callback [Unit] {
      def pass (v: Unit) = fiber.execute (ready (true))
      def fail (t: Throwable) = fiber.execute (panic (t))
    })

    generation += 1
    val attached = disks map (_.path)
    val boot = BootBlock (generation, attached)
    disks foreach (_.checkpoint (boot, latch))
  }

  class Panicked (t: Throwable) extends State {

    def reattach (items: Seq [(Path, File)], cb: Callback [Unit]): Unit =
      cb.fail (new PanickedException (t))

    def attach (items: Seq [DiskDrive], cb: Callback [Unit]): Unit =
      cb.fail (new PanickedException (t))

    def checkpoint (cb: Callback [Unit]): Unit =
      cb.fail (new PanickedException (t))

    override def toString = s"Panicked(${t})"
  }

  object Ready extends State with ReattachCompleted {

    def attach (items: Seq [DiskDrive], cb: Callback [Unit]): Unit =
      Callback.guard (cb) {
        state = new Attaching (items, cb, Queue.empty, List.empty)
      }

    def checkpoint (cb: Callback [Unit]): Unit =
      state = new Checkpointing (List (cb))

    override def toString = "Ready"
  }

  private def openFile (path: Path, exec: ExecutorService): (Path, File) = {
    import StandardOpenOption.{READ, WRITE}
    (path, File.open (path, Set (READ, WRITE), Set.empty, exec))
  }

  def _reattach (items: Seq [(Path, File)], cb: Callback [Unit]): Unit =
    Callback.guard (cb) {
      require (!items.isEmpty, "Must list at least one file to reaattach.")
      fiber.execute (state.reattach (items, cb))
    }

  def reattach (items: Seq [Path], executor: ExecutorService, cb: Callback [Unit]): Unit =
    Callback.guard (cb) {
      val files = items map (openFile (_, executor))
      fiber.execute (state.reattach (files, cb))
    }

  private def makeDisks (items: Seq [(Path, File, DiskDriveConfig)]): Seq [DiskDrive] =
    for ((path, file, config) <- items) yield
      new DiskDrive (path, file, config, scheduler, log)

  private def openDisk (item: (Path, DiskDriveConfig), exec: ExecutorService): DiskDrive = {
    val (path, config) = item
    import StandardOpenOption.{CREATE, READ, WRITE}
    val file = File.open (path, Set (CREATE, READ, WRITE), Set.empty, exec)
    new DiskDrive (path, file, config, scheduler, log)
  }

  private def openDisks (items: Seq [(Path, DiskDriveConfig)], exec: ExecutorService): Seq [DiskDrive] =
    items map (openDisk (_, exec))

  def _attach (items: Seq [(Path, File, DiskDriveConfig)], cb: Callback [Unit]): Unit =
    Callback.guard (cb) {
      require (!items.isEmpty, "Must list at least one file to attach.")
      val disks = makeDisks (items)
      fiber.execute (state.attach (disks, cb))
    }

  def attach (items: Seq [(Path, DiskDriveConfig)], exec: ExecutorService, cb: Callback [Unit]): Unit =
    Callback.guard (cb) {
      require (!items.isEmpty, "Must llist at least one path to attach.")
      val disks = openDisks (items, exec)
      fiber.execute (state.attach (disks, cb))
    }

  def checkpoint (cb: Callback [Unit]): Unit =
    fiber.execute (state.checkpoint (cb))

  override def toString = state.toString
}
