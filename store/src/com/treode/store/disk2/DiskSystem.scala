package com.treode.store.disk2

import java.nio.file.{Path, StandardOpenOption}
import java.util.concurrent.ExecutorService
import scala.collection.JavaConversions._
import scala.collection.immutable.Queue

import com.treode.async._
import com.treode.async.io.File
import com.treode.buffer.PagedBuffer
import com.treode.pickle.unpickle

private class DiskSystem (scheduler: Scheduler, executor: ExecutorService) {

  case class SuperBlocks (path: Path, file: File, sb1: Option [SuperBlock], sb2: Option [SuperBlock])
  case class Attachment (disks: Seq [Disk], cb: Callback [Unit])

  type Attachments = Queue [Attachment]
  type Checkpoints = List [Callback [Unit]]

  val fiber = new Fiber (scheduler)
  var disks = Set.empty [Disk]
  var generation = 0
  var state: State = new Opening

  private def initFile (item: (Path, File, DiskConfig)): Disk = {
    val (path, file, config) = item
    val disk = new Disk (path, file, config)
    disk.init()
    disk
  }

  private def initFiles (items: Seq [(Path, File, DiskConfig)]): Seq [Disk] =
    items map (initFile _)

  private def initPath (item: (Path, DiskConfig)): Disk = {
    val (path, config) = item
    import StandardOpenOption.{CREATE, READ, WRITE}
    require (disks forall (_.path != path), s"Path $path is already attached.")
    val file = File.open (path, Set (CREATE, READ, WRITE), Set.empty, executor)
    initFile (path, file, config)
  }

  private def initPaths (items: Seq [(Path, DiskConfig)]): Seq [Disk] =
    items map (initPath _)

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

  private def readSuperBlocks (path: Path, cb: Callback [SuperBlocks]): Unit =
    Callback.guard (cb) {
      import StandardOpenOption.{READ, WRITE}
      val file = File.open (path, Set (READ, WRITE), Set.empty, executor)
      readSuperBlocks (path, file, cb)
    }

  trait State {
    def _reattach (items: Seq [(Path, File)], cb: Callback [Unit]): Unit
    def reattach (items: Seq [Path], cb: Callback [Unit]): Unit
    def _attach (items: Seq [(Path, File, DiskConfig)], cb: Callback [Unit])
    def attach (items: Seq [(Path, DiskConfig)], cb: Callback [Unit])
    def checkpoint (cb: Callback [Unit])
  }

  trait QueueAttachAndCheckpoint {

    var attachments: Attachments
    var checkpoints: Checkpoints

    def _attach (items: Seq [(Path, File, DiskConfig)], cb: Callback [Unit]): Unit =
      Callback.guard (cb) {
       attachments = attachments.enqueue (Attachment (initFiles (items), cb))
      }

    def attach (items: Seq [(Path, DiskConfig)], cb: Callback [Unit]): Unit =
      Callback.guard (cb) {
        attachments = attachments.enqueue (Attachment (initPaths (items), cb))
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

    def _reattach (items: Seq [(Path, File)], cb: Callback [Unit]): Unit =
      cb.fail (new RecoveryCompletedException)

    def reattach (items: Seq [Path], cb: Callback [Unit]): Unit =
      cb.fail (new RecoveryCompletedException)
  }

  class Opening extends State {

    var checkpoints = List.empty [Callback [Unit]]

    def _reattach (items: Seq [(Path, File)], cb: Callback [Unit]): Unit =
      Callback.guard (cb) {
        require (!items.isEmpty, "Must at least one file to reattach.")
        val next = new Recovering (cb, Queue.empty, checkpoints)
        state = next
        next._reattach (items)
      }

    def reattach (items: Seq [Path], cb: Callback [Unit]): Unit =
      Callback.guard (cb) {
        require (!items.isEmpty, "Must at least one path to reattach.")
        val next = new Recovering (cb, Queue.empty, checkpoints)
        state = next
        next.reattach (items)
      }

    def _attach (items: Seq [(Path, File, DiskConfig)], cb: Callback [Unit]): Unit =
      Callback.guard (cb) {
        state = new Attaching (initFiles (items), cb, Queue.empty, checkpoints)
      }

    def attach (items: Seq [(Path, DiskConfig)], cb: Callback [Unit]): Unit =
      Callback.guard (cb) {
        state = new Attaching (initPaths (items), cb, Queue.empty, checkpoints)
      }

    def checkpoint (cb: Callback [Unit]): Unit =
      checkpoints ::= cb

    override def toString = "Opening"
  }

  class Recovering (
      cb: Callback [Unit],
      var attachments: Attachments,
      var checkpoints: Checkpoints)
  extends State with QueueAttachAndCheckpoint {

    var reattaching = Set.empty [Path]
    var count = 0
    var reads = List.empty [SuperBlocks]
    var thrown = List.empty [Throwable]

    def _superBlocksRead() {
      if (reads.size + thrown.size < count) {
        return
      }

      if (thrown.size > 0) {
        panic (MultiException (thrown))
        cb.fail (MultiException (thrown))
        return
      }

      val sb1 = reads.map (_.sb1) .flatten
      val sb2 = reads.map (_.sb2) .flatten
      if (sb1.size == 0 && sb2.size == 0) {
        panic (new NoSuperBlocksException)
        cb.fail (new NoSuperBlocksException)
        return
      }

      val gen1 = if (sb1.isEmpty) -1 else sb1.map (_.gen) .max
      val count1 = sb1 count (_.boot.gen == gen1)
      val gen2 = if (sb2.isEmpty) -1 else sb2.map (_.gen) .max
      val count2 = sb2 count (_.boot.gen == gen2)
      if (count1 != count && count2 != count) {
        panic (new InconsistentSuperBlocksException)
        cb.fail (new InconsistentSuperBlocksException)
        return
      }

      val useGen1 = (count1 == count) && (gen1 > gen2 || count2 != count)
      val boot = if (useGen1) reads.head.sb1.get.boot else reads.head.sb2.get.boot

      val unseen = boot.disks -- reattaching
      if (!unseen.isEmpty) {
        reattaching ++= unseen
        count += unseen.size
        unseen foreach (readSuperBlocks (_, superBlocksRead))
        return
      }

      for (read <- reads) {
        val superblock = if (useGen1) read.sb1.get else read.sb2.get
        val disk = new Disk (read.path, read.file, superblock.config)
        disk.recover (superblock)
        disks += disk
      }

      generation = boot.gen

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

    def _reattach (items: Seq [(Path, File)]) {
      reattaching ++= items map (_._1)
      count += items.size
      for ((path, file) <- items)
        readSuperBlocks (path, file, superBlocksRead)
    }

    def reattach (items: Seq [Path]) {
      val unseen = items.toSet -- reattaching
      reattaching ++= unseen
      count += unseen.size
      unseen foreach (readSuperBlocks (_, superBlocksRead))
    }

    def _reattach (items: Seq [(Path, File)], cb: Callback [Unit]): Unit =
      cb.fail (new ReattachmentPendingException)

    def reattach (items: Seq [Path], cb: Callback [Unit]): Unit =
      cb.fail (new ReattachmentPendingException)

    override def toString = "Recovering"
  }

  class Attaching (
      items: Seq [Disk], cb: Callback [Unit],
      var attachments: Attachments,
      var checkpoints: Checkpoints)
  extends State with QueueAttachAndCheckpoint with ReattachCompleted {

    val attached = disks map (_.path)
    val attaching = items map (_.path)
    val boot = BootBlock (generation+1, attached ++ attaching)

    def latch3 = Callback.latch (disks.size, new Callback [Unit] {
      def pass (v: Unit) = fiber.execute (ready (true))
      def fail (t: Throwable) = fiber.execute (panic (t))
    })

    def latch2 = Callback.latch (disks.size, new Callback [Unit] {
      def pass (v: Unit): Unit = fiber.execute {
        require (generation == boot.gen-1)
        generation = boot.gen
        disks ++= items
        ready (true)
        cb()
      }
      def fail (t: Throwable): Unit = fiber.execute {
        val reboot = BootBlock (generation, attached)
        val _latch3 = latch3
        disks foreach (_.checkpoint (reboot, _latch3))
        cb.fail (t)
      }})

    val latch1 = Callback.latch (items.size, new Callback [Unit] {
      def pass (v: Unit): Unit = fiber.execute {
        val _latch2 = latch2
        disks foreach (_.checkpoint (boot, _latch2))
      }
      def fail (t: Throwable): Unit = fiber.execute {
        ready (false)
        cb.fail (t)
      }})

    items foreach (_.checkpoint (boot, latch1))

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

    def _reattach (items: Seq [(Path, File)], cb: Callback [Unit]): Unit =
      cb.fail (new PanickedException (t))

    def reattach (items: Seq [Path], cb: Callback [Unit]): Unit =
      cb.fail (new PanickedException (t))

    def _attach (items: Seq [(Path, File, DiskConfig)], cb: Callback [Unit]): Unit =
      cb.fail (new PanickedException (t))

    def attach (items: Seq [(Path, DiskConfig)], cb: Callback [Unit]): Unit =
      cb.fail (new PanickedException (t))

    def checkpoint (cb: Callback [Unit]): Unit =
      cb.fail (new PanickedException (t))

    override def toString = s"Panicked(${t})"
  }

  object Ready extends State with ReattachCompleted {

    def _attach (items: Seq [(Path, File, DiskConfig)], cb: Callback [Unit]): Unit =
      Callback.guard (cb) {
        state = new Attaching (initFiles (items), cb, Queue.empty, List.empty)
      }

    def attach (items: Seq [(Path, DiskConfig)], cb: Callback [Unit]): Unit =
      Callback.guard (cb) {
        state = new Attaching (initPaths (items), cb, Queue.empty, List.empty)
      }

    def checkpoint (cb: Callback [Unit]): Unit =
      state = new Checkpointing (List (cb))

    override def toString = "Ready"
  }

  def _reattach (items: Seq [(Path, File)], cb: Callback [Unit]): Unit =
    fiber.execute (state._reattach (items, cb))

  def reattach (items: Seq [Path], cb: Callback [Unit]): Unit =
    fiber.execute (state.reattach (items, cb))

  def _attach (items: Seq [(Path, File, DiskConfig)], cb: Callback [Unit]): Unit =
    fiber.execute (state._attach (items, cb))

  def attach (items: Seq [(Path, DiskConfig)], cb: Callback [Unit]): Unit =
    fiber.execute (state.attach (items, cb))

  def checkpoint (cb: Callback [Unit]): Unit =
    fiber.execute (state.checkpoint (cb))

  override def toString = state.toString
}
