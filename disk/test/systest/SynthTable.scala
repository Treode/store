package systest

import java.util.concurrent.locks.ReentrantReadWriteLock

import com.treode.async.{AsyncIterator, Callback, callback, continue}
import com.treode.disk._
import com.treode.pickle.Picklers

class SynthTable (
    lock: ReentrantReadWriteLock,
    var gen: Long,
    var primary: MemTier,
    var secondary: MemTier,
    var tiers: Tiers) (
        implicit disks: Disks, config: TestConfig) extends Table with PageHandler [Long] {

  private val readLock = lock.readLock()
  private val writeLock = lock.writeLock()

  private def read (key: Int, cb: Callback [Option [Int]]) {

    readLock.lock()
    val (primary, secondary, tiers) = try {
      (this.primary, this.secondary, this.tiers)
    } finally {
      readLock.unlock()
    }

    val keyCell = Cell (key, None)

    var cell = primary.floor (keyCell)
    if (cell != null && cell.key == key) {
      cb (cell.value)
      return
    }

    cell = secondary.floor (keyCell)
    if (cell != null && cell.key == key) {
      cb (cell.value)
      return
    }

    var i = 0
    val loop = new Callback [Option [Cell]] {

      def pass (cell: Option [Cell]) {
        cell match {
          case Some (cell) => cb (cell.value)
          case None =>
            i += 1
            if (i < tiers.size)
              tiers (i) .read (key, this)
            else
              cb (None)
        }}

      def fail (t: Throwable) = cb.fail (t)
    }

    if (i < tiers.size)
      tiers (i) .read (key, loop)
    else
      cb (None)
  }

  def get (key: Int, cb: Callback [Option [Int]]): Unit =
    read (key, cb)

  def iterator (cb: Callback [CellIterator]) {

    readLock.lock()
    val (primary, secondary, tiers) = try {
      (this.primary, this.secondary, this.tiers)
    } finally {
      readLock.unlock()
    }

    TierIterator.merge (primary, secondary, tiers, cb)
  }

  private def update (key: Int, value: Option [Int], cb: Callback [Unit]) {
    readLock.lock()
    try {
      primary.add (Cell (key, value))
    } finally {
      readLock.unlock()
    }
    SynthTable.update.record (gen, key, value) (cb)
  }

  def put (key: Int, value: Int, cb: Callback [Unit]): Unit =
    update (key, Some (value), cb)

  def delete (key: Int, cb: Callback [Unit]): Unit =
    update (key, None, cb)

  def probe (groups: Set [Long], cb: Callback [Set [Long]]): Unit =
    cb (groups intersect tiers.active)

  def compact (groups: Set [Long], cb: Callback [Unit]) {
    checkpoint (callback (cb) (_ => ()))
  }

  def checkpoint (cb: Callback [Tiers]) {

    val epoch = disks.join (cb)

    writeLock.lock()
    val (generation, primary, tiers) = try {
      require (secondary.isEmpty)
      val g = this.gen
      val p = this.primary
      this.gen += 1
      this.primary = secondary
      this.secondary = p
      (g, p, this.tiers)
    } finally {
      writeLock.unlock()
    }

    val built = continue (epoch) { tier: Tier =>
      writeLock.lock()
      val meta = try {
        this.secondary = newMemTier
        this.tiers = Tiers (tier)
        this.tiers
      } finally {
        writeLock.unlock()
      }
      epoch (meta)
    }

    val merged = continue (epoch) { iter: CellIterator =>
      TierBuilder.build (generation, iter, built)
    }

    TierIterator.merge (primary, emptyMemTier, tiers, merged)
  }}

object SynthTable {

  val root = {
    import Picklers._
    val tiers = Tiers.pickler
    new RootDescriptor (0x2B30D8AF, tiers)
  }

  val update = {
    import Picklers._
    new RecordDescriptor (0x6AC99D09, tuple (ulong, int, option (int)))
  }

  val compact = {
    import Picklers._
    val tiers = Tiers.pickler
    new RecordDescriptor (0xA67C3DD1, tiers)
  }

  def recover (cb: Callback [Table]) (implicit recovery: Recovery, config: TestConfig) {

    val medic = new SynthMedic

    root.reload { tiers => implicit reload =>
      medic.reload (tiers)
      reload.ready()
    }

    update.replay ((medic.update _).tupled)

    compact.replay (medic.compact _)

    recovery.launch { implicit launch =>
      import launch.disks
      val table = medic.close()
      root.checkpoint (table.checkpoint _)
      TierPage.pager.handle (table)
      cb (table)
      launch.ready()
    }}
}
