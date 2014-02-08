package systest

import java.nio.file.Paths
import com.treode.async._
import com.treode.async.io.StubFile
import com.treode.disk.{Disks, DiskDriveConfig}
import org.scalatest.FlatSpec

import SystemSpec._

class SystemSpec extends FlatSpec {

  def setup (disk: StubFile, config: DiskDriveConfig) (
      implicit scheduler: StubScheduler, testConfig: TestConfig): Table = {

      implicit val recovery = Disks.recover()
      val tableCb = new CallbackCaptor [Table]
      Table.recover (tableCb)
      val disksCb = new CallbackCaptor [Disks]
      recovery.attach (Seq ((Paths.get ("a"), disk, config)), disksCb)
      scheduler.runTasks()
      disksCb.passed
      tableCb.passed
  }

  def recover (disk: StubFile) (
      implicit scheduler: StubScheduler, testConfig: TestConfig): Table = {

      implicit val recovery = Disks.recover()
      val tableCb = new CallbackCaptor [Table]
      Table.recover (tableCb)
      val disksCb = new CallbackCaptor [Disks]
      recovery.reattach (Seq ((Paths.get ("a"), disk)), disksCb)
      scheduler.runTasks()
      disksCb.passed
      tableCb.passed
  }

  "It" should "work" in {

    implicit val testConfig = new TestConfig (1<<12)
    val diskDriveConfig = DiskDriveConfig (20, 16, 1<<30)

    implicit val scheduler = StubScheduler.random()
    val disk = new StubFile
    val tracker = new TrackingTable

    {
      val _table = setup (disk, diskDriveConfig)
      val table = new TrackedTable (_table, tracker)
      table.putAndPass (1, 1)
      println (table.toMap)
    }

    {
      val table = recover (disk)
      println (table.toMap)
    }}}

object SystemSpec {

  implicit class RichTable (table: Table) (implicit scheduler: StubScheduler) {

    def putAndPass (key: Int, value: Int) {
      val cb = new CallbackCaptor [Unit]
      table.put (key, value, cb)
      scheduler.runTasks()
      cb.passed
    }

    def toMap(): Map [Int, Int] = {
      val builder = Map.newBuilder [Int, Int]
      val cb = new CallbackCaptor [Unit]
      table.iterator (continue (cb) { iter =>
        AsyncIterator.foreach (iter, cb) { case (cell, cb) =>
          invoke (cb) {
            builder += cell.key -> cell.value.get
          }}})
      scheduler.runTasks()
      builder.result
    }}

  class TrackedTable (table: Table, tracker: TrackingTable) extends Table {

    def put (key: Int, value: Int, cb: Callback [Unit]) {
      tracker.putting (key, value)
      table.put (key, value, callback (cb) { _ =>
        tracker.put (key, value)
      })
    }

    def delete (key: Int, cb: Callback [Unit]) {
      tracker.deleting (key)
      table.delete (key, callback (cb) { _ =>
        tracker.deleted (key)
      })
    }

    def iterator (cb: Callback [CellIterator]): Unit =
      table.iterator (cb)
  }

  class TrackingTable {

    private var attempted = Map.empty [Int, Int]
    private var accepted = Map.empty [Int, Int]

    def putting (key: Int, value: Int): Unit =
      attempted += (key -> value)

    def put (key: Int, value: Int): Unit =
      accepted += (key -> value)

    def deleting (key: Int): Unit =
      attempted -= key

    def deleted (key: Int): Unit =
      accepted -= key

    def check (recovered: Map [Int, Int]) {
      var okay = true
      for ((key, value) <- recovered)
        okay &&= (accepted.get (key) == Some (value) || attempted.get (key) == Some (value))
      assert (okay, s"Bad recovery.\n$attempted\n$accepted\n$recovered")
    }}
}
