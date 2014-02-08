package systest

import java.nio.file.Paths
import com.treode.async.{CallbackCaptor, StubScheduler}
import com.treode.async.io.StubFile
import com.treode.disk.{Disks, DiskDriveConfig}
import org.scalatest.FlatSpec

import SystestTools._

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
