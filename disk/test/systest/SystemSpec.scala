package systest

import java.nio.file.Paths
import scala.util.Random

import com.treode.async.{CallbackCaptor, StubScheduler}
import com.treode.async.io.StubFile
import com.treode.disk.{Disks, DisksConfig, DiskGeometry}
import org.scalatest.FlatSpec

import SystestTools._

class SystemSpec extends FlatSpec {

  def setup (disk: StubFile, geometry: DiskGeometry) (
      implicit scheduler: StubScheduler, testConfig: TestConfig): Table = {

      implicit val disksConfig = DisksConfig (14, 1<<24, 1<<16, 10, 1)
      implicit val recovery = Disks.recover()
      val tableCb = CallbackCaptor [Table]
      Table.recover (tableCb)
      val disksCb = CallbackCaptor [Disks]
      recovery.attach (Seq ((Paths.get ("a"), disk, geometry)), disksCb)
      scheduler.runTasks()
      disksCb.passed
      tableCb.passed
  }

  def recover (disk: StubFile) (
      implicit scheduler: StubScheduler, testConfig: TestConfig): Table = {

    implicit val config = DisksConfig (14, 1<<24, 1<<16, 10, 1)
    implicit val recovery = Disks.recover()
    val tableCb = CallbackCaptor [Table]
    Table.recover (tableCb)
    val disksCb = CallbackCaptor [Disks]
    recovery.reattach (Seq ((Paths.get ("a"), disk)), disksCb)
    scheduler.runTasks()
    disksCb.passed
    tableCb.passed
  }

  "It" should "work" in {

    implicit val testConfig = new TestConfig (1<<12)
    implicit val config = DisksConfig (14, 1<<24, 1<<16, 10, 1)
    val geometry = DiskGeometry (20, 13, 1<<30)

    implicit val random = new Random (0)
    implicit val scheduler = StubScheduler.random (random)
    val disk = new StubFile
    val tracker = new TrackingTable

    {
      val _table = setup (disk, geometry)
      val table = new TrackedTable (_table, tracker)
      table.putAndPass (random.nextPut (10000, 1000): _*)
    }

    {
      val table = recover (disk)
      tracker.check (table.toMap)
    }}}
