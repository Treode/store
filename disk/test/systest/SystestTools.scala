package systest

import scala.util.Random
import com.treode.async._
import org.scalatest.Assertions

import Assertions._

object SystestTools {

  implicit class RichRandom (random: Random) {

    def nextPut (nkeys: Int, nputs: Int): Seq [(Int, Int)] =
      Seq.fill (nputs) (random.nextInt (nkeys), random.nextInt (Int.MaxValue))
  }

  implicit class RichTable (table: Table) {

    def getAndPass (key: Int) (implicit scheduler: StubScheduler): Option [Int] = {
      val cb = new CallbackCaptor [Option [Int]]
      table.get (key, cb)
      scheduler.runTasks()
      cb.passed
    }

    def putAndPass (kvs: (Int, Int)*) (implicit scheduler: StubScheduler) {
      val cb = new CallbackCaptor [Unit]
      val latch = Callback.latch (kvs.size, cb)
      for ((key, value) <- kvs)
        table.put (key, value, latch)
      scheduler.runTasks()
      cb.passed
    }

    def deleteAndPass (ks: Int*) (implicit scheduler: StubScheduler) {
      val cb = new CallbackCaptor [Unit]
      for (key <- ks)
        table.delete (key, cb)
      scheduler.runTasks()
      cb.passed
    }

    def toMap (implicit scheduler: StubScheduler): Map [Int, Int] = {
      val builder = Map.newBuilder [Int, Int]
      val cb = new CallbackCaptor [Unit]
      table.iterator (continue (cb) { iter =>
        AsyncIterator.foreach (iter, cb) { case (cell, cb) =>
          invoke (cb) {
            if (cell.value.isDefined)
              builder += cell.key -> cell.value.get
          }}})
      scheduler.runTasks()
      cb.passed
      builder.result
    }

    def toSeq  (implicit scheduler: StubScheduler): Seq [(Int, Int)] = {
      val builder = Seq.newBuilder [(Int, Int)]
      val cb = new CallbackCaptor [Unit]
      table.iterator (continue (cb) { iter =>
        AsyncIterator.foreach (iter, cb) { case (cell, cb) =>
          invoke (cb) {
            if (cell.value.isDefined)
              builder += cell.key -> cell.value.get
          }}})
      scheduler.runTasks()
      cb.passed
      builder.result
    }

    def expectNone (key: Int) (implicit scheduler: StubScheduler): Unit =
      expectResult (None) (getAndPass (key))

    def expectValue (key: Int, value: Int) (implicit scheduler: StubScheduler): Unit =
      expectResult (Some (value)) (getAndPass (key))

    def expectValues (kvs: (Int, Int)*) (implicit scheduler: StubScheduler): Unit =
      expectResult (kvs.sorted) (toSeq)
  }

  implicit class RichSynthTable (table: SynthTable) (implicit scheduler: StubScheduler) {

    def checkpointAndPass(): Tiers = {
      val cb = new CallbackCaptor [Tiers]
      table.checkpoint (cb)
      scheduler.runTasks()
      cb.passed
    }}

  class TrackedTable (table: Table, tracker: TrackingTable) extends Table {

    def get (key: Int, cb: Callback [Option [Int]]): Unit =
      table.get (key, cb)

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
