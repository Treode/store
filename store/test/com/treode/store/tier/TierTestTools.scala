package com.treode.store.tier

import com.treode.async.{Async, StubScheduler}
import com.treode.store.{Bytes, TimedTestTools}
import org.scalatest.Assertions

import Assertions.assertResult
import Async.async

private object TierTestTools extends TimedTestTools {

  implicit class TierRichInt (v: Int) {
    def :: (k: Bytes): TierCell = TierCell (k, Some (Bytes (v)))
  }

  implicit class TierRichOption (v: Option [Bytes]) {
    def :: (k: Bytes): TierCell = TierCell (k, v)
  }

  implicit class RichTable (table: TierTable) (implicit scheduler: StubScheduler) {

    def ceiling (key: Int, limit: Int) (implicit scheduler: StubScheduler): (Int, Option [Int]) = {
      val c = table.ceiling (Bytes (key), Bytes (limit)) .pass
      (c.key.int, c.value.map (_.int))
    }

    def get (key: Int) (implicit scheduler: StubScheduler): Option [Int] =
      table.get (Bytes (key)) .pass.map (_.int)

    def putAll (kvs: (Int, Int)*) {
      for ((key, value) <- kvs)
        table.put (Bytes (key), Bytes (value))
      scheduler.runTasks()
    }

    def deleteAll (ks: Int*) {
      for (key <- ks)
        table.delete (Bytes (key))
      scheduler.runTasks()
    }

    def toSeq  (implicit scheduler: StubScheduler): Seq [(Int, Int)] =
      for (c <- table.iterator.toSeq; if c.value.isDefined)
        yield (c.key.int, c.value.get.int)

    def toMap (implicit scheduler: StubScheduler): Map [Int, Int] =
      toSeq.toMap

    def expectNone (key: Int): Unit =
      assertResult (None) (get (key))

    def expectValue (key: Int, value: Int): Unit =
      assertResult (Some (value)) (get (key))

    def expectCeiling (key: Int, limit: Int, found: Int, value: Int): Unit =
      assertResult (found -> Some (value)) (ceiling (key, limit))

    def expectNoCeiling (key: Int, limit: Int, found: Int): Unit =
      assertResult (found -> None) (ceiling (key, limit))

    def expectValues (kvs: (Int, Int)*): Unit =
      assertResult (kvs.sorted) (toSeq)
  }}
