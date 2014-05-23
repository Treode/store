package com.treode.store.paxos

import scala.util.Random

import com.treode.async.{Async, Scheduler}
import com.treode.async.implicits._
import com.treode.async.stubs.StubScheduler
import com.treode.async.stubs.implicits._
import com.treode.store.{Bytes, Cell}
import PaxosTestTools._

class PaxosTracker {

  private var attempted = Map .empty [Bytes, Set [Int]] .withDefaultValue (Set.empty)
  private var accepted = Map.empty [Bytes, Int] .withDefaultValue (-1)

  private def proposing (key: Bytes, value: Int): Unit =
    synchronized {
      if (!(accepted contains key))
        attempted += key -> (attempted (key) + value)
    }

  private def learned (key: Bytes, value: Int): Unit =
    synchronized {
      attempted -= key
      if (accepted contains key)
        assert (accepted (key) == value, "Paxos conflicts")
      else
        accepted += key -> value
    }

  def propose (host: StubPaxosHost, key: Bytes, value: Int): Async [Unit] = {
    require (value >= 0)
    proposing (key, value)
    for {
      got <- host.propose (key, 0, Bytes (value))
    } yield {
      learned (key, got.int)
    }}

  def batch (nputs: Int, hs: StubPaxosHost*) (implicit random: Random): Async [Unit] = {
    val khs = for (k <- random.nextKeys (nputs); h <- hs) yield (k, h)
    for ((k, h) <- khs.latch.unit)
      propose (h, k, random.nextInt (1<<20))
  }

  def batches (nbatches: Int, nputs: Int, hs: StubPaxosHost*) (
      implicit random: Random, scheduler: Scheduler): Async [Unit] =
    for (n <- (0 until nbatches) .async)
      batch (nputs, hs:_*)

  def check (host: StubPaxosHost) (implicit scheduler: Scheduler): Async [Unit] =
    for {
      _ <- for ((key, value) <- accepted.async) {
            for {
              _found <- host.propose (key, 0, Bytes (-1))
            } yield {
              val found = _found.int
              assert (
                  found == value,
                  s"Expected ${key.long} to be $value, found $found.")
            }}
      _ <- for ((key, values) <- attempted.async; if !(accepted contains key)) {
            for {
              _found <- host.propose (key, 0, Bytes (-1))
            } yield {
              val found = _found.int
              assert (
                  found == -1 || (values contains found),
                  s"Expected ${key.long} to be one of $values, found $found")
            }}
    } yield ()

  def check (cells: Seq [Cell]) {
    val all = cells.map (c => (c.key, c.value.get.int)) .toMap.withDefaultValue (-1)
    for ((key, value) <- accepted) {
      val found = all (key)
      assert (
          found == value,
          s"Expected ${key.long} to be $value, found $found.")
    }
    for ((key, found) <- all; if !(accepted contains key)) {
      val values = attempted (key)
      assert (
          values contains found,
          s"Expected ${key.long} to be one of $values, found $found")

    }}}

