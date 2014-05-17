package com.treode.store.paxos

import scala.util.Random

import com.treode.async.{Async, Scheduler}
import com.treode.async.implicits._
import com.treode.async.stubs.StubScheduler
import com.treode.async.stubs.implicits._
import com.treode.store.Bytes

import PaxosTestTools._

class PaxosTracker {

  private var attempted = Map .empty [Bytes, Set [Int]] .withDefaultValue (Set.empty)
  private var accepted = Map.empty [Bytes, Int] .withDefaultValue (-1)

  private def proposing (key: Bytes, value: Int): Unit =
    synchronized {
      attempted += key -> (attempted (key) + value)
    }

  private def learned (key: Bytes, value: Int): Unit =
    synchronized {
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

  def batch (host: StubPaxosHost, nputs: Int) (implicit random: Random): Async [Unit] =
    for (k <- random.nextKeys (nputs) .latch.unit)
      propose (host, k, random.nextInt (1<<20))

  def batches (host: StubPaxosHost, nbatches: Int, nputs: Int) (
      implicit random: Random, scheduler: Scheduler): Async [Unit] =
    for {
      n <- (0 until nbatches) .async
      k <- random.nextKeys (nputs) .latch.unit
    } {
      propose (host, k, random.nextInt (1<<20))
    }

  def check (host: StubPaxosHost) (implicit scheduler: Scheduler): Async [Unit] = {
    for {
      _ <- for ((key, value) <- accepted.async)
            for (_found <- host.propose (key, 0, Bytes (-1)); found = _found.int)
              yield assert (
                  found == value,
                  "Expected $key to be $value, found $found.")
      _ <- for ((key, values) <- attempted.async; if !(accepted contains key))
            for (_found <- host.propose (key, 0, Bytes (-1)); found = _found.int)
              yield assert (
                  found == -1 || (values contains found),
                  "Expected $key to be one of $values, found $found")
    } yield ()
  }}
