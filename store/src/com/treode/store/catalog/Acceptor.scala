/*
 * Copyright 2014 Treode, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.treode.store.catalog

import scala.util.{Failure, Success}

import com.treode.async.{Callback, Fiber}
import com.treode.cluster.{MessageDescriptor, Peer}
import com.treode.store.{BallotNumber, CatalogId, TimeoutException}

import Callback.ignore

private class Acceptor (val key: CatalogId, val version: Int, kit: CatalogKit) {
  import kit.{acceptors, broker, cluster, scheduler}
  import kit.config.{closedLifetime, deliberatingTimeout}
  import kit.library.releaser

  private val fiber = new Fiber
  private val epoch = releaser.join()
  var state: State = null

  trait State {
    def ask (proposer: Peer, ballot: Long, default: Patch)
    def propose (proposer: Peer, ballot: Long, patch: Patch)
    def choose (chosen: Patch)
  }

  private def cleanup() {
    releaser.leave (epoch)
    scheduler.delay (closedLifetime) (acceptors.remove (key, version, Acceptor.this))
  }

  private def cleanup (s: State): Unit =
    fiber.execute {
      if (state == s)
        cleanup()
    }

  private def panic (s: State, t: Throwable): Unit =
    fiber.execute {
      if (state == s) {
        state = new Panicked (t)
        throw t
      }}

  class Opening extends State {

    def ask (proposer: Peer, ballot: Long, default: Patch): Unit =
      state = new Getting (_.ask (proposer, ballot, default), default)

    def propose (proposer: Peer, ballot: Long, patch: Patch): Unit =
      state = new Getting (_.propose (proposer, ballot, patch), patch)

    def choose (chosen: Patch): Unit =
      state = new Getting (_.choose (chosen), chosen)

    override def toString = "Acceptor.Opening"
  }

  class Getting (var op: State => Unit, default: Patch) extends State {

    broker.get (key) run {
      case Success (cat) =>
        fiber.execute {
          if (state == Getting.this) {
            state =
              if (version == cat.version + 1)
                new Deliberating (default)
              else
                new Stale
            op (state)
          }}
      case Failure (t) =>
        panic (Getting.this, t)
    }

    def ask (proposer: Peer, ballot: Long, default: Patch): Unit =
      op = (_.ask (proposer, ballot, default))

    def propose (proposer: Peer, ballot: Long, patch: Patch): Unit =
      op = (_.propose (proposer, ballot, patch))

    def choose (chosen: Patch): Unit =
      op = (_.choose (chosen))

    override def toString = "Acceptor.Getting"
  }

  class Deliberating (default: Patch) extends State {

    var ballot: BallotNumber = BallotNumber.zero
    var proposal: Proposal = Option.empty
    var proposers = Set.empty [Peer]

    fiber.delay (deliberatingTimeout) (timeout())

    def timeout() {
      if (state == Deliberating.this) {
        kit.propose (key, default) .run { result =>
          fiber.execute {
            if (state == Deliberating.this) {
              result match {
                case Success (v) =>
                  state = new Closed (v)
                case Failure (_: TimeoutException) =>
                  state = new Stale
                case Failure (t) =>
                  state = new Panicked (t)
                  throw t
              }}}}}}

    def ask (proposer: Peer, _ballot: Long, default: Patch) {
      proposers += proposer
      val ballot = BallotNumber (_ballot, proposer.id)
      if (ballot < this.ballot) {
        Proposer.refuse (key, version, this.ballot.number) (proposer)
      } else {
        this.ballot = ballot
        Proposer.grant (key, version, ballot.number, proposal) (proposer)
      }}

    def propose (proposer: Peer, _ballot: Long, patch: Patch) {
      proposers += proposer
      val ballot = BallotNumber (_ballot, proposer.id)
      if (ballot < this.ballot) {
        Proposer.refuse (key, version, this.ballot.number) (proposer)
      } else {
        this.ballot = ballot
        this.proposal = Some ((ballot, patch))
        Proposer.accept (key, version, ballot.number) (proposer)
      }}

    def choose (chosen: Patch) {
      state = new Closed (chosen)
    }

    override def toString = s"Acceptor.Deliberating($key, $proposal)"
  }

  class Closed (val chosen: Patch) extends State {

    broker.patch (key, chosen) run {
      case Success (v) =>
        cleanup (Closed.this)
      case Failure (t) =>
        panic (Closed.this, t)
    }

    def ask (proposer: Peer, ballot: Long, default: Patch): Unit =
      Proposer.chosen (key, version, chosen) (proposer)

    def propose (proposer: Peer, ballot: Long, patch: Patch): Unit =
      Proposer.chosen (key, version, chosen) (proposer)

    def choose (chosen: Patch): Unit =
      require (chosen.checksum == this.chosen.checksum, "Paxos disagreement")

    override def toString = s"Acceptor.Closed($key, $chosen)"
  }

  class Stale extends State {

    cleanup()

    def ask (proposer: Peer, ballot: Long, default: Patch): Unit = ()

    def propose (proposer: Peer, ballot: Long, patch: Patch): Unit = ()

    def choose (chosen: Patch): Unit = ()

    override def toString = s"Acceptor.Stale($key)"
  }

  class Panicked (thrown: Throwable) extends State {

    cleanup()

    def ask (proposer: Peer, ballot: Long, default: Patch): Unit = ()
    def propose (proposer: Peer, ballot: Long, patch: Patch): Unit = ()
    def choose (chosen: Patch): Unit = ()

    override def toString = s"Acceptor.Panicked($key, $thrown)"
  }

  def ask (proposer: Peer, ballot: Long, default: Patch): Unit =
    fiber.execute (state.ask (proposer, ballot, default))

  def propose (proposer: Peer, ballot: Long, patch: Patch): Unit =
    fiber.execute (state.propose (proposer, ballot, patch))

  def choose (chosen: Patch): Unit =
    fiber.execute (state.choose (chosen))

  def dispose(): Unit =
    releaser.leave (epoch)

  override def toString = state.toString
}

private object Acceptor {

  val ask = {
    import CatalogPicklers._
    MessageDescriptor (0xFF9BFCEDF7D2E129L, tuple (uint, catId, uint, ulong, patch))
  }

  val propose = {
    import CatalogPicklers._
    MessageDescriptor (0xFF3E59E358D49679L, tuple (uint, catId, uint, ulong, patch))
  }

  val choose = {
    import CatalogPicklers._
    MessageDescriptor (0xFF3CF1687A498C79L, tuple (catId, uint, patch))
  }}
