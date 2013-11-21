package com.treode.store.cluster.paxos

import com.treode.async.{CallbackCaptor, StubScheduler}
import com.treode.store.{Bytes, SimpleAccessor}

trait PaxosTestTools {

  implicit class TestableAcceptor (a: Acceptor) {

    def isRestoring = a.state == a.Restoring
    def isDeliberating = classOf [Acceptor#Deliberating] .isInstance (a.state)
    def isClosed = classOf [Acceptor#Closed] .isInstance (a.state)

    def getChosen: Option [Int] = {
      if (isClosed)
        Some (a.state.asInstanceOf [Acceptor#Closed] .chosen.int)
      else if (isDeliberating)
        a.state.asInstanceOf [Acceptor#Deliberating] .proposal.map (_._2.int)
      else
        None
    }}

  implicit class TestableSimpleAccessor [K, V] (db: SimpleAccessor [K, V]) {

    def get (k: K) (implicit scheduler: StubScheduler): Option [V] = {
      val cb = new CallbackCaptor [Option [V]]
      db.get (k, cb)
      scheduler.runTasks()
      cb.passed
    }}}
