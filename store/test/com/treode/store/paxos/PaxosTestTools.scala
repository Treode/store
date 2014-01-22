package com.treode.store.paxos

import com.treode.async.{CallbackCaptor, StubScheduler}
import com.treode.store.{Bytes, SimpleAccessor}

trait PaxosTestTools {

  implicit class TestableAcceptor (a: Acceptor) {

    def isRestoring = a.state.isInstanceOf [Acceptor#Restoring]
    def isDeliberating = a.state.isInstanceOf  [Acceptor#Deliberating]
    def isClosed = a.state.isInstanceOf [Acceptor#Closed]

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
