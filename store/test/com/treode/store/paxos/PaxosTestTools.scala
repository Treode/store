package com.treode.store.paxos

import com.treode.async.{CallbackCaptor, StubScheduler}

trait PaxosTestTools {

  implicit class TestableAcceptor (a: Acceptor) {

    def isOpening = a.state.isInstanceOf [Acceptor#Opening]
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
    }}}
