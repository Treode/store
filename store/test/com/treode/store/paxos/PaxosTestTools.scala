package com.treode.store.paxos

import com.treode.async.{CallbackCaptor, StubScheduler}
import com.treode.cluster.StubHost
import com.treode.store.{Cohort, TimedTestTools}

private object PaxosTestTools extends TimedTestTools {

  implicit class TestableAcceptor (a: Acceptor) {

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

  def settled (num: Int, h1: StubHost, h2: StubHost, h3: StubHost): Cohort =
    Cohort.settled (num, h1.localId, h2.localId, h3.localId)

  def moving (
      num: Int,
      origin: (StubHost, StubHost, StubHost),
      target: (StubHost, StubHost, StubHost)
  ): Cohort = {
    val (o1, o2, o3) = origin
    val (t1, t2, t3) = target
    Cohort (
        num,
        Set (o1.localId, o2.localId, o3.localId),
        Set (t1.localId, t2.localId, t3.localId))
  }}
