package com.treode.store.paxos

import com.treode.store.{Atlas, Cohort, StoreTestTools}

private object PaxosTestTools extends StoreTestTools {

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

  def expectAtlas (version: Int, cohorts: Cohort*) (hosts: Seq [StubPaxosHost]) {
    val atlas = Atlas (cohorts.toArray, version)
    for (host <- hosts)
      host.expectAtlas (atlas)
  }}
