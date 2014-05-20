package com.treode.store.paxos

import scala.util.Random

import com.treode.store.{Atlas, Bytes, Cohort, StoreTestTools}

private object PaxosTestTools extends StoreTestTools {

  implicit class RichRandom (random: Random) {

    def nextKey(): Bytes =
      Bytes (PaxosPicklers.fixedLong, random.nextLong & 0x7FFFFFFFFFFFFFFFL)

    def nextKeys (count: Int): Seq [Bytes] =
      Seq.fill (count) (nextKey())
  }

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
