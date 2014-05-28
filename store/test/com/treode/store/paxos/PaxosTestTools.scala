package com.treode.store.paxos

import scala.util.Random

import com.treode.store.{Atlas, Bytes, Cohort, StoreTestTools}

private object PaxosTestTools extends StoreTestTools {

  implicit class PaxosRichRandom (random: Random) {

    def nextKey(): Bytes =
      Bytes (PaxosPicklers.fixedLong, random.nextLong & 0x7FFFFFFFFFFFFFFFL)

    def nextKeys (count: Int): Seq [Bytes] =
      Seq.fill (count) (nextKey())
  }

  def expectAtlas (version: Int, cohorts: Cohort*) (hosts: Seq [StubPaxosHost]) {
    val atlas = Atlas (cohorts.toArray, version)
    for (host <- hosts)
      host.expectAtlas (atlas)
  }}
