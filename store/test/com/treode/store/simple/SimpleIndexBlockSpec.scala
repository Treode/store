package com.treode.store.simple

import com.esotericsoftware.kryo.io.{Input, Output}
import com.treode.pickle.{Picklers, pickle, unpickle}
import com.treode.store.{Bytes, Fruits, TxClock}
import org.scalatest.WordSpec

class SimpleIndexBlockSpec extends WordSpec {
  import Fruits.{Apple, Banana, Kiwi, Kumquat, Orange}

  private def entry (key: Bytes): IndexEntry =
    new IndexEntry (key, 0)

  private def newBlock (entries: IndexEntry*): IndexBlock =
    new IndexBlock (Array (entries: _*))

  private def entriesEqual (expected: IndexEntry, actual: IndexEntry) {
    expectResult (expected.key) (actual.key)
    expectResult (expected.pos) (actual.pos)
  }

  private def blocksEqual (expected: IndexBlock, actual: IndexBlock) {
    expectResult (expected.entries.length) (actual.entries.length)
    for (i <- 0 until expected.entries.length)
      entriesEqual (expected.entries (i), actual.entries (i))
  }

  private def checkPickle (block: IndexBlock) {
    val output = new Output (1024)
    pickle (IndexBlock.pickle, block, output)
    val input = new Input (output.getBuffer)
    val result = unpickle (IndexBlock.pickle, input)
    blocksEqual (block, result)
  }

  "A simple IndexBlock" when {

    "empty" should {

      val block = newBlock ()

      "find nothing" in {
        expectResult (0) (block.find (Apple))
      }

      "pickle and unpickle to the same value" in {
        checkPickle (block)
      }}

    "holding one entry k=kiwi" should {

      val block = newBlock (entry (Kiwi))

      "find apple before kiwi" in {
        expectResult (0) (block.find (Apple))
      }

      "find kiwi using kiwi" in {
        expectResult (0) (block.find (Kiwi))
      }

      "find orange after kiwi" in {
        expectResult (1) (block.find (Orange))
      }

      "pickle and unpickle to the same value" in {
        checkPickle (block)
      }}

    "holding three entries" should {

      val block = newBlock (
          entry (Apple),
          entry (Kiwi),
          entry (Orange))

      "find apple using apple" in {
        expectResult (0) (block.find (Apple))
      }

      "find kiwi using banana" in {
        expectResult (1) (block.find (Banana))
      }

      "find kiwi using kiwi" in {
        expectResult (1) (block.find (Kiwi))
      }

      "find orange using kumquat" in {
        expectResult (2) (block.find (Kumquat))
      }

      "find orange using orange" in {
        expectResult (2) (block.find (Orange))
      }

      "pickle and unpickle to the same value" in {
        checkPickle (block)
      }}}}
