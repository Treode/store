package com.treode.store.simple

import com.esotericsoftware.kryo.io.{Input, Output}
import com.treode.pickle.{Picklers, pickle, unpickle}
import com.treode.store.{Bytes, Fruits}
import org.scalatest.WordSpec

class SimpleCellBlockSpec extends WordSpec with TestTools {
  import Fruits.{Apple, Banana, Kiwi, Kumquat, Orange}

  private def newBlock (entries: Cell*): CellBlock =
    new CellBlock (Array (entries: _*))

  private def entriesEqual (expected: Cell, actual: Cell) {
    expectResult (expected.key) (actual.key)
    expectResult (expected.value) (actual.value)
  }

  private def blocksEqual (expected: CellBlock, actual: CellBlock) {
    expectResult (expected.entries.length) (actual.entries.length)
    for (i <- 0 until expected.entries.length)
      entriesEqual (expected.entries (i), actual.entries (i))
  }

  private def checkPickle (block: CellBlock) {
    val output = new Output (1024)
    pickle (CellBlock.pickle, block, output)
    val input = new Input (output.getBuffer)
    val result = unpickle (CellBlock.pickle, input)
    blocksEqual (block, result)
  }

  "A simple CellBlock" when {

    "empty" should {

      val block = newBlock ()

      "find nothing" in {
        expectResult (0) (block.find (Apple))
      }

      "pickle and unpickle to the same value" in {
        checkPickle (block)
      }}

    "holding one entry k=kiwi" should {

      val block = newBlock (Kiwi::None)

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

      val block = newBlock (Apple::None, Kiwi::None, Orange::None)

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
