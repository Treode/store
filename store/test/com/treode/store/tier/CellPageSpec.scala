package com.treode.store.tier

import com.treode.buffer.PagedBuffer
import com.treode.store.{Bytes, Cell, Fruits}
import org.scalatest.WordSpec

import Fruits.{AllFruits, Apple, Banana, Kiwi, Kumquat, Orange}
import TierTestTools._

class CellPageSpec extends WordSpec {

  private def newPage (entries: Cell*): CellPage =
    new CellPage (Array (entries: _*))

  private def entriesEqual (expected: Cell, actual: Cell) {
    assertResult (expected.key) (actual.key)
    assertResult (expected.value) (actual.value)
  }

  private def pagesEqual (expected: CellPage, actual: CellPage) {
    assertResult (expected.entries.length) (actual.entries.length)
    for (i <- 0 until expected.entries.length)
      entriesEqual (expected.entries (i), actual.entries (i))
  }

  private def checkPickle (page: CellPage) {
    val buffer = PagedBuffer (12)
    TierCellPage.pickler.pickle (page, buffer)
    val result = TierCellPage.pickler.unpickle (buffer)
    pagesEqual (page, result)
  }

  "A simple CellPage" when {

    "empty" should {

      val page = newPage ()

      "find nothing" in {
        assertResult (0) (page.ceiling (Apple, 0))
      }

      "pickle and unpickle to the same value" in {
        checkPickle (page)
      }}

    "holding a list of fruits" should {

      val page = newPage (AllFruits.map (_##0): _*)

      "pickle and unpickle to the same value" in {
        checkPickle (page)
      }}

    "holding a list of repeated keys" should {

      val page = newPage (Apple##0, Apple##0, Apple##0)

      "pickle and unpickle to the same value" in {
        checkPickle (page)
      }}

    "holding one entry k=kiwi" should {

      val page = newPage (Kiwi##0)

      "find apple before kiwi" in {
        assertResult (0) (page.ceiling (Apple, 0))
      }

      "find kiwi using kiwi" in {
        assertResult (0) (page.ceiling (Kiwi, 0))
      }

      "find orange after kiwi" in {
        assertResult (1) (page.ceiling (Orange, 0))
      }

      "pickle and unpickle to the same value" in {
        checkPickle (page)
      }}

    "holding three entries" should {

      val page = newPage (Apple##0, Kiwi##0, Orange##0)

      "find apple using apple" in {
        assertResult (0) (page.ceiling (Apple, 0))
      }

      "find kiwi using banana" in {
        assertResult (1) (page.ceiling (Banana, 0))
      }

      "find kiwi using kiwi" in {
        assertResult (1) (page.ceiling (Kiwi, 0))
      }

      "find orange using kumquat" in {
        assertResult (2) (page.ceiling (Kumquat, 0))
      }

      "find orange using orange" in {
        assertResult (2) (page.ceiling (Orange, 0))
      }

      "pickle and unpickle to the same value" in {
        checkPickle (page)
      }}}}
