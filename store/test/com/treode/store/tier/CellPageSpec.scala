package com.treode.store.tier

import com.treode.buffer.PagedBuffer
import com.treode.store.{Bytes, Fruits}
import org.scalatest.WordSpec

import Fruits.{AllFruits, Apple, Banana, Kiwi, Kumquat, Orange}
import TierTestTools._

class CellPageSpec extends WordSpec {

  private def newPage (entries: TierCell*): TierCellPage =
    new TierCellPage (Array (entries: _*))

  private def entriesEqual (expected: TierCell, actual: TierCell) {
    assertResult (expected.key) (actual.key)
    assertResult (expected.value) (actual.value)
  }

  private def pagesEqual (expected: TierCellPage, actual: TierCellPage) {
    assertResult (expected.entries.length) (actual.entries.length)
    for (i <- 0 until expected.entries.length)
      entriesEqual (expected.entries (i), actual.entries (i))
  }

  private def checkPickle (page: TierCellPage) {
    val buffer = PagedBuffer (12)
    TierCellPage.pickler.pickle (page, buffer)
    val result = TierCellPage.pickler.unpickle (buffer)
    pagesEqual (page, result)
  }

  "A simple CellPage" when {

    "empty" should {

      val page = newPage ()

      "find nothing" in {
        assertResult (0) (page.ceiling (Apple))
      }

      "pickle and unpickle to the same value" in {
        checkPickle (page)
      }}

    "holding a list of fruits" should {

      val page = newPage (AllFruits.map (_::None): _*)

      "pickle and unpickle to the same value" in {
        checkPickle (page)
      }}

    "holding a list of repeated keys" should {

      val page = newPage (Apple::None, Apple::None, Apple::None)

      "pickle and unpickle to the same value" in {
        checkPickle (page)
      }}

    "holding one entry k=kiwi" should {

      val page = newPage (Kiwi::None)

      "find apple before kiwi" in {
        assertResult (0) (page.ceiling (Apple))
      }

      "find kiwi using kiwi" in {
        assertResult (0) (page.ceiling (Kiwi))
      }

      "find orange after kiwi" in {
        assertResult (1) (page.ceiling (Orange))
      }

      "pickle and unpickle to the same value" in {
        checkPickle (page)
      }}

    "holding three entries" should {

      val page = newPage (Apple::None, Kiwi::None, Orange::None)

      "find apple using apple" in {
        assertResult (0) (page.ceiling (Apple))
      }

      "find kiwi using banana" in {
        assertResult (1) (page.ceiling (Banana))
      }

      "find kiwi using kiwi" in {
        assertResult (1) (page.ceiling (Kiwi))
      }

      "find orange using kumquat" in {
        assertResult (2) (page.ceiling (Kumquat))
      }

      "find orange using orange" in {
        assertResult (2) (page.ceiling (Orange))
      }

      "pickle and unpickle to the same value" in {
        checkPickle (page)
      }}}}
