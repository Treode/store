package com.treode.store.tier

import com.treode.buffer.PagedBuffer
import com.treode.store.{Bytes, Fruits, TxClock}
import org.scalatest.WordSpec

class IndexPageSpec extends WordSpec {
  import Fruits.{Apple, Banana, Kiwi, Kumquat, Orange}

  private def entry (key: Bytes): IndexEntry =
    new IndexEntry (key, 0, 0, 0)

  private def newPage (entries: IndexEntry*): IndexPage =
    new IndexPage (Array (entries: _*))

  private def entriesEqual (expected: IndexEntry, actual: IndexEntry) {
    assertResult (expected.key) (actual.key)
    assertResult (expected.pos) (actual.pos)
  }

  private def pagesEqual (expected: IndexPage, actual: IndexPage) {
    assertResult (expected.entries.length) (actual.entries.length)
    for (i <- 0 until expected.entries.length)
      entriesEqual (expected.entries (i), actual.entries (i))
  }

  private def checkPickle (page: IndexPage) {
    val buffer = PagedBuffer (12)
    IndexPage.pickler.pickle (page, buffer)
    val result = IndexPage.pickler.unpickle (buffer)
    pagesEqual (page, result)
  }

  "A simple IndexPage" when {

    "empty" should {

      val page = newPage ()

      "find nothing" in {
        assertResult (0) (page.ceiling (Apple))
      }

      "pickle and unpickle to the same value" in {
        checkPickle (page)
      }}

    "holding one entry k=kiwi" should {

      val page = newPage (entry (Kiwi))

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

      val page = newPage (
          entry (Apple),
          entry (Kiwi),
          entry (Orange))

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
