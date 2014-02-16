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
    expectResult (expected.key) (actual.key)
    expectResult (expected.pos) (actual.pos)
  }

  private def pagesEqual (expected: IndexPage, actual: IndexPage) {
    expectResult (expected.entries.length) (actual.entries.length)
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
        expectResult (0) (page.find (Apple))
      }

      "pickle and unpickle to the same value" in {
        checkPickle (page)
      }}

    "holding one entry k=kiwi" should {

      val page = newPage (entry (Kiwi))

      "find apple before kiwi" in {
        expectResult (0) (page.find (Apple))
      }

      "find kiwi using kiwi" in {
        expectResult (0) (page.find (Kiwi))
      }

      "find orange after kiwi" in {
        expectResult (1) (page.find (Orange))
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
        expectResult (0) (page.find (Apple))
      }

      "find kiwi using banana" in {
        expectResult (1) (page.find (Banana))
      }

      "find kiwi using kiwi" in {
        expectResult (1) (page.find (Kiwi))
      }

      "find orange using kumquat" in {
        expectResult (2) (page.find (Kumquat))
      }

      "find orange using orange" in {
        expectResult (2) (page.find (Orange))
      }

      "pickle and unpickle to the same value" in {
        checkPickle (page)
      }}}}
