package com.treode.pickle

import scala.util.Random

import org.scalatest.matchers.ShouldMatchers
import org.scalatest.prop.PropertyChecks
import org.scalatest.{FlatSpec, PropSpec, Specs}

class PicklersSpec extends Specs (PicklersBehaviors, PicklersProperties)

private trait PicklersSpecCommon extends ShouldMatchers {
  import Picklers._

  case class Url (host: String, port: Int, path: String)

  sealed abstract class Bookmark
  case class Link (name: String, link: Url) extends Bookmark
  case class Folder (name: String, bookmarks: List [Bookmark]) extends Bookmark

  val url = wrap3 (string, int, string) {
    Url.apply _
  } {
    x => (x.host, x.port, x.path)
  }

  val bookmark = laze (tagged [Bookmark] (
      0x1 -> link,
      0x2 -> folder))

  val link = wrap2 (string, url) {
    Link.apply _
  } {
    x => (x.name, x.link)
  }

  val folder: Pickler [Folder] = wrap2 (string, list (bookmark)) {
    Folder.apply _
  } {
    x => (x.name, x.bookmarks)
  }

  def check [A] (pa: Pickler [A], x: A) {
    expectResult (x) {
      val buffer = Buffer (10)
      pickle (pa, x, buffer)
      val y = unpickle (pa, buffer)
      y
    }}}

private object PicklersBehaviors extends FlatSpec with PicklersSpecCommon {
  import Picklers._

  "A Pickler" should "read and write tuples" in {
    check (tuple (string, int), ("So long, and thanks for all the fish", 42))
    check (tuple (double, double, double), (math.Pi, math.E, 0.207879576))
  }

  it should "read and write recursive structures" in {
    val expected = Folder (
      "bookmarks",
      List (
        Folder (
          "searches",
          List (Link ("google", Url ("www.google.com", 80, "/")),
               Link ("yahoo!", Url ("www.yahoo.com", 80, "/")))),
        Link ("mail", Url ("mail.google.com", 80, "/"))))

    check (bookmark, expected)
  }

  it should "read and write very long messages" in {
    check (seq (long), Seq.fill (2^20) (Random.nextLong))
  }}

private object PicklersProperties extends PropSpec with PropertyChecks with PicklersSpecCommon {
  import Picklers._

  property ("A Pickler reads and writes bytes") {
    forAll ("x") ((x: Byte) => check (byte, x))
  }

  property ("A Pickler reads and writes ints") {
    forAll ("x") ((x: Int) => check (int, x))
  }

  property ("A Pickler reads and writes longs") {
    forAll ("x") ((x: Long) => check (long, x))
  }

  property ("A Pickler reads and writes fixed ints") {
    forAll ("x") ((x: Int) => check (Picklers.fixedInt, x))
  }

  property ("A Pickler reads and writes fixed longs") {
    forAll ("x") ((x: Long) => check (Picklers.fixedLong, x))
  }

  property ("A Pickler reads and writes unsigned ints") {
    forAll ("x") ((x: Int) => check (Picklers.uint, x))
  }

  property ("A Pickler reads and writes unsigned longs") {
    forAll ("x") ((x: Long) => check (Picklers.ulong, x))
  }

  property ("A Pickler reads and writes floats") {
    forAll ("x") ((x: Float) => check (float, x))
  }

  property ("A Pickler reads and writes doubles") {
    forAll ("x") ((x: Double) => check (double, x))
  }

  property ("A Pickler reads and writes strings") {
    forAll ("x") ((x: String) => check (string, x))
  }

  property ("A Pickler reads and writes lists") {
    forAll ("x", maxSize (3)) ((x: List [Int]) => check (list (int), x))
  }}
