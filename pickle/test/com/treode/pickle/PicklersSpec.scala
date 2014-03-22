package com.treode.pickle

import scala.util.Random

import com.treode.buffer.PagedBuffer
import org.scalatest.FlatSpec
import org.scalatest.prop.PropertyChecks

import Picklers._
import PropertyChecks._

class PicklersSpec extends FlatSpec {


  case class Url (host: String, port: Int, path: String)

  sealed abstract class Bookmark
  case class Link (name: String, link: Url) extends Bookmark
  case class Folder (name: String, bookmarks: List [Bookmark]) extends Bookmark

  val url =
    wrap (string, int, string)
    .build (x => Url.apply (x._1, x._2, x._3))
    .inspect (x => (x.host, x.port, x.path))

  val bookmark = laze (tagged [Bookmark] (
      0x1 -> link,
      0x2 -> folder))

  val link =
    wrap (string, url)
    .build (x => Link (x._1, x._2))
    .inspect (x => (x.name, x.link))

  val folder: Pickler [Folder] =
    wrap (string, list (bookmark))
    .build (x => Folder (x._1, x._2))
    .inspect (x => (x.name, x.bookmarks))

  def check [A] (pa: Pickler [A], x: A) {
    assertResult (x) {
      val buffer = PagedBuffer (10)
      pa.pickle (x, buffer)
      val y = pa.unpickle (buffer)
      y
    }}

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
  }

  it should "read and write bytes" in {
    forAll ("x") ((x: Byte) => check (byte, x))
  }

  it should "read and write ints" in {
    forAll ("x") ((x: Int) => check (int, x))
  }

  it should "read and write longs" in {
    forAll ("x") ((x: Long) => check (long, x))
  }

  it should "read and write fixed ints" in {
    forAll ("x") ((x: Int) => check (Picklers.fixedInt, x))
  }

  it should "read and write fixed longs" in {
    forAll ("x") ((x: Long) => check (Picklers.fixedLong, x))
  }

  it should "read and write unsigned ints" in {
    forAll ("x") ((x: Int) => check (Picklers.uint, x))
  }

  it should "read and write unsigned longs" in {
    forAll ("x") ((x: Long) => check (Picklers.ulong, x))
  }

  it should "read and write floats" in {
    forAll ("x") ((x: Float) => check (float, x))
  }

  it should "read and write doubles" in {
    forAll ("x") ((x: Double) => check (double, x))
  }

  it should "read and write strings" in {
    forAll ("x") ((x: String) => check (string, x))
  }

  it should "read and write lists" in {
    forAll ("x", maxSize (3)) ((x: List [Int]) => check (list (int), x))
  }}
