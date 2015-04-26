/*
 * Copyright 2014 Treode, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.treode.store

import com.treode.async.implicits._
import com.treode.async.stubs.StubScheduler
import org.scalatest.FreeSpec

import Fruits.{Apple, Banana}
import StoreTestTools._
import Window.{Between, Latest, Through}

class WindowSpec extends FreeSpec {

  def concat [A, B] (x: (Seq [A], Seq [B]), y: (Seq [A], Seq [B])): (Seq [A], Seq [B]) =
    (x._1 ++ y._1, x._2 ++ y._2)

  def testOverlap (window: Window, latest: Long, earliest: Long, expected: Boolean) {
    s"$window should ${if (expected) "overlap" else "not overlap"} [$earliest, $latest]" in {
      assertResult (expected) (window.overlaps (latest, earliest))
    }}

  "Window.Latest should" - {

    val filter = Latest (3, true, 2, true)

    def testFilter (items: (Seq [Cell], Seq [Cell])*) {
      val in = items .map (_._1) .flatten
      val out = items .map (_._2) .flatten
      s"filter ${testStringOf (in)}" in {
        implicit val scheduler = StubScheduler.random()
        assertCells (out: _*) (in.batch.window (filter))
      }}

    val apples = Seq (
        ( Seq (Apple##4::4, Apple##3::3, Apple##2::2, Apple##1::1),
          Seq (Apple##3::3)),
        ( Seq (Apple##4::4, Apple##3::3, Apple##2::2),
          Seq (Apple##3::3)),
        ( Seq (Apple##4::4, Apple##3::3, Apple##1::1),
          Seq (Apple##3::3)),
        ( Seq (Apple##4::4, Apple##3::3),
          Seq (Apple##3::3)),
        ( Seq (Apple##4::4, Apple##2::2, Apple##1::1),
          Seq (Apple##2::2)),
        ( Seq (Apple##4::4, Apple##2::2),
          Seq (Apple##2::2)),
        ( Seq (Apple##4::4, Apple##1::1),
          Seq ()),
        ( Seq (Apple##4::4),
          Seq ()),
        ( Seq (Apple##3::3, Apple##2::2, Apple##1::1),
          Seq (Apple##3::3)),
        ( Seq (Apple##3::3, Apple##2::2),
          Seq (Apple##3::3)),
        ( Seq (Apple##3::3, Apple##1::1),
          Seq (Apple##3::3)),
        ( Seq (Apple##3::3),
          Seq (Apple##3::3)),
        ( Seq (Apple##2::2, Apple##1::1),
          Seq (Apple##2::2)),
        ( Seq (Apple##2::2),
          Seq (Apple##2::2)),
        ( Seq (Apple##1::1),
          Seq ()))

    val bananas = Seq (
        ( Seq (Banana##4::4, Banana##3::3, Banana##2::2, Banana##1::1),
          Seq (Banana##3::3)),
        ( Seq (Banana##4::4, Banana##3::3, Banana##2::2),
          Seq (Banana##3::3)),
        ( Seq (Banana##4::4, Banana##3::3, Banana##1::1),
          Seq (Banana##3::3)),
        ( Seq (Banana##4::4, Banana##3::3),
          Seq (Banana##3::3)),
        ( Seq (Banana##4::4, Banana##2::2, Banana##1::1),
          Seq (Banana##2::2)),
        ( Seq (Banana##4::4, Banana##2::2),
          Seq (Banana##2::2)),
        ( Seq (Banana##4::4, Banana##1::1),
          Seq ()),
        ( Seq (Banana##4::4),
          Seq ()),
        ( Seq (Banana##3::3, Banana##2::2, Banana##1::1),
          Seq (Banana##3::3)),
        ( Seq (Banana##3::3, Banana##2::2),
          Seq (Banana##3::3)),
        ( Seq (Banana##3::3, Banana##1::1),
          Seq (Banana##3::3)),
        ( Seq (Banana##3::3),
          Seq (Banana##3::3)),
        ( Seq (Banana##2::2, Banana##1::1),
          Seq (Banana##2::2)),
        ( Seq (Banana##2::2),
          Seq (Banana##2::2)),
        ( Seq (Banana##1::1),
          Seq ()))

    testFilter ((Seq.empty, Seq.empty))
    for (a <- apples)
      testFilter (a)
    for (a <- apples; b <- bananas)
      testFilter (concat (a, b))

    testOverlap (Latest (3, true,  2, true ), 2, 1, true )
    testOverlap (Latest (3, true,  2, false), 2, 1, false)
    testOverlap (Latest (3, true,  2, true ), 3, 2, true )
    testOverlap (Latest (3, true,  2, false), 3, 2, true )
    testOverlap (Latest (3, false, 2, true ), 3, 2, true )
    testOverlap (Latest (3, false, 2, false), 3, 2, true )
    testOverlap (Latest (3, true,  2, true ), 4, 3, true )
    testOverlap (Latest (3, false, 2, true ), 4, 3, false)
  }

  "Window.Between should" - {

    val filter = Between (3, true, 2, true)

    def test (items: (Seq [Cell], Seq [Cell])*) {
      val in = items .map (_._1) .flatten
      val out = items .map (_._2) .flatten
      s"handle ${testStringOf (in)}" in {
        implicit val scheduler = StubScheduler.random()
        assertCells (out: _*) (in.batch.window (filter))
      }}

    val apple1 = (
        Seq (Apple##1::1),
        Seq ())

    val apple2 = (
        Seq (Apple##2::2),
        Seq (Apple##2::2))

    val apple3 = (
        Seq (Apple##2::2, Apple##1::1),
        Seq (Apple##2::2))

    val apple4 = (
        Seq (Apple##3::3, Apple##2::2, Apple##1::1),
        Seq (Apple##3::3, Apple##2::2))

    val apple5 = (
        Seq (Apple##4::4, Apple##3::3, Apple##2::2, Apple##1::1),
        Seq (Apple##3::3, Apple##2::2))

    val apple6 = (
        Seq (Apple##4::4, Apple##3::3, Apple##2::2),
        Seq (Apple##3::3, Apple##2::2))

    val apple7 = (
        Seq (Apple##4::4, Apple##3::3),
        Seq (Apple##3::3))

    val apples = Seq (apple1, apple2, apple3, apple4, apple5, apple6, apple7)

    test ((Seq.empty, Seq.empty))
    for (a <- apples)
      test (a)

    testOverlap (Between (3, true,  2, true ), 2, 1, true )
    testOverlap (Between (3, true,  2, false), 2, 1, false)
    testOverlap (Between (3, true,  2, true ), 3, 2, true )
    testOverlap (Between (3, true,  2, false), 3, 2, true )
    testOverlap (Between (3, false, 2, true ), 3, 2, true )
    testOverlap (Between (3, false, 2, false), 3, 2, true )
    testOverlap (Between (3, true,  2, true ), 4, 3, true )
    testOverlap (Between (3, false, 2, true ), 4, 3, false)
  }

  "Window.Through should" - {

    val filter = Through (3, true, 2)

    def test (items: (Seq [Cell], Seq [Cell])*) {
      val in = items .map (_._1) .flatten
      val out = items .map (_._2) .flatten
      s"handle ${testStringOf (in)}" in {
        implicit val scheduler = StubScheduler.random()
        assertCells (out: _*) (in.batch.window (filter))
      }}

    val apple1 = (
        Seq (Apple##1::1),
        Seq (Apple##1::1))

    val apple2 = (
        Seq (Apple##2::2),
        Seq (Apple##2::2))

    val apple3 = (
        Seq (Apple##2::2, Apple##1::1),
        Seq (Apple##2::2))

    val apple4 = (
        Seq (Apple##3::3, Apple##2::2, Apple##1::1),
        Seq (Apple##3::3, Apple##2::2))

    val apple5 = (
        Seq (Apple##4::4, Apple##3::3, Apple##2::2, Apple##1::1),
        Seq (Apple##3::3, Apple##2::2))

    val apple6 = (
        Seq (Apple##4::4, Apple##3::3, Apple##2::2),
        Seq (Apple##3::3, Apple##2::2))

    val apple7 = (
        Seq (Apple##4::4, Apple##3::3),
        Seq (Apple##3::3))

    val apples = Seq (apple1, apple2, apple3, apple4, apple5, apple6, apple7)

    val banana1 = (
        Seq (Banana##1::1),
        Seq (Banana##1::1))

    val banana2 = (
        Seq (Banana##2::2),
        Seq (Banana##2::2))

    val banana3 = (
        Seq (Banana##2::2, Banana##1::1),
        Seq (Banana##2::2))

    val banana4 = (
        Seq (Banana##3::3, Banana##2::2, Banana##1::1),
        Seq (Banana##3::3, Banana##2::2))

    val banana5 = (
        Seq (Banana##4::4, Banana##3::3, Banana##2::2, Banana##1::1),
        Seq (Banana##3::3, Banana##2::2))

    val banana6 = (
        Seq (Banana##4::4, Banana##3::3, Banana##2::2),
        Seq (Banana##3::3, Banana##2::2))

    val banana7 = (
        Seq (Banana##4::4, Banana##3::3),
        Seq (Banana##3::3))

    val bananas = Seq (banana1, banana2, banana3, banana4, banana5, banana6, banana7)

    test ((Seq.empty, Seq.empty))
    for (a <- apples)
      test (a)
    for (a <- apples; b <- bananas)
      test (concat (a, b))

    testOverlap (Through (3, true,  2), 2, 1, true )
    testOverlap (Through (3, true,  2), 3, 2, true )
    testOverlap (Through (3, false, 2), 3, 2, true )
    testOverlap (Through (3, true,  2), 4, 3, true )
    testOverlap (Through (3, false, 2), 4, 3, false)
  }}
