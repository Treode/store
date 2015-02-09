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

class FiltersSpec extends FreeSpec {

  def concat [A, B] (x: (Seq [A], Seq [B]), y: (Seq [A], Seq [B])): (Seq [A], Seq [B]) =
    (x._1 ++ y._1, x._2 ++ y._2)

  "The dedupe filter should" - {

    def test (items: (Seq [Cell], Seq [Cell])*) {
      val in = items .map (_._1) .flatten
      val out = items .map (_._2) .flatten
      s"handle ${testStringOf (in)}" in {
        implicit val scheduler = StubScheduler.random()
        assertCells (out: _*) (in.batch.dedupe)
      }}

    val apple1 = (
        Seq (Apple##0::1),
        Seq (Apple##0::1))

    val apple2 = (
        Seq (Apple##0::2, Apple##0::1),
        Seq (Apple##0::2))

    val apple3 = (
        Seq (Apple##0::3, Apple##0::2, Apple##0::1),
        Seq (Apple##0::3))

    val banana1 = (
        Seq (Banana##0::1),
        Seq (Banana##0::1))

    val banana2 = (
        Seq (Banana##0::2, Banana##0::1),
        Seq (Banana##0::2))

    val banana3 = (
        Seq (Banana##0::3, Banana##0::2, Banana##0::1),
        Seq (Banana##0::3))

    test ()
    test (apple1)
    test (apple2)
    test (apple3)
    test (apple1, banana1)
    test (apple2, banana1)
    test (apple3, banana1)
    test (apple1, banana2)
    test (apple2, banana2)
    test (apple3, banana2)
    test (apple1, banana3)
    test (apple2, banana3)
    test (apple3, banana3)
  }

  "The retire filter should" - {

    def test (item: (Seq [Cell], Seq [Cell])) {
      val in = item._1
      val out = item._2
      s"handle ${testStringOf (in)}" in {
        implicit val scheduler = StubScheduler.random()
        assertCells (out: _*) (in.batch.retire (7))
      }}

    val apple1 = (
        Seq (Apple##1::1),
        Seq (Apple##1::1))

    val apple2 = (
        Seq (Apple##2::2, Apple##1::1),
        Seq (Apple##2::2))

    val apple3 = (
        Seq (Apple##3::3, Apple##2::2, Apple##1::1),
        Seq (Apple##3::3))

    val apple4 = (
        Seq (Apple##7::7),
        Seq (Apple##7::7))

    val apple5 = (
        Seq (Apple##8::8, Apple##7::7),
        Seq (Apple##8::8, Apple##7::7))

    val apple6 = (
        Seq (Apple##9::9, Apple##8::8, Apple##7::7),
        Seq (Apple##9::9, Apple##8::8, Apple##7::7))

    val apple7 = concat (apple4, apple1)
    val apple8 = concat (apple5, apple1)
    val apple9 = concat (apple6, apple1)
    val apple10 = concat (apple4, apple2)
    val apple11 = concat (apple5, apple2)
    val apple12 = concat (apple6, apple2)
    val apple13 = concat (apple4, apple3)
    val apple14 = concat (apple5, apple3)
    val apple15 = concat (apple6, apple3)

    val apples = Seq (
        apple1, apple2, apple3, apple4, apple5, apple6, apple7, apple8, apple9, apple10, apple11,
        apple12, apple13, apple14, apple15)

    val banana1 = (
        Seq (Banana##1::1),
        Seq (Banana##1::1))

    val banana2 = (
        Seq (Banana##2::2, Banana##1::1),
        Seq (Banana##2::2))

    val banana3 = (
        Seq (Banana##3::3, Banana##2::2, Banana##1::1),
        Seq (Banana##3::3))

    val banana4 = (
        Seq (Banana##7::7),
        Seq (Banana##7::7))

    val banana5 = (
        Seq (Banana##8::8, Banana##7::7),
        Seq (Banana##8::8, Banana##7::7))

    val banana6 = (
        Seq (Banana##9::9, Banana##8::8, Banana##7::7),
        Seq (Banana##9::9, Banana##8::8, Banana##7::7))

    val banana7 = concat (banana4, banana1)
    val banana8 = concat (banana5, banana1)
    val banana9 = concat (banana6, banana1)
    val banana10 = concat (banana4, banana2)
    val banana11 = concat (banana5, banana2)
    val banana12 = concat (banana6, banana2)
    val banana13 = concat (banana4, banana3)
    val banana14 = concat (banana5, banana3)
    val banana15 = concat (banana6, banana3)

    val bananas = Seq (
        banana1, banana2, banana3, banana4, banana5, banana6, banana7, banana8, banana9, banana10,
        banana11, banana12, banana13, banana14, banana15)

    test ((Seq.empty, Seq.empty))
    for (a <- apples)
      test (a)
    for (a <- apples; b <- bananas)
      test (concat (a, b))
  }}
