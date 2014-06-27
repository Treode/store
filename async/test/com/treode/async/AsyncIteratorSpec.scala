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

package com.treode.async

import com.treode.async.implicits._
import com.treode.async.stubs.StubScheduler
import com.treode.async.stubs.implicits._
import org.scalatest.FlatSpec

import Async.supply
import AsyncIteratorTestTools._

class AsyncIteratorSpec extends FlatSpec {

  private def map [A, B] (xs: A*) (f: A => B) (implicit s: StubScheduler): AsyncIterator [B] =
    xs.async.map (f)

  private def flatMap [A, B] (xs: A*) (ys: B*) (implicit s: StubScheduler): AsyncIterator [(A, B)] =
    xs.async.flatMap (x => ys.async.map (y => (x, y)))

  private def filter [A] (xs: A*) (p: A => Boolean) (implicit s: StubScheduler): AsyncIterator [A] =
    xs.async.filter (p)

  private def whilst [A] (xs: A*) (p: A => Boolean) (f: A => Any) (implicit s: StubScheduler): Option [A] =
    xs.async.whilst (p) (x => supply (f (x))) .pass

  "AsyncIterator.foreach" should "handle an empty sequence" in {
    implicit val scheduler = StubScheduler.random()
    assertSeq () (Seq.empty.async)
  }

  it should "handle a sequence of one element" in {
    implicit val scheduler = StubScheduler.random()
    assertSeq (1) (adapt (1))
  }

  it should "handle a sequence of three elements" in {
    implicit val scheduler = StubScheduler.random()
    assertSeq (1, 2, 3) (adapt (1, 2, 3))
  }

  it should "stop at the first exception" in {
    implicit val scheduler = StubScheduler.random()
    var consumed = Set.empty [Int]
    var provided = Set.empty [Int]
    assertFail [DistinguishedException] {
      val i1 = track (adapt (1, 2, 3, 4)) (consumed += _)
      val i2 = failWhen (i1) (_ == 3)
      track (i2) (provided += _)
    }
    assertResult (Set (1, 2, 3)) (consumed)
    assertResult (Set (1, 2)) (provided)
  }

  it should "work with the for keyword" in {
    val xs = Seq.newBuilder [Int]
    implicit val scheduler = StubScheduler.random()
    val task =
      for (x <- adapt (1, 2, 3))
        supply (xs += x)
    task.pass
    assertResult (Seq (1, 2, 3)) (xs.result)
  }

  "AsyncIterator.map" should "handle an empty sequence" in {
    implicit val scheduler = StubScheduler.random()
    assertSeq () (map [Int, Int] () (_ * 2))
  }

  it should "handle a sequence of one element" in {
    implicit val scheduler = StubScheduler.random()
    assertSeq (2) (map (1) (_ * 2))
  }

  it should "handle a sequence of three elements" in {
    implicit val scheduler = StubScheduler.random()
    assertSeq (2, 4, 6) (map (1, 2, 3) (_ * 2))
  }

  it should "stop at the first exception from the input iterator" in {
    implicit val scheduler = StubScheduler.random()
    var consumed = Set.empty [Int]
    var provided = Set.empty [Int]
    assertFail [DistinguishedException] {
      val i1 = track (adapt (1, 2, 3, 4)) (consumed += _)
      val i2 = failWhen (i1) (_ == 3)
      val i3 = i2 map (_ * 2)
      track (i3) (provided += _)
    }
    assertResult (Set (1, 2, 3)) (consumed)
    assertResult (Set (2, 4)) (provided)
  }

  it should "work with the for keyword" in {
    implicit val scheduler = StubScheduler.random()
    val iter = for (x <- adapt (1, 2, 3)) yield x * 2
    assertSeq (2, 4, 6) (iter)
  }

  "AsyncIterator.flatMap" should "handle empty sequences" in {
    implicit val scheduler = StubScheduler.random()
    assertSeq () (flatMap [Int, Int] () (1, 2, 3))
  }

  it should "handle an empty outer sequence" in {
    implicit val scheduler = StubScheduler.random()
    assertSeq () (flatMap [Int, Int] () (1, 2, 3))
  }

  it should "handle an empty inner sequence" in {
    implicit val scheduler = StubScheduler.random()
    assertSeq () (flatMap [Int, Int] (1, 2, 3) ())
  }

  it should "handle an outer sequence of 1" in {
    implicit val scheduler = StubScheduler.random()
    assertSeq ((1, 1), (1, 2), (1, 3)) (flatMap [Int, Int] (1) (1, 2, 3))
  }

  it should "handle an inner sequence of 1" in {
    implicit val scheduler = StubScheduler.random()
    assertSeq ((1, 1), (2, 1), (3, 1)) (flatMap [Int, Int] (1, 2, 3) (1))
  }

  it should "handle an both sequences of three" in {
    implicit val scheduler = StubScheduler.random()
    assertSeq ((1,1), (1,2), (1,3), (2,1), (2,2), (2,3), (3,1), (3,2), (3,3)) {
      flatMap [Int, Int] (1, 2, 3) (1, 2, 3)
    }}

  it should "stop at the first exception from the outer iterator" in {
    implicit val scheduler = StubScheduler.random()
    var consumed = Set.empty [Int]
    var provided = Set.empty [(Int, Int)]
    assertFail [DistinguishedException] {
      val i1 = track (adapt (1, 2, 3, 4)) (consumed += _)
      val i2 = failWhen (i1) (_ == 3)
      val i3 = i2 flatMap (x => adapt (1) map (y => (x, y)))
      track (i3) (provided += _)
    }
    assertResult (Set (1, 2, 3)) (consumed)
    assertResult (Set ((1, 1), (2, 1))) (provided)
  }

  it should "stop at the first exception from the inner iterator" in {
    implicit val scheduler = StubScheduler.random()
    var consumed = Set.empty [Int]
    var provided = Set.empty [(Int, Int)]
    assertFail [DistinguishedException] {
      val i1 = track (adapt (1, 2, 3, 4)) (consumed += _)
      val i2 = failWhen (i1) (_ == 3)
      val i3 = adapt (1, 2, 3) flatMap (x => i2 map (y => (x, y)))
      track (i3) (provided += _)
    }
    assertResult (Set (1, 2, 3)) (consumed)
    assertResult (Set ((1, 1), (1, 2))) (provided)
  }

  "AsyncIterator.filter" should "handle [] -> []" in {
    implicit val scheduler = StubScheduler.random()
    assertSeq () (filter [Int] () (_ => false))
  }

  it should "handle [1] -> []" in {
    implicit val scheduler = StubScheduler.random()
    assertSeq () (filter [Int] () (_ => false))
  }

  it should "handle [1] -> [1]" in {
    implicit val scheduler = StubScheduler.random()
    assertSeq (1) (filter (1) (_ => true))
  }

  it should "handle [1, 2] -> []" in {
    implicit val scheduler = StubScheduler.random()
    assertSeq () (filter (1, 2) (_ => false))
  }

  it should "handle [1, 2] -> [1, 2]" in {
    implicit val scheduler = StubScheduler.random()
    assertSeq (1, 2) (filter (1, 2) (_ => true))
  }

  it should "handle [1, 2] -> [1]" in {
    implicit val scheduler = StubScheduler.random()
    assertSeq (1) (filter (1, 2) (i => (i & 1) == 1))
  }

  it should "handle [1, 2] -> [2]" in {
    implicit val scheduler = StubScheduler.random()
    assertSeq (2) (filter (1, 2) (i => (i & 1) == 0))
  }

  it should "handle [1, 2, 3] -> []" in {
    implicit val scheduler = StubScheduler.random()
    assertSeq () (filter (1, 2, 3) (_ => false))
  }

  it should "handle [1, 2, 3] -> [1, 2, 3]" in {
    implicit val scheduler = StubScheduler.random()
    assertSeq (1, 2, 3) (filter (1, 2, 3) (_ => true))
  }

  it should "handle [1, 2, 3] -> [2]" in {
    implicit val scheduler = StubScheduler.random()
    assertSeq (2) (filter (1, 2, 3) (i => (i & 1) == 0))
  }

  it should "handle [1, 2, 3] -> [1, 3]" in {
    implicit val scheduler = StubScheduler.random()
    assertSeq (1, 3) (filter (1, 2, 3) (i => (i & 1) == 1))
  }

  it should "handle [1, 2, 3] -> [1, 2]" in {
    implicit val scheduler = StubScheduler.random()
    assertSeq (1, 2) (filter (1, 2, 3) (_ < 3))
  }

  it should "handle [1, 2, 3] -> [2, 3]" in {
    implicit val scheduler = StubScheduler.random()
    assertSeq (2, 3) (filter (1, 2, 3) (_ > 1))
  }

  it should "stop at the first exception from the input iterator" in {
    implicit val scheduler = StubScheduler.random()
    var consumed = Set.empty [Int]
    var provided = Set.empty [Int]
    assertFail [DistinguishedException] {
      val i1 = track (adapt (1, 2, 3, 4)) (consumed += _)
      val i2 = failWhen (i1) (_ == 3)
      val i3 = i2.filter (i => (i & 1) == 1)
      track (i3) (provided += _)
    }
    assertResult (Set (1, 2, 3)) (consumed)
    assertResult (Set (1)) (provided)
  }

  it should "work with the for keyword" in {
    implicit val scheduler = StubScheduler.random()
    val iter =
      for (x <- adapt (1, 2, 3, 4); if (x & 1) == 0) yield x
    assertSeq (2, 4) (iter)
  }

  "AsyncIterator.whilst" should "stop when the condition fails before the iterator stops" in {
    implicit val scheduler = StubScheduler.random()
    var seen = Set.empty [Int]
    val last = whilst (1, 2, 3) (_ < 3) (seen += _)
    assertResult (Set (1, 2)) (seen)
    assertResult (Some (3)) (last)
  }

  it should "stop when the input iterator stops before the condition fails" in {
    implicit val scheduler = StubScheduler.random()
    var seen = Set.empty [Int]
    val last = whilst (1, 2, 3) (_ < 4) (seen += _)
    assertResult (Set (1, 2, 3)) (seen)
    assertResult (None) (last)
  }

  it should "stop when the condition fails immediately" in {
    implicit val scheduler = StubScheduler.random()
    var seen = Set.empty [Int]
    val last = whilst (1, 2, 3) (_ < 0) (seen += _)
    assertResult (Set.empty) (seen)
    assertResult (Some (1)) (last)
  }

  it should "handle an empty input iterator when the condition passes" in {
    implicit val scheduler = StubScheduler.random()
    var seen = Set.empty [Int]
    val last = whilst () (_ => true) (seen += _)
    assertResult (Set.empty) (seen)
    assertResult (None) (last)
  }

  it should "handle an empty input iterator when the condition fails" in {
    implicit val scheduler = StubScheduler.random()
    var seen = Set.empty [Int]
    val last = whilst () (_ => false) (seen += _)
    assertResult (Set.empty) (seen)
    assertResult (None) (last)
  }

  it should "stop at the first exception from the input iterator" in {
    implicit val scheduler = StubScheduler.random()
    var consumed = Set.empty [Int]
    var seen = Set.empty [Int]
    val i1 = track (adapt (1, 2, 3, 4)) (consumed += _)
    val i2 = failWhen (i1) (_ == 3)
    i2.whilst (_ < 5) (x => supply (seen += x)) .fail [DistinguishedException]
    assertResult (Set (1, 2, 3)) (consumed)
    assertResult (Set (1, 2)) (seen)
  }

  it should "stop at the first exception thrown from the predicate" in {
    implicit val scheduler = StubScheduler.random()
    var consumed = Set.empty [Int]
    var seen = Set.empty [Int]
    val i1 = track (adapt (1, 2, 3, 4)) (consumed += _)
    i1.whilst { x =>
      if (x == 3) throw new DistinguishedException
      x < 5
    } (x => supply (seen += x)) .fail [DistinguishedException]
    assertResult (Set (1, 2, 3)) (consumed)
    assertResult (Set (1, 2)) (seen)
  }

  it should "stop at the first exception thrown from the body" in {
    implicit val scheduler = StubScheduler.random()
    var consumed = Set.empty [Int]
    var seen = Set.empty [Int]
    val i1 = track (adapt (1, 2, 3, 4)) (consumed += _)
    i1.whilst (_ < 5) { x =>
      if (x == 3) throw new DistinguishedException
      supply (seen += x)
    } .fail [DistinguishedException]
    assertResult (Set (1, 2, 3)) (consumed)
    assertResult (Set (1, 2)) (seen)
  }

  it should "stop at the first exception returned from the body" in {
    implicit val scheduler = StubScheduler.random()
    var consumed = Set.empty [Int]
    var seen = Set.empty [Int]
    val i1 = track (adapt (1, 2, 3, 4)) (consumed += _)
    i1.whilst (_ < 5) { x =>
      supply {
        if (x == 3) throw new DistinguishedException
        seen += x
      }
    } .fail [DistinguishedException]
    assertResult (Set (1, 2, 3)) (consumed)
    assertResult (Set (1, 2)) (seen)
  }

  "AsyncIterator.toMap" should "build the elements into a map" in {
    implicit val scheduler = StubScheduler.random()
    assertResult (Map.empty) {
      adapt() .toMap.pass
    }
    assertResult (Map (1 -> 10)) {
      adapt (1 -> 10) .toMap.pass
    }
    assertResult (Map (1 -> 10, 2 -> 20)) {
      adapt (1 -> 10, 2 -> 20) .toMap.pass
    }
    assertResult (Map (1 -> 10, 2 -> 20, 3 -> 30)) {
      adapt (1 -> 10, 2 -> 20, 3 -> 30) .toMap.pass
    }}

  "AsyncIterator.toMapWhile" should "build the initial elements into a map" in {
    implicit val scheduler = StubScheduler.random()
    assertResult ((Map.empty, None)) {
      adapt [(Int, Int)] () .toMapWhile (_._1 < 2) .pass
    }
    assertResult ((Map (1 -> 10), None)) {
      adapt (1 -> 10) .toMapWhile (_._1 < 2) .pass
    }
    assertResult ((Map (1 -> 10), Some (2 -> 20))) {
      adapt (1 -> 10, 2 -> 20) .toMapWhile (_._1 < 2) .pass
    }
    assertResult ((Map (1 -> 10), Some (2 -> 20))) {
      adapt (1 -> 10, 2 -> 20, 3 -> 30) .toMapWhile (_._1 < 2) .pass
    }}

  "AsyncIterator.toSeq" should "build the elements into a sequence" in {
    implicit val scheduler = StubScheduler.random()
    assertResult (Seq.empty) {
      adapt() .toSeq.pass
    }
    assertResult (Seq (1)) {
      adapt (1) .toSeq.pass
    }
    assertResult (Seq (1, 2)) {
      adapt (1, 2) .toSeq.pass
    }
    assertResult (Seq (1, 2, 3)) {
      adapt (1, 2, 3) .toSeq.pass
    }}

  "AsyncIterator.toSeqWhile" should "build the initial elements into a sequence" in {
    implicit val scheduler = StubScheduler.random()
    assertResult ((Seq.empty, None)) {
      adapt [Int] () .toSeqWhile (_ < 2) .pass
    }
    assertResult ((Seq (1), None)) {
      adapt (1) .toSeqWhile (_ < 2) .pass
    }
    assertResult ((Seq (1), Some (2))) {
      adapt (1, 2) .toSeqWhile (_ < 2) .pass
    }
    assertResult ((Seq (1), Some (2))) {
      adapt (1, 2, 3) .toSeqWhile (_ < 2) .pass
    }}}
