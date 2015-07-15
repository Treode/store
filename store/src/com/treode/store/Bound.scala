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

import com.treode.pickle.Pickler

/** Minimum and maximum bounds.  Case classes are nested in the [[Bound$ companion object]]. */
sealed abstract class InfiniteBound [A] {

  /** Less than accounting for inclusive/exclusive. */
  def <* (v: A) (implicit ordering: Ordering [A]): Boolean

  /** Greater than accounting for inclusive/exclusive. */
  def >* (v: A) (implicit ordering: Ordering [A]): Boolean

  def map [B] (f: A => B): InfiniteBound [B]
}

/** Inclusive and exclusive bounds.  Case classes are nested in the [[Bound$ companion object]]. */
sealed abstract class Bound [A] extends InfiniteBound [A] {

  def bound: A

  def inclusive: Boolean

  def map [B] (f: A => B): Bound [B]
}

object Bound {

  case class Minimum [A] () extends InfiniteBound [A] {

    def <* (other: A) (implicit ordering: Ordering [A]) =
      true

    def >*  (other: A) (implicit ordering: Ordering [A]) =
      false

    def map [B] (f: A => B): InfiniteBound [B] =
      Minimum()
  }

  case class Maximum [A] () extends InfiniteBound [A] {

    def <* (other: A) (implicit ordering: Ordering [A]) =
      false

    def >*  (other: A) (implicit ordering: Ordering [A]) =
      true

    def map [B] (f: A => B): InfiniteBound [B] =
      Maximum()
  }

  case class Inclusive [A] (bound: A) extends Bound [A] {

    def inclusive = true

    def <* (other: A) (implicit ordering: Ordering [A]) =
      ordering.lteq (bound, other)

    def >* (other: A) (implicit ordering: Ordering [A]) =
      ordering.gteq (bound, other)

    def map [B] (f: A => B): Bound [B] =
      Inclusive (f (bound))
  }

  case class Exclusive [A] (bound: A) extends Bound [A] {

    def inclusive = false

    def <* (other: A) (implicit ordering: Ordering [A]) =
      ordering.lt (bound, other)

    def >*  (other: A) (implicit ordering: Ordering [A]) =
      ordering.gt (bound, other)

    def map [B] (f: A => B): Bound [B] =
      Exclusive (f (bound))
  }

  def apply [A] (bound: A, inclusive: Boolean): Bound [A] =
    if (inclusive)
      Inclusive (bound)
    else
      Exclusive (bound)

  val firstKey = Inclusive (Key.MinValue)

  def pickler [A] (pa: Pickler [A]) = {
    import StorePicklers._
    tagged [Bound [A]] (
      0x1 -> wrap (pa) .build (new Inclusive (_)) .inspect (_.bound),
      0x2 -> wrap (pa) .build (new Exclusive (_)) .inspect (_.bound))
    }}
