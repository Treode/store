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

package com.treode.disk

import scala.util.Random

class Stuff (val seed: GroupId, val items: Seq [Int]) {

  override def equals (other: Any): Boolean =
    other match {
      case that: Stuff => seed == that.seed && items == that.items
      case _ => false
    }

  override def toString = f"Stuff(0x${seed.id}%016X, 0x${items.hashCode}%08X)"
}

object Stuff {

  val countLimit = 100
  val valueLimit = Int.MaxValue

  def apply (seed: GroupId): Stuff = {
    val r = new Random (seed.id)
    val xs = Seq.fill (r.nextInt (countLimit)) (r.nextInt (valueLimit))
    new Stuff (seed, xs)
  }

  def apply (seed: GroupId, length: Int): Stuff = {
    val r = new Random (seed.id)
    val xs = Seq.fill (length) (r.nextInt (valueLimit))
    new Stuff (seed, xs)
  }

  val pickler = {
    import DiskPicklers._
    wrap (groupId, seq (int))
    .build (v => new Stuff (v._1, v._2))
    .inspect (v => (v.seed, v.items))
  }

  val pager = PageDescriptor (0x26, Stuff.pickler)
}
