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

import GenerationDocket.DocketId

/** A convenient facade for a `Map [(TypeId, ObjectId), Set [Long]]`. */
private class GenerationDocket private (private var pages: Map [DocketId, Set [Long]])
extends Iterable [(DocketId, Set [Long])] with Traversable [(DocketId, Set [Long])] {

  def this() = this (Map.empty.withDefaultValue (Set.empty))

  def add (id: DocketId, gen: Long): Unit =
    pages += id -> (pages (id) + gen)

  def add (typ: TypeId, obj: ObjectId, gen: Long): Unit =
    add (DocketId (typ, obj), gen)

  def add (id: DocketId, gens: Set [Long]): Unit =
    pages += id -> (pages (id) ++ gens)

  def add (typ: TypeId, obj: ObjectId, gens: Set [Long]): Unit =
    add (DocketId (typ, obj), gens)

  def remove (id: DocketId): Set [Long] = {
    val gens = pages (id)
    pages -= id
    gens
  }

  def remove (typ: TypeId, obj: ObjectId): Set [Long] =
    remove (DocketId (typ, obj))

  def contains (id: DocketId): Boolean =
    pages contains id

  def contains (typ: TypeId, obj: ObjectId): Boolean =
    contains (DocketId (typ, obj))

  def contains (id: DocketId, gen: Long): Boolean =
    pages (id) contains gen

  def contains (typ: TypeId, obj: ObjectId, gen: Long): Boolean =
    contains (DocketId (typ, obj), gen)

  def iterator = pages.iterator

  override def isEmpty = pages.isEmpty

  override def foreach [U] (f: ((DocketId, Set [Long])) => U) = pages.foreach (f)

  override def toString = s"GenerationDocket(${pages mkString ","})"
}

private object GenerationDocket {

  case class DocketId (typ: TypeId, obj: ObjectId)
}
