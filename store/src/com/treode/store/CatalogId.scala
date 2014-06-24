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

import scala.language.implicitConversions

import com.google.common.primitives.UnsignedLongs
import com.treode.pickle.Picklers

class CatalogId (val id: Long) extends AnyVal with Ordered [CatalogId] {

  def compare (that: CatalogId): Int =
    UnsignedLongs.compare (this.id, that.id)

  override def toString =
    if (id < 256) f"Catalog:$id%02X" else f"Catalog:$id%016X"
}

object CatalogId extends Ordering [CatalogId] {

  val MinValue = CatalogId (0)

  val MaxValue = CatalogId (-1)

  implicit def apply (id: Long): CatalogId =
    new CatalogId (id)

  def compare (x: CatalogId, y: CatalogId): Int =
    x compare y

  val pickler = {
    import Picklers._
    wrap (fixedLong) build (apply _) inspect (_.id)
  }}
