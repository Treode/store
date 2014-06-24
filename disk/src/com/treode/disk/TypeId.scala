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

import scala.language.implicitConversions

import com.google.common.primitives.UnsignedLongs

class TypeId private (val id: Long) extends AnyVal with Ordered [TypeId] {

  def compare (that: TypeId): Int =
    UnsignedLongs.compare (this.id, that.id)

  override def toString =
    if (id < 256) f"Type:$id%02X" else f"Type:$id%016X"
}

object TypeId extends Ordering [TypeId] {

  val MinValue = TypeId (0)

  val MaxValue = TypeId (-1)

  implicit def apply (id: Long): TypeId =
    new TypeId (id)

  def compare (x: TypeId, y: TypeId): Int =
    x compare y

  val pickler = {
    import DiskPicklers._
    wrap (fixedLong) build (new TypeId (_)) inspect (_.id)
  }}
