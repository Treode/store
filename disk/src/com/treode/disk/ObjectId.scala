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
import com.treode.pickle.Picklers

class ObjectId private (val id: Long) extends AnyVal with Ordered [ObjectId] {

  def compare (that: ObjectId): Int =
    UnsignedLongs.compare (this.id, that.id)

  override def toString =
    if (id < 256) f"Object:$id%02X" else f"Object:$id%016X"
}

object ObjectId extends Ordering [ObjectId] {

  val MinValue = ObjectId (0)

  val MaxValue = ObjectId (-1)

  implicit def apply (id: Long): ObjectId =
    new ObjectId (id)

  def compare (x: ObjectId, y: ObjectId): Int =
    x compare y

  val pickler = {
    import Picklers._
    wrap (fixedLong) build (apply _) inspect (_.id)
  }}
