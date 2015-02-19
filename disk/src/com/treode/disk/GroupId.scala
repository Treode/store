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

class GroupId private (val id: Long) extends AnyVal with Ordered [GroupId] {

  def compare (that: GroupId): Int =
    UnsignedLongs.compare (this.id, that.id)

  override def toString =
    if (id < 256) f"Group:$id%02X" else f"Group:$id%016X"
}

object GroupId extends Ordering [GroupId] {

  val MinValue = GroupId (0)

  val MaxValue = GroupId (-1)

  implicit def apply (id: Long): GroupId =
    new GroupId (id)

  def compare (x: GroupId, y: GroupId): Int =
    x compare y

  val pickler = {
    import Picklers._
    wrap (ulong) build (apply _) inspect (_.id)
  }}
