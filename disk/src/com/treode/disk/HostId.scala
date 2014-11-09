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

class HostId private (val id: Long) extends AnyVal with Ordered [HostId] {

  def compare (that: HostId): Int =
    UnsignedLongs.compare (this.id, that.id)

  override def toString =
    if (id < 256) f"Host:$id%02X" else f"Host:$id%016X"
}

object HostId extends Ordering [HostId] {

  val MinValue = HostId (0)

  val MaxValue = HostId (-1)

  implicit def apply (id: Long): HostId =
    new HostId (id)

  def compare (x: HostId, y: HostId): Int =
    x compare y

  val pickler = {
    import Picklers._
    wrap (fixedLong) build (apply _) inspect (_.id)
  }}
