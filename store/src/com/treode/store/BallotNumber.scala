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

import com.treode.cluster.HostId

private class BallotNumber private (
    val number: Long,
    val host: HostId) extends Ordered [BallotNumber] {

  /** Prevent preferential treatment of higher numbered hosts. */
  private def ordinal = math.abs (number - host.id)

  def compare (that: BallotNumber): Int = {
    val r = number compareTo that.number
    if (r != 0) r else ordinal compareTo that.ordinal
  }

  override def equals (other: Any): Boolean =
    other match {
      case that: BallotNumber => number == that.number && host == that.host
      case _ => false
    }

  override def hashCode: Int = (number, host).hashCode

  override def toString = f"BallotNumber:$number%X:${host.id}%X"
}

private object BallotNumber extends Ordering [BallotNumber] {

  val zero = new BallotNumber (0, 0)

  def apply (number: Long, host: HostId) = new BallotNumber (number, host)

  def compare (x: BallotNumber, y: BallotNumber) = x compare (y)

  val pickler = {
    import StorePicklers._
    wrap (ulong, hostId) build {
      v => BallotNumber (v._1, v._2)
    } inspect {
      v => (v.number, v.host)
    }}}
