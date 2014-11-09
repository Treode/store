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

import java.lang.{Integer => JInt}
import java.nio.file.Path
import java.util.{Arrays, Objects}

private case class BootBlock (
    sysid: Array [Byte],
    gen: Int,
    number: Int,
    drives: Set [Path]) {

  override def hashCode: Int =
    Objects.hash (Arrays.hashCode (sysid): JInt, gen: JInt, number: JInt, drives)

  override def equals (other: Any): Boolean =
    other match {
      case that: BootBlock =>
        Arrays.equals (sysid, that.sysid) &&
        gen == that.gen &&
        number == that.number &&
        drives == that.drives
      case _ => false
    }

  val (hostId,cellId) = DiskPicklers.sysid.fromByteArray (sysid)
  log.superBootBlockSysid(hostId,cellId,drives)

}

private object BootBlock {

  val pickler = {
    import DiskPicklers._
    wrap (array (byte), uint, uint, set (path))
    .build ((apply _).tupled)
    .inspect (v => (v.sysid, v.gen, v.number, v.drives))
  }}
