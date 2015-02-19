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

import com.treode.async.Callback
import com.treode.buffer.Output

private abstract class PickledPage (
    val typ: TypeId,
    val obj: ObjectId,
    val group: GroupId,
    val cb: Callback [Position]
) {
  def byteSize: Int
  def write (out: Output)
}

private object PickledPage {

  def apply [P] (
      desc: PageDescriptor [P],
      obj: ObjectId,
      group: GroupId,
      page: P,
      cb: Callback [Position]
  ): PickledPage =
    new PickledPage (desc.id, obj, group, cb) {
      def byteSize = desc.ppag.byteSize (page)
      def write (out: Output) = desc.ppag.pickle (page, out)
      override def toString = s"PickledPage($obj, $group)"
    }}
