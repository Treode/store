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

package com.treode.disk.stubs

import com.treode.disk.{GroupId, ObjectId, PageDescriptor, TypeId}

private case class StubPage (typ: TypeId, obj: ObjectId, group: GroupId, data: Array [Byte]) {

  def length = data.length
}

private object StubPage {

  def apply [P] (desc: PageDescriptor [P], obj: ObjectId, group: GroupId, page: P): StubPage =
    new StubPage (desc.id, obj, group, desc.ppag.toByteArray (page))
}
