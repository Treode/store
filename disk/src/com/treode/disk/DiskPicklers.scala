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

import java.nio.file.Paths
import com.treode.pickle.Picklers

private trait DiskPicklers extends Picklers {

  def path = wrap (string) build (Paths.get (_)) inspect (_.toString)

  def boot = BootBlock.pickler
  def intSet = IntSet.pickler
  def geometry = DriveGeometry.pickler
  def groupId = GroupId.pickler
  def objectId = ObjectId.pickler
  def pageLedger = PageLedger.Zipped.pickler
  def pos = Position.pickler
  def typeId = TypeId.pickler
}

private object DiskPicklers extends DiskPicklers
