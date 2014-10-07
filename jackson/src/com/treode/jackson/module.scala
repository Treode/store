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

package com.treode.jackson

import java.nio.file.Path
import scala.collection.JavaConversions._

import com.fasterxml.jackson.core.Version
import com.fasterxml.jackson.databind.JsonDeserializer
import com.fasterxml.jackson.databind.module.SimpleModule
import com.treode.cluster.HostId
import com.treode.disk.{DriveAttachment, DriveGeometry}
import com.treode.store.Cohort

import Cohort.{Empty, Issuing, Moving, Settled}

class TreodeModule extends SimpleModule (
    "TreodeModule",
     new Version (0, 1, 0, "", "", ""),
     Map [Class [_], JsonDeserializer [_]] (
         classOf [DriveGeometry] -> DriveGeometryDeserializer,
         classOf [DriveAttachment] -> DriveAttachmentDeserializer,
         classOf [Cohort] -> CohortDeserializer,
         classOf [HostId] -> HostIdDeserializer,
         classOf [Path] -> PathDeserializer),
     List (
         DriveGeometrySerializer,
         DriveAttachmentSerializer,
         DriveDigestSerializer,
         EmptySerializer,
         HostIdSerializer,
         IssuingSerializer,
         MovingSerializer,
         PathSerializer,
         SettledSerializer,
         TableDigestSerializer,
         TierDigestSerializer))

object DefaultTreodeModule extends TreodeModule
