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
         SettledSerializer))

object DefaultTreodeModule extends TreodeModule
