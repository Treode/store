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
import com.fasterxml.jackson.core.{JsonGenerator, JsonParser, TreeNode}
import com.fasterxml.jackson.databind.{DeserializationContext, JsonNode, SerializerProvider}
import com.fasterxml.jackson.databind.deser.std.StdDeserializer
import com.fasterxml.jackson.databind.ser.std.StdSerializer
import com.treode.disk.{DriveAttachment, DriveDigest, DriveGeometry}

object DriveGeometrySerializer extends StdSerializer [DriveGeometry] (classOf [DriveGeometry]) {

  def serialize (value: DriveGeometry, jgen: JsonGenerator, provider: SerializerProvider) {
    val codec = jgen.getCodec
    jgen.writeStartObject()
    jgen.writeObjectField ("segmentBits", value.segmentBits)
    jgen.writeObjectField ("blockBits", value.blockBits)
    jgen.writeObjectField ("diskBytes", value.diskBytes)
    jgen.writeEndObject()
  }}

object DriveGeometryDeserializer extends StdDeserializer [DriveGeometry] (classOf [DriveGeometry]) {

  def deserialize (jparser: JsonParser, context: DeserializationContext): DriveGeometry = {
    val codec = jparser.getCodec
    val node = codec.readTree [TreeNode] (jparser)
    val segmentBits = node.get ("segmentBits") .asInstanceOf [JsonNode]
    val blockBits = node.get ("blockBits") .asInstanceOf [JsonNode]
    val diskBytes = node.get ("diskBytes") .asInstanceOf [JsonNode]
    if (segmentBits == null || blockBits == null || diskBytes == null)
      throw context.mappingException ("Malformed drive geometry")
    try {
      DriveGeometry (segmentBits.asInt, blockBits.asInt, diskBytes.asLong)
    } catch {
      case e: IllegalArgumentException =>
        throw context.mappingException (e.getMessage)
    }}}

object DriveDigestSerializer extends StdSerializer [DriveDigest] (classOf [DriveDigest]) {

  def serialize (value: DriveDigest, jgen: JsonGenerator, provider: SerializerProvider) {
    val codec = jgen.getCodec
    jgen.writeStartObject()
    jgen.writeFieldName ("path")
    codec.writeValue (jgen, value.path)
    jgen.writeFieldName ("geometry")
    codec.writeValue (jgen, value.geometry)
    jgen.writeObjectField ("allocated", value.allocated)
    jgen.writeObjectField ("draining", value.draining)
    jgen.writeEndObject()
  }}

object DriveAttachmentSerializer
extends StdSerializer [DriveAttachment] (classOf [DriveAttachment]) {

  def serialize (value: DriveAttachment, jgen: JsonGenerator, provider: SerializerProvider) {
    val codec = jgen.getCodec
    jgen.writeStartObject()
    jgen.writeFieldName ("path")
    codec.writeValue (jgen, value.path)
    jgen.writeFieldName ("geometry")
    codec.writeValue (jgen, value.geometry)
    jgen.writeEndObject()
  }}

object DriveAttachmentDeserializer
extends StdDeserializer [DriveAttachment] (classOf [DriveAttachment]) {

  def deserialize (jparser: JsonParser, context: DeserializationContext): DriveAttachment = {
    val codec = jparser.getCodec
    val node = codec.readTree [TreeNode] (jparser)
    val _path = node.get ("path") .asInstanceOf [JsonNode]
    if (_path == null)
      throw context.mappingException ("Malformed drive attachment.")
    val path = codec.treeToValue (_path, classOf [Path])
    val _geom = node.get ("geometry") .asInstanceOf [JsonNode]
    val geom =
      if (_geom == null)
        DriveGeometry (30, 13, 1 << 40) // dummy
      else
        codec.treeToValue (_geom, classOf [DriveGeometry])
    DriveAttachment (path, geom)
  }}
