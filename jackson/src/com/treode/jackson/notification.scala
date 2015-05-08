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

import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.databind.SerializerProvider
import com.fasterxml.jackson.databind.ser.std.StdSerializer
import com.treode.notify.Notification
import com.treode.notify.Message

object NotificationSerializer extends StdSerializer [Notification [_]] (classOf [Notification [_]]) {
  def serialize (value: Notification [_], jgen: JsonGenerator, provider: SerializerProvider) {
    val codec = jgen.getCodec
    jgen.writeStartArray()
    for (m <- value.messages)
      codec.writeValue (jgen, m)
    jgen.writeEndArray()
  }
}

object MessageSerializer extends StdSerializer [Message] (classOf [Message]) {
  def serialize (value: Message, jgen: JsonGenerator, provider: SerializerProvider) {
    val codec = jgen.getCodec
    jgen.writeStartObject()
    jgen.writeObjectField("message", value.en)
    jgen.writeEndObject()
  }
}
