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

package com.treode.twitter.server

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.treode.jackson.DefaultTreodeModule
import com.twitter.finagle.http.{MediaType, Request, Response}
import org.jboss.netty.handler.codec.http.HttpResponseStatus

package object handler {

  private [handler] val mapper = new ObjectMapper with ScalaObjectMapper
  mapper.registerModule (DefaultScalaModule)
  mapper.registerModule (DefaultTreodeModule)

  private [handler] object respond {

    def apply (req: Request, status: HttpResponseStatus): Response = {
      val rsp = req.response
      rsp.status = status
      rsp
    }

    def json (req: Request, status: HttpResponseStatus, value: Any): Response = {
      val rsp = req.response
      rsp.status = status
      rsp.mediaType = MediaType.Json
      rsp.write (mapper.writeValueAsString (value))
      rsp.close()
      rsp
    }
  }

  private [handler] implicit class RichRequest (request: Request) {

    def readJson [A: Manifest]: A =
      request.withReader (mapper.readValue [A] (_))
  }}
