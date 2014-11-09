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

package com.treode.twitter.finagle.http.filter

import com.fasterxml.jackson.core.JsonProcessingException
import com.twitter.finagle.{Filter, Service}
import com.twitter.finagle.http.{Request, Response, Status}
import com.twitter.util.Future

object JsonExceptionFilter extends Filter [Request, Response, Request, Response] {

  private def respond (req: Request, exc: Exception): Future [Response] = {
    val rsp = req.response
    rsp.status = Status.BadRequest
    rsp.mediaType = "text/plain"
    rsp.write (exc.getMessage)
    rsp.close()
    Future.value (rsp)
  }

  def apply (req: Request, service: Service [Request, Response]): Future [Response] =
    Future.monitored {
        service (req.asInstanceOf [Request])
    } rescue {
    case e: JsonProcessingException =>
      respond (req, e)
    }}
