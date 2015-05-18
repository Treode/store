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

import com.treode.notify.Notification, Notification._
import com.treode.twitter.finagle.http.{RichResponse, mapper}
import com.twitter.finagle.http.{Request, Response, Status}
import org.jboss.netty.handler.codec.http.HttpResponseStatus

package object handler {

  private [handler] object respond {

    def apply (req: Request, status: HttpResponseStatus = Status.Ok): Response = {
      val rsp = req.response
      rsp.status = status
      rsp
    }

    def apply (req: Request, note: Notification [Unit]): Response = {
      val rsp = req.response
      note match {
        case errors @ Errors (_) =>
          rsp.write(mapper.writeValueAsString (note))
          rsp.status = Status.BadRequest
        case Result (_) =>
          rsp.status = Status.Ok
      }
      rsp
    }

    def json (req: Request, value: Any): Response = {
      val rsp = req.response
      rsp.status = Status.Ok
      rsp.json = value
      rsp
    }}}
