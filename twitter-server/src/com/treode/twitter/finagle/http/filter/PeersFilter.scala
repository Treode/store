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

import java.net.SocketAddress

import com.treode.store.{Store, StoreController}
import com.treode.twitter.finagle.http.{RichRequest, RichResponse, mapper}
import com.twitter.util.Future
import com.twitter.finagle.{Filter, Service}
import com.twitter.finagle.http.{Method, Request, Response, Status}

class PeersFilter private (path: String, controller: StoreController)
extends Filter [Request, Response, Request, Response] {

  def apply (req: Request, service: Service [Request, Response]): Future [Response] = {
    if (req.path == path) {
      req.method match {
        case Method.Get =>
          val rsp = req.response
          rsp.status = Status.Ok
          rsp.json = controller.hosts (req.slice)
          Future.value (rsp)
        case _ =>
          val rsp = req.response
          rsp.status = Status.MethodNotAllowed
          Future.value (rsp)
      }
    } else {
      service (req)
    }}}

object PeersFilter {

  def apply (path: String, controller: StoreController): PeersFilter =
    new PeersFilter (path, controller)
}
