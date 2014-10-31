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

package movies

import com.treode.async.Async
import com.treode.cluster.HostId
import com.treode.finatra.AsyncFinatraController
import com.treode.store.Store
import com.twitter.finatra.{Request, ResponseBuilder}

import movies.{PhysicalModel => PM}

class AnalyticsResource (implicit store: Store) extends AsyncFinatraController {

  // TODO: Stream these potentially long responses.
  // These scan the full table an build the entire JSON array before sending any of it. Finatra
  // does not make it easy to stream a response. We are exploring options.

  get ("/rdd/movies") { request =>
    val rt = request.getLastModificationBefore
    for {
      result <- PM.Movie.list (rt)
    } yield {
      render.ok.appjson (request, result)
    }}

  get ("/rdd/actors") { request =>
    val rt = request.getLastModificationBefore
    for {
      result <- PM.Actor.list (rt)
    } yield {
      render.ok.appjson (request, result)
    }}

  get ("/rdd/roles") { request =>
    val rt = request.getLastModificationBefore
    for {
      result <- PM.Roles.list (rt)
    } yield {
      render.ok.appjson (request, result)
    }}
}
