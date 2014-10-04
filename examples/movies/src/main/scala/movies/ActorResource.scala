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
import com.treode.store._
import com.twitter.finatra.{Request, ResponseBuilder}

import movies.{DisplayModel => DM}
import Async.supply

class ActorResource (host: HostId, movies: MovieStore) extends AsyncFinatraController {

  def read (request: Request, id: String): Async [ResponseBuilder] = {
    val rt = request.getLastModificationBefore
    val ct = request.getIfModifiedSince
    for {
      (vt, movie) <- movies.readActor (rt, id)
    } yield {
      movie match {
        case Some (v) if ct < vt =>
          render.header (ETag, vt.toString) .appjson (v)
        case Some (v) =>
          render.status (NotModified) .nothing
        case None =>
          render.notFound.nothing
      }}}

  def query (request: Request): Async [ResponseBuilder] = supply {
    render.status (NotImplemented) .nothing
  }

  get ("/actor/:id") { request =>
    request.getId match {
      case Some (id) => read (request, id)
      case None => query (request)
    }}

  get ("/actor") { request =>
    query (request)
  }

  def post (request: Request): Async [ResponseBuilder] = {
    val xid = request.getTransactionId (host)
    val ct = request.getIfUnmodifiedSince
    val actor = request.readJsonAs [DM.Actor] ()
    (for {
      (id, vt) <- movies.create (xid, ct, actor)
    } yield {
      render.ok
        .header (ETag, vt.toString)
        .header (Location, s"/actor/$id")
        .nothing
    }) .recover {
      case _: StaleException =>
        render.status (PreconditionFailed) .nothing
    }}

  post ("/actor/") { request =>
    post (request)
  }

  post ("/actor") { request =>
    post (request)
  }

  put ("/actor/:id") { request =>
    val xid = request.getTransactionId (host)
    val ct = request.getIfUnmodifiedSince
    val id = request.getId.getOrBadRequest ("ID required")
    val actor = request.readJsonAs [DM.Actor] ()
    (for {
      vt <- movies.update (xid, ct, id, actor)
    } yield {
      render.ok.header (ETag, vt.toString) .nothing
    }) .recover {
      case _: StaleException =>
        render.status (PreconditionFailed) .nothing
    }}}
