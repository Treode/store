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

import java.nio.file.Path

import com.fasterxml.jackson.databind.JsonNode
import com.treode.async.Async
import com.treode.disk.DriveAttachment
import com.treode.finatra.AsyncFinatraController
import com.treode.store.{Cohort, Store}
import com.twitter.finatra.{Controller => FinatraController}

import Async.supply
import Store.Controller

class Admin (controller: Controller) extends AsyncFinatraController {

  get ("/atlas") { request =>
    supply (render.appjson (request, controller.cohorts))
  }

  put ("/atlas") { request =>
    controller.cohorts = request.readJsonAs [Array [Cohort]]
    supply (render.ok)
  }

  get ("/drives") { request =>
    for {
      drives <- controller.drives
    } yield {
      render.appjson (request, drives)
    }}

  post ("/drives/attach") { request =>
    val drives = request.readJsonAs [Seq [DriveAttachment]]
    for {
      _ <- controller.attach (drives: _*)
    } yield {
      render.ok
    }}

  post ("/drives/drain") { request =>
    val paths = request.readJsonAs [Seq [Path]]
    for {
      _ <- controller.drain (paths: _*)
    } yield {
      render.ok
    }}

  get ("/tables") { request =>
    for {
      tables <- controller.tables
    } yield {
      render.appjson (request, tables)
    }}}
