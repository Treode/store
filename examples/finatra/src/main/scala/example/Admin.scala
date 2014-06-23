package example

import java.nio.file.Path

import com.fasterxml.jackson.databind.JsonNode
import com.treode.async.Async
import com.treode.disk.DriveAttachment
import com.treode.store.{Cohort, Store}
import com.twitter.finatra.{Controller => FinatraController}

import Async.supply
import Store.Controller

class Admin (controller: Controller) extends AsyncFinatraController {

  get ("/atlas") { request =>
    supply (render.appjson (controller.cohorts))
  }

  put ("/atlas") { request =>
    controller.cohorts = request.readJsonAs [Array [Cohort]]
    supply (render.ok)
  }

  get ("/drives") { request =>
    for {
      drives <- controller.drives
    } yield {
      render.appjson (drives)
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
    }}}
