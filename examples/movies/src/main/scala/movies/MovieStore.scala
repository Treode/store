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

import scala.util.Random

import com.treode.async.Async
import com.treode.store.{Store, TxClock, TxId}
import com.treode.store.util._

import movies.{DisplayModel => DM, PhysicalModel => PM}

import Async.{guard, supply}

/** See README.md. */
class MovieStore (implicit random: Random, store: Store) {

  def readMovie (rt: TxClock, movieId: Long): Async [(TxClock, Option [DM.Movie])] = {
    val tx = new Transaction (rt)
    for {
      _ <- PM.Movie.fetchForDisplay (tx, movieId)
    } yield {
      val movie =
        for (root <- tx.get (PM.MovieTable) (movieId))
          yield DM.Movie (tx, movieId, root)
      (tx.vt, movie)
    }}

  def create (xid: TxId, ct: TxClock, movie: DM.Movie): Async [(Long, TxClock)] = guard {
    val tx = new Transaction (ct)
    val movieId = random.nextPositiveLong()
    for {
      _ <- PM.Movie.fetchForSave (tx, movieId, movie.actorIds)
      _ = PM.Movie.create (tx, movieId, movie)
      wt <- tx.execute (xid)
    } yield {
      (movieId, wt)
    }}

  def update (xid: TxId, ct: TxClock, movieId: Long, movie: DM.Movie): Async [TxClock] = guard {
    val tx = new Transaction (ct)
    for {
      _ <- PM.Movie.fetchForSave (tx, movieId, movie.actorIds)
      _ = PM.Movie.save (tx, movieId, movie)
      wt <- tx.execute (xid)
    } yield {
      wt
    }}

  def readActor (rt: TxClock, actorId: Long): Async [(TxClock, Option [DM.Actor])] = {
    val tx = new Transaction (rt)
    for {
      _ <- PM.Actor.fetchForDisplay (tx, actorId)
    } yield {
      val actor =
        for (root <- tx.get (PM.ActorTable) (actorId))
          yield DM.Actor (tx, actorId, root)
      (tx.vt, actor)
    }}

  def create (xid: TxId, ct: TxClock, actor: DM.Actor): Async [(Long, TxClock)] = guard {
    val tx = new Transaction (ct)
    val actorId = random.nextPositiveLong()
    for {
      _ <- PM.Actor.fetchForSave (tx, actorId, actor.movieIds)
      _ = PM.Actor.create (tx, actorId, actor)
      wt <- tx.execute (xid)
    } yield {
      (actorId, wt)
    }}

  def update (xid: TxId, ct: TxClock, actorId: Long, actor: DM.Actor): Async [TxClock] = guard {
    val tx = new Transaction (ct)
    for {
      _ <- PM.Actor.fetchForSave (tx, actorId, actor.movieIds)
      _ = PM.Actor.save (tx, actorId, actor)
      wt <- tx.execute (xid)
    } yield {
      wt
    }}
}
