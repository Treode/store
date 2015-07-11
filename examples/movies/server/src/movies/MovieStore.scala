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
import com.treode.store.{Bound, Key, Slice, Store, TxClock, TxId, Window}
import com.treode.store.alt.{TableDescriptor, Transaction}

import movies.{DisplayModel => DM, PhysicalModel => PM, SearchResult => SR}

import Async.{guard, supply}

/** See README.md. */
class MovieStore (implicit random: Random, store: Store) {


  def query (rt: TxClock, query: String, movies: Boolean, actors: Boolean): Async [SearchResult] = guard {
    val tx = new Transaction (rt)
    for {
      entry <- PM.IndexEntry.prefix (tx, query.toLowerCase, movies, actors)
      _ <- tx.fetcher
          .fetch (PM.MovieTable) (entry.movies)
          .fetch (PM.ActorTable) (entry.actors)
          .async()
    } yield {
      SearchResult.lookup (tx, entry)
    }}

  def readMovie (rt: TxClock, movieId: String): Async [(TxClock, Option [DM.Movie])] = guard {
    val tx = new Transaction (rt)
    for {
      _ <- PM.Movie.fetchForDisplay (tx, movieId)
    } yield {
      val movie =
        for (root <- tx.get (PM.MovieTable) (movieId))
          yield DM.Movie (tx, movieId, root)
      (tx.vt, movie)
    }}

  def create (xid: TxId, rt: TxClock, ct: TxClock, movie: DM.Movie): Async [(String, TxClock)] = guard {
    val tx = new Transaction (rt)
    val movieId = random.nextId()
    for {
      _ <- PM.Movie.fetchForSave (tx, movieId, movie)
      _ = PM.Movie.create (tx, movieId, movie)
      wt <- tx.execute (xid, ct)
    } yield {
      (movieId, wt)
    }}

  def update (xid: TxId, rt: TxClock, ct: TxClock, movieId: String, movie: DM.Movie): Async [TxClock] = guard {
    val tx = new Transaction (rt)
    for {
      _ <- PM.Movie.fetchForSave (tx, movieId, movie)
      _ = PM.Movie.save (tx, movieId, movie)
      wt <- tx.execute (xid, ct)
    } yield {
      wt
    }}

  def readActor (rt: TxClock, actorId: String): Async [(TxClock, Option [DM.Actor])] = guard {
    val tx = new Transaction (rt)
    for {
      _ <- PM.Actor.fetchForDisplay (tx, actorId)
    } yield {
      val actor =
        for (root <- tx.get (PM.ActorTable) (actorId))
          yield DM.Actor (tx, actorId, root)
      (tx.vt, actor)
    }}

  def create (xid: TxId, rt: TxClock, ct: TxClock, actor: DM.Actor): Async [(String, TxClock)] = guard {
    val tx = new Transaction (rt)
    val actorId = random.nextId()
    for {
      _ <- PM.Actor.fetchForSave (tx, actorId, actor)
      _ = PM.Actor.create (tx, actorId, actor)
      wt <- tx.execute (xid, ct)
    } yield {
      (actorId, wt)
    }}

  def update (xid: TxId, rt: TxClock, ct: TxClock, actorId: String, actor: DM.Actor): Async [TxClock] = guard {
    val tx = new Transaction (rt)
    for {
      _ <- PM.Actor.fetchForSave (tx, actorId, actor)
      _ = PM.Actor.save (tx, actorId, actor)
      wt <- tx.execute (xid, ct)
    } yield {
      wt
    }}
}
