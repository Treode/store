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

import com.fasterxml.jackson.annotation.JsonIgnore
import com.treode.finatra.BadRequestException
import com.treode.store.alt.Transaction
import org.joda.time.DateTime

import movies.{PhysicalModel => PM, SearchResult => SR}

case class SearchResult (movies: Seq [SR.Movie], actors: Seq [SR.Actor])

/** See README.md. */
object SearchResult {

  case class Movie (id: String, title: String, released: DateTime)

  object Movie {

    def lookup (tx: Transaction, movieId: String): Option [Movie] =
      for (movie <- tx.get (PM.MovieTable) (movieId))
        yield Movie (movieId, movie.title, movie.released)

    def lookup (tx: Transaction, movieIds: Set [String]): Seq [Movie] =
      movieIds.toSeq.map (lookup (tx, _)) .flatten
  }

  case class Actor (id: String, name: String, born: DateTime)

  object Actor {

    def lookup (tx: Transaction, actorId: String): Option [Actor] =
      for (actor <- tx.get (PM.ActorTable) (actorId))
        yield Actor (actorId, actor.name, actor.born)

    def lookup (tx: Transaction, actorIds: Set [String]): Seq [Actor] =
      actorIds.toSeq.map (lookup (tx, _)) .flatten
  }

  def lookup (tx: Transaction, entry: PM.IndexEntry): SearchResult =
    SearchResult (Movie.lookup (tx, entry.movies), Actor.lookup (tx, entry.actors))
}
