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
import com.treode.async.{Async, BatchIterator, Scheduler}
import com.treode.async.implicits._
import com.treode.pickle.Picklers
import com.treode.store.{Batch, Bound, Store, TxClock, Window}
import com.treode.store.alt.{Froster, TableDescriptor, Transaction}
import com.treode.twitter.finagle.http.BadRequestException
import org.joda.time.DateTime

import Async.supply
import movies.{AnalyticsModel => AM, DisplayModel => DM}

/** See README.md. */
private object PhysicalModel {

  case class Movie (title: String, released: DateTime) {

    @JsonIgnore
    lazy val titleLowerCase =
      if (title == null) null else title.toLowerCase

    private def merge (that: DM.Movie): Movie =
      Movie (
        title = that.title orDefault (title),
        released = that.released orDefault (released))

    private def validate() {
      title orBadRequest ("Movie must have a title.")
    }

    private def addToTitleIndex (tx: Transaction, movieId: String, title: String): Unit =
      if (title != null) {
        val entry = tx.get (Index) (title) getOrElse (IndexEntry.empty)
        entry.copy (movies = entry.movies + movieId) .save (tx, title)
      }

    private def removeFromTitleIndex (tx: Transaction, movieId: String, title: String): Unit =
      if (title != null) {
        val entry = tx.get (Index) (title) getOrElse (IndexEntry.empty)
        entry.copy (movies = entry.movies - movieId) .save (tx, title)
      }

    private def create (tx: Transaction, movieId: String) {
      validate()
      tx.create (MovieTable) (movieId, this)
      addToTitleIndex (tx, movieId, titleLowerCase)
    }

    private def save (tx: Transaction, movieId: String, that: Movie) {
      that.validate()
      if (this != that)
        tx.update (MovieTable) (movieId, that)
      if (this.titleLowerCase != that.titleLowerCase) {
        removeFromTitleIndex (tx, movieId, this.titleLowerCase)
        addToTitleIndex (tx, movieId, that.titleLowerCase)
      }}

    private def save (tx: Transaction, movieId: String, that: DM.Movie) {
      save (tx, movieId, merge (that))
      if (that.cast != null)
        Cast.save (tx, movieId, that.cast)
      else if (tx.get (CastTable) (movieId) .isEmpty)
        Cast.save (tx, movieId, Seq.empty)
    }}

  object Movie {

    private val empty: Movie =
      new Movie (null, null)

    private def apply (movie: DM.Movie): Movie =
      new Movie (movie.title, movie.released)

    /** Prefetch all data that's needed to compose a JSON object for the movie. */
    def fetchForDisplay (tx: Transaction, movieId: String): Async [Unit] =
      for {
        _ <- tx.fetcher
            .fetch (MovieTable) (movieId)
            .fetch (CastTable) (movieId)
            .async()
        cast = tx.get (CastTable) (movieId) .getOrElse (Cast.empty)
        _ <- tx.fetch (ActorTable) (cast.actorIds: _*)
      } yield ()

    /** Prefetch all data that's needed to decompose the JSON object and create or update the
      * movie.
      */
    def fetchForSave (tx: Transaction, movieId: String, update: DM.Movie): Async [Unit] =
      for {
        _ <- tx.fetcher
            .fetch (MovieTable) (movieId)
            .fetch (CastTable) (movieId)
            .fetch (Index) .when (update.title != null) (update.titleLowerCase)
            .async()
        movie = tx.get (MovieTable) (movieId) .getOrElse (Movie.empty)
        cast = tx.get (CastTable) (movieId) .getOrElse (Cast.empty)
        _ <- tx.fetcher
            .fetch (RolesTable) (cast.actorIds)
            .fetch (RolesTable) (update.actorIds)
            .fetch (Index) .when (movie.title != null) (movie.titleLowerCase)
            .async()
      } yield ()

    def list (window: Window) (implicit store: Store): BatchIterator [AM.Movie] =
      for {
        cell <- MovieTable.scan (
            window = window,
            batch = Batch (4000, 1 << 18))
        if cell.value.isDefined
      } yield {
        val movie = cell.value.get
        new AM.Movie (cell.key, movie.title, movie.released)
      }

    /** Create a new movie in the database. This also makes the implied changes to the actors'
      * roles.
      */
    def create (tx: Transaction, movieId: String, movie: DM.Movie) {
      Movie (movie) .create (tx, movieId)
      Cast.convert (movie.cast.orDefault (Seq.empty)) .create (tx, movieId)
    }

    /** Save an update from JSON to the database. This walks the DisplayModel and updates only
      * those database rows that need changes. If there are changes in the cast, of course this
      * changes the movie's cast in the database, and it also makes the implied changes to the
      * actors' roles.
      */
    def save (tx: Transaction, movieId: String, movie: DM.Movie) {
      tx.get (MovieTable) (movieId) match {
        case Some (m) => m.save (tx, movieId, movie)
        case None => empty.save (tx, movieId, movie)
      }}}

  case class Cast (members: Seq [CastMember]) {

    def actorIds = members.map (_.actorId)

    def byActorId = members.map (m => (m.actorId, m)) .toMap

    def merge (that: Cast): Cast =
      Cast (
          members =
            for (member <- that.members)
              yield byActorId.get (member.actorId) match {
                case Some (m) => m.merge (member)
                case None => member
              }
      )

    def validate() {
      if (actorIds exists (_ == null))
        throw new BadRequestException ("All cast members must have an actorId.")
      if (actorIds.toSet.size < actorIds.size)
        throw new BadRequestException ("An actor may have only one role in a movie.")
    }

    def remove (tx: Transaction, movieId: String, actorId: String) {
      if (members forall (_.actorId != actorId))
        return
      val newMembers = members filter (_.actorId != actorId)
      tx.update (CastTable) (movieId, Cast (newMembers))
    }

    def save (tx: Transaction, actorId: String, that: Role) {
      val newMembers = CastMember (actorId, that.role) +: members.filter (_.actorId != actorId)
      tx.update (CastTable) (that.movieId, Cast (newMembers))
    }

    def create (tx: Transaction, movieId: String) {
      validate()
      tx.create (CastTable) (movieId, this)
      for (m <- members) {
        val roles = tx
            .get (RolesTable) (m.actorId)
            .getOrBadRequest (s"No such actor ${m.actorId}.")
        roles.save (tx, movieId, m)
      }}

    private def save (tx: Transaction, movieId: String, that: Cast) {

      validate()

      if (byActorId == that.byActorId)
        return

      tx.update (CastTable) (movieId, that)

      for {
        member <- members
        if !(that.byActorId contains member.actorId)
        roles <- tx.get (RolesTable) (member.actorId)
      } roles.remove (tx, member.actorId, movieId)

      for {
        newm <- that.members
        oldm = byActorId.get (newm.actorId)
        if oldm.isEmpty || oldm.get != newm
      } {
        val roles = tx
            .get (RolesTable) (newm.actorId)
            .getOrBadRequest (s"No such actor ${newm.actorId}.")
        roles.save (tx, movieId, newm)
      }}

    def save (tx: Transaction, movieId: String, that: Seq [DM.CastMember]): Unit =
      save (tx, movieId, merge (Cast.convert (that)))
  }

  object Cast {

    val empty = Cast (Seq.empty)

    def convert (members: Seq [DM.CastMember]): Cast =
      new Cast (members map (CastMember (_)))

    def save (tx: Transaction, movieId: String, cast: Seq [DM.CastMember]): Unit =
      tx.get (CastTable) (movieId) match {
        case Some (_cast) => _cast.save (tx, movieId, cast)
        case None => convert (cast) .create (tx, movieId)
      }}

  case class CastMember (actorId: String, role: String) {

    def merge (that: CastMember): CastMember =
      CastMember (
          actorId = actorId,
          role = that.role orDefault (role))
  }

  object CastMember {

    def apply (member: DM.CastMember): CastMember =
      new CastMember (member.actorId, member.role)

    def apply (actorId: String, role: DM.Role): CastMember =
      new CastMember (actorId, role.role)
  }

  case class Actor (name: String, born: DateTime) {

    @JsonIgnore
    lazy val nameLowerCase =
      if (name == null) null else name.toLowerCase

    private def merge (that: DM.Actor): Actor =
      Actor (
        name = that.name orDefault (name),
        born = that.born orDefault (born))

    private def validate() {
      name orBadRequest ("Actor must have a name.")
    }

    private def addToNameIndex (tx: Transaction, actorId: String, name: String): Unit =
      if (name != null) {
        val entry = tx.get (Index) (name) getOrElse (IndexEntry.empty)
        entry.copy (actors = entry.actors + actorId) .save (tx, name)
      }

    private def removeFromNameIndex (tx: Transaction, actorId: String, name: String): Unit =
      if (name != null) {
        val entry = tx.get (Index) (name) getOrElse (IndexEntry.empty)
        entry.copy (actors = entry.actors - actorId) .save (tx, name)
      }

    private def create (tx: Transaction, actorId: String) {
      validate()
      tx.create (ActorTable) (actorId, this)
      addToNameIndex (tx, actorId, nameLowerCase)
    }

    private def save (tx: Transaction, actorId: String, that: Actor) {
      that.validate()
      if (this != that)
        tx.update (ActorTable) (actorId, that)
      if (this.name != that.name) {
        removeFromNameIndex (tx, actorId, this.nameLowerCase)
        addToNameIndex (tx, actorId, that.nameLowerCase)
      }}

    private def save (tx: Transaction, actorId: String, that: DM.Actor) {
      save (tx, actorId, merge (that))
      if (that.roles != null)
        Roles.save (tx, actorId, that.roles)
      else if (tx.get (RolesTable) (actorId) .isEmpty)
        Roles.save (tx, actorId, Seq.empty)
    }}

  object Actor {

    private val empty = new Actor (null, null)

    private def apply (actor: DM.Actor): Actor =
      new Actor (actor.name, actor.born)

    /** Prefetch all data that's needed to compose a JSON object for the actor. */
    def fetchForDisplay (tx: Transaction, actorId: String): Async [Unit] =
      for {
        _ <- tx.fetcher
            .fetch (ActorTable) (actorId)
            .fetch (RolesTable) (actorId)
            .async()
        roles = tx.get (RolesTable) (actorId) .getOrElse (Roles.empty)
        _ <- tx.fetch (MovieTable) (roles.movieIds: _*)
      } yield ()

    /** Prefetch all data that's needed to decompose a JSON object and create or update the
      * actor. */
    def fetchForSave (tx: Transaction, actorId: String, update: DM.Actor): Async [Unit] =
      for {
        _ <- tx.fetcher
            .fetch (ActorTable) (actorId)
            .fetch (RolesTable) (actorId)
            .fetch (Index) .when (update.name != null) (update.nameLowerCase)
            .async()
        actor = tx.get (ActorTable) (actorId) .getOrElse (Actor.empty)
        roles = tx.get (RolesTable) (actorId) .getOrElse (Roles.empty)
        _ <- tx.fetcher
            .fetch (CastTable) (roles.movieIds)
            .fetch (CastTable) (update.movieIds)
            .fetch (Index) .when (actor.name != null) (actor.nameLowerCase)
            .async()
      } yield ()

    def list (window: Window) (implicit store: Store): BatchIterator [AM.Actor] =
      for {
        cell <- ActorTable.scan (
            window = window,
            batch = Batch (4000, 1 << 18))
        if cell.value.isDefined
      } yield {
        val actor = cell.value.get
        new AM.Actor (cell.key, actor.name, actor.born)
      }

    /** Create a new actor in the database. This also makes the implied changes to the movies'
      * cast.
      */
    def create (tx: Transaction, actorId: String, actor: DM.Actor) {
      Actor (actor) .create (tx, actorId)
      Roles.convert (actor.roles.orDefault (Seq.empty)) .create (tx, actorId)
    }

    /** Save an update from JSON to the database. This walks the DisplayModel and updates only
      * those database rows that need changes. If there are changes in the roles, of course this
      * changes the actor's roles in the database, and it also makes the implied changes to the
      * movies' cast.
      */
    def save (tx: Transaction, actorId: String, actor: DM.Actor) {
      tx.get (ActorTable) (actorId) match {
        case Some (a) => a.save (tx, actorId, actor)
        case None => empty.save (tx, actorId, actor)
      }}}

  case class Roles (roles: Seq [Role]) {

    def movieIds = roles.map (_.movieId)

    def byMovieId = roles.map (m => (m.movieId, m)) .toMap

    def merge (that: Roles): Roles =
      Roles (
          roles =
            for (role <- that.roles)
              yield byMovieId.get (role.movieId) match {
                case Some (r) => r.merge (role)
                case None => role
              }
      )

    def validate() {
      if (movieIds exists (_ == null))
        throw new BadRequestException ("All roles must have a movieId.")
      if (movieIds.toSet.size < movieIds.size)
        throw new BadRequestException ("An actor may have only one role in a movie.")
    }

    def remove (tx: Transaction, actorId: String, movieId: String) {
      if (roles forall (_.movieId != movieId))
        return
      val newRoles = roles filter (_.movieId != movieId)
      tx.update (RolesTable) (actorId, Roles (newRoles))
    }

    def save (tx: Transaction, movieId: String, that: CastMember) {
      val newRoles = Role (movieId, that.role) +: roles.filter (_.movieId != movieId)
      tx.update (RolesTable) (that.actorId, Roles (newRoles))
    }

    def create (tx: Transaction, actorId: String) {
      validate()
      tx.create (RolesTable) (actorId, this)
      for (r <- roles) {
        val cast = tx
            .get (CastTable) (r.movieId)
            .getOrBadRequest (s"No such movie ${r.movieId}.")
        cast.save (tx, actorId, r)
      }}

    private def save (tx: Transaction, actorId: String, that: Roles) {

      validate()

      if (byMovieId == that.byMovieId)
        return

      tx.update (RolesTable) (actorId, that)

      for {
        role <- roles
        if !(that.byMovieId contains role.movieId)
        cast <- tx.get (CastTable) (role.movieId)
      } cast.remove (tx, role.movieId, actorId)

      for {
        newr <- that.roles
        oldr = byMovieId.get (newr.movieId)
        if oldr.isEmpty || oldr.get != newr
      } {
        val cast = tx
            .get (CastTable) (newr.movieId)
            .getOrBadRequest (s"No such movie ${newr.movieId}")
        cast.save (tx, actorId, newr)
      }}

    def save (tx: Transaction, movieId: String, that: Seq [DM.Role]): Unit =
      save (tx, movieId, merge (Roles.convert (that)))
  }

  object Roles {

    val empty = Roles (Seq.empty)

    def list (window: Window) (implicit store: Store): BatchIterator [AM.Role] =
      for {
        cell <- RolesTable.scan (
            window = window,
            batch = Batch (4000, 1 << 18))
        if cell.value.isDefined
        val actorId = cell.key
        role <- cell.value.get.roles
      } yield {
        AM.Role (actorId, role.movieId, role.role)
      }

    def convert (roles: Seq [DM.Role]): Roles =
      new Roles (roles map (Role.apply (_)))

    def save (tx: Transaction, actorId: String, roles: Seq [DM.Role]): Unit =
      tx.get (RolesTable) (actorId) match {
        case Some (_roles) => _roles.save (tx, actorId, roles)
        case None => convert (roles) .create (tx, actorId)
      }}

  case class Role (movieId: String, role: String) {

    def merge (that: Role): Role =
      Role (
          movieId = movieId,
          role = that.role orDefault (role))
  }

  object Role {

    def apply (role: DM.Role): Role =
      new Role (role.movieId, role.role)

    def apply  (movieId: String, member: DM.CastMember): Role =
      new Role (movieId, member.role)
  }

  case class IndexEntry (movies: Set [String], actors: Set [String]) {

    def isEmpty = movies.isEmpty && actors.isEmpty

    def save (tx: Transaction, key: String): Unit =
      if (isEmpty)
        tx.delete (Index) (key)
      else
        tx.update (Index) (key, this)

    def ++ (that: IndexEntry): IndexEntry =
      new IndexEntry (movies ++ that.movies, actors ++ that.actors)
  }

  object IndexEntry {

    val empty = IndexEntry (Set.empty, Set.empty)

    val pickler = {
      import Picklers._
      wrap (tuple (set (string), set (string)))
      .build (v => new IndexEntry (v._1, v._2))
      .inspect (v => (v.movies, v.actors))
    }

    val froster = Froster (pickler)

    /** Get the entries from the index. Choose entries such that
      * - The prefix of the entry's key matches the given key
      * - The entry counts only if it has movies or actors as selected
      * - Listing at most 10 entries
      */
    def prefix (
        tx: Transaction,
        key: String,
        movies: Boolean,
        actors: Boolean
    ) (implicit
        store: Store
    ): Async [IndexEntry] = {
      var count = 10
      Index.from (
          start = Bound ((key, TxClock.MaxValue), true),
          window = tx.latest,
          batch = Batch (count, 1 << 16))
      .filter (_.value.isDefined)
      .filter { cell =>
        val entry = cell.value.get
        (movies && !entry.movies.isEmpty) || (actors && !entry.actors.isEmpty)
      }
      .toSeqWhile { cell =>
        count -= 1
        count > 0 && cell.key.startsWith (key)
      }
      .map { case (cells, next) =>
        cells
          .map { cell =>
            IndexEntry (
              if (movies) cell.value.get.movies else Set.empty,
              if (actors) cell.value.get.actors else Set.empty)
          }
          .fold (empty) (_ ++ _)
      }}}

  val MovieTable =
    TableDescriptor (0xA57FDF4417D46CBCL, Froster.string, Froster.bson [Movie])

  val CastTable =
    TableDescriptor (0x98343A201B58A827L, Froster.string, Froster.bson [Cast])

  val ActorTable =
    TableDescriptor (0xDB67009587B57F0DL, Froster.string, Froster.bson [Actor])

  val RolesTable =
    TableDescriptor (0x57F7EA70C4CD4613L, Froster.string, Froster.bson [Roles])

  val Index =
    TableDescriptor (0x5368785B1A81BF4EL, Froster.string, IndexEntry.froster)
}
