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
import com.treode.finatra.BadRequestException
import com.treode.store.alt.{Froster, TableDescriptor, Transaction}

import movies.{DisplayModel => DM}

/** See README.md. */
private object PhysicalModel {

  private def unique [A] (s1: Seq [A], s2: Seq [A]): Seq [A] =
    Set (s1 ++ s2: _*) .toSeq

  case class Movie (title: String) {

    private def merge (that: DM.Movie): Movie =
      Movie (title = that.title orDefault (title))

    private def validate() {
      title orBadRequest ("Movie must have a title.")
    }

    private def create (tx: Transaction, movieId: Long) {
      validate()
      tx.create (MovieTable) (movieId, this)
      tx.create (MovieTitleIndex) (title, movieId)
    }

    private def save (tx: Transaction, movieId: Long, that: Movie) {
      that.validate()
      if (this != that)
        tx.update (MovieTable) (movieId, that)
      if (this.title != that.title) {
        if (this.title != null)
          tx.delete (MovieTitleIndex) (this.title)
        tx.create (MovieTitleIndex) (that.title, movieId)
      }}

    private def save (tx: Transaction, movieId: Long, that: DM.Movie) {
      save (tx, movieId, merge (that))
      if (that.cast != null)
        Cast.save (tx, movieId, that.cast)
    }}

  object Movie {

    private val empty: Movie =
      new Movie (null)

    private def apply (movie: DM.Movie): Movie =
      new Movie (movie.title)

    /** Prefetch all data that's needed to compose a JSON object for the movie. */
    def fetchForDisplay (tx: Transaction, movieId: Long): Async [Unit] =
      for {
        _ <- tx.fetch ((MovieTable, movieId), (CastTable, movieId))
        cast = tx.get (CastTable) (movieId) .getOrElse (Cast.empty)
        _ <- tx.fetch (ActorTable) (cast.actorIds: _*)
      } yield ()

    /** Prefetch all data that's needed to decompose a JSON object and update the movie. */
    def fetchForSave (tx: Transaction, movieId: Long, actorIds: Seq [Long]): Async [Unit] =
      for {
        _ <- tx.fetch ((MovieTable, movieId), (CastTable, movieId))
        cast = tx.get (CastTable) (movieId) .getOrElse (Cast.empty)
        _ <- tx.fetch (RolesTable) (unique (cast.actorIds, actorIds): _*)
      } yield ()

    /** Create a new movie in the database. This also makes the implied changes to the actors'
      * roles.
      */
    def create (tx: Transaction, movieId: Long, movie: DM.Movie) {
      Movie (movie) .create (tx, movieId)
      Cast.convert (movie.cast.orDefault (Seq.empty)) .create (tx, movieId)
    }

    /** Save an update from JSON to the database. This walks the DisplayModel and updates only
      * those database rows that need changes. If there are changes in the cast, of course this
      * changes the movie's cast in the database, and it also makes the implied changes to the
      * actors' roles.
      */
    def save (tx: Transaction, movieId: Long, movie: DM.Movie) {
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
      if (actorIds exists (_ == 0))
        throw new BadRequestException ("All cast members must have an actorId.")
      if (actorIds.toSet.size < actorIds.size)
        throw new BadRequestException ("An actor may have only one role in a movie.")
    }

    def remove (tx: Transaction, movieId: Long, actorId: Long) {
      if (members forall (_.actorId != actorId))
        return
      val newMembers = members filter (_.actorId != actorId)
      tx.update (CastTable) (movieId, Cast (newMembers))
    }

    def save (tx: Transaction, actorId: Long, that: Role) {
      val newMembers = CastMember (actorId, that.role) +: members.filter (_.actorId != actorId)
      tx.update (CastTable) (that.movieId, Cast (newMembers))
    }

    def create (tx: Transaction, movieId: Long) {
      validate()
      tx.create (CastTable) (movieId, this)
      for (m <- members) {
        val roles = tx
            .get (RolesTable) (m.actorId)
            .getOrBadRequest (s"No such actor ${m.actorId}.")
        roles.save (tx, movieId, m)
      }}

    private def save (tx: Transaction, movieId: Long, that: Cast) {

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

    def save (tx: Transaction, movieId: Long, that: Seq [DM.CastMember]): Unit =
      save (tx, movieId, merge (Cast.convert (that)))
  }

  object Cast {

    val empty = Cast (Seq.empty)

    def convert (members: Seq [DM.CastMember]): Cast =
      new Cast (members map (CastMember (_)))

    def save (tx: Transaction, movieId: Long, cast: Seq [DM.CastMember]): Unit =
      tx.get (CastTable) (movieId) match {
        case Some (_cast) => _cast.save (tx, movieId, cast)
        case None => convert (cast) .create (tx, movieId)
      }}

  case class CastMember (actorId: Long, role: String) {

    def merge (that: CastMember): CastMember =
      CastMember (
          actorId = actorId,
          role = that.role orDefault (role))
  }

  object CastMember {

    def apply (member: DM.CastMember): CastMember =
      new CastMember (member.actorId, member.role)

    def apply (actorId: Long, role: DM.Role): CastMember =
      new CastMember (actorId, role.role)
  }

  case class Actor (name: String) {

    private def merge (that: DM.Actor): Actor =
      Actor (name = that.name orDefault (name))

    private def validate() {
      name orBadRequest ("Actor must have a name.")
    }

    private def create (tx: Transaction, actorId: Long) {
      validate()
      tx.create (ActorTable) (actorId, this)
      tx.create (ActorNameIndex) (name, actorId)
    }

    private def save (tx: Transaction, actorId: Long, that: Actor) {
      that.validate()
      if (this != that)
        tx.update (ActorTable) (actorId, that)
      if (this.name != that.name) {
        if (this.name != null)
          tx.delete (ActorNameIndex) (this.name)
        tx.create (ActorNameIndex) (that.name, actorId)
      }}

    private def save (tx: Transaction, actorId: Long, that: DM.Actor) {
      save (tx, actorId, merge (that))
      if (that.roles != null)
        Roles.save (tx, actorId, that.roles)
    }}

  object Actor {

    private val empty = new Actor (null)

    private def apply (actor: DM.Actor): Actor =
      new Actor (actor.name)

    /** Prefetch all data that's needed to compose a JSON object for the actor. */
    def fetchForDisplay (tx: Transaction, actorId: Long): Async [Unit] =
      for {
        _ <- tx.fetch ((ActorTable, actorId), (RolesTable, actorId))
        roles = tx.get (RolesTable) (actorId) .getOrElse (Roles.empty)
        _ <- tx.fetch (MovieTable) (roles.movieIds: _*)
      } yield ()

    /** Prefetch all data that's needed to decompose a JSON object and update the actor. */
    def fetchForSave (tx: Transaction, actorId: Long, movieIds: Seq [Long]): Async [Unit] =
      for {
        _ <- tx.fetch ((ActorTable, actorId), (RolesTable, actorId))
        roles = tx.get (RolesTable) (actorId) .getOrElse (Roles.empty)
        _ <- tx.fetch (CastTable) (unique (roles.movieIds, movieIds): _*)
      } yield ()

    /** Create a new actor in the database. This also makes the implied changes to the movies'
      * cast.
      */
    def create (tx: Transaction, actorId: Long, actor: DM.Actor) {
      Actor (actor) .create (tx, actorId)
      Roles.convert (actor.roles.orDefault (Seq.empty)) .create (tx, actorId)
    }

    /** Save an update from JSON to the database. This walks the DisplayModel and updates only
      * those database rows that need changes. If there are changes in the roles, of course this
      * changes the actor's roles in the database, and it also makes the implied changes to the
      * movies' cast.
      */
    def save (tx: Transaction, actorId: Long, actor: DM.Actor) {
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
      if (movieIds exists (_ == 0))
        throw new BadRequestException ("All roles must have a movieId.")
      if (movieIds.toSet.size < movieIds.size)
        throw new BadRequestException ("An actor may have only one role in a movie.")
    }

    def remove (tx: Transaction, actorId: Long, movieId: Long) {
      if (roles forall (_.movieId != movieId))
        return
      val newRoles = roles filter (_.movieId != movieId)
      tx.update (RolesTable) (actorId, Roles (newRoles))
    }

    def save (tx: Transaction, movieId: Long, that: CastMember) {
      val newRoles = Role (movieId, that.role) +: roles.filter (_.movieId != movieId)
      tx.update (RolesTable) (that.actorId, Roles (newRoles))
    }

    def create (tx: Transaction, actorId: Long) {
      validate()
      tx.create (RolesTable) (actorId, this)
      for (r <- roles) {
        val cast = tx
            .get (CastTable) (r.movieId)
            .getOrBadRequest (s"No such movie ${r.movieId}.")
        cast.save (tx, actorId, r)
      }}

    private def save (tx: Transaction, actorId: Long, that: Roles) {

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

    def save (tx: Transaction, movieId: Long, that: Seq [DM.Role]): Unit =
      save (tx, movieId, merge (Roles.convert (that)))
  }

  object Roles {

    val empty = Roles (Seq.empty)

    def convert (roles: Seq [DM.Role]): Roles =
      new Roles (roles map (Role.apply (_)))

    def save (tx: Transaction, actorId: Long, roles: Seq [DM.Role]): Unit =
      tx.get (RolesTable) (actorId) match {
        case Some (_roles) => _roles.save (tx, actorId, roles)
        case None => convert (roles) .create (tx, actorId)
      }}

  case class Role (movieId: Long, role: String) {

    def merge (that: Role): Role =
      Role (
          movieId = movieId,
          role = that.role orDefault (role))
  }

  object Role {

    def apply (role: DM.Role): Role =
      new Role (role.movieId, role.role)

    def apply  (movieId: Long, member: DM.CastMember): Role =
      new Role (movieId, member.role)
  }

  val MovieTable =
    TableDescriptor (0xA57FDF4417D46CBCL, Froster.long, Froster.bson [Movie])

  val MovieTitleIndex =
    TableDescriptor (0x5BADD72FF250EFECL, Froster.string, Froster.long)

  val CastTable =
    TableDescriptor (0x98343A201B58A827L, Froster.long, Froster.bson [Cast])

  val ActorTable =
    TableDescriptor (0xDB67009587B57F0DL, Froster.long, Froster.bson [Actor])

  val ActorNameIndex =
    TableDescriptor (0x8BB6A8029399BADEL, Froster.string, Froster.long)

  val RolesTable =
    TableDescriptor (0x57F7EA70C4CD4613L, Froster.long, Froster.bson [Roles])
}
