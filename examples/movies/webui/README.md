# The Movies Example



## Data Models

### Conceptual Model

The conceptual model is not manifest in this code anywhere. Nonetheless, it is useful to consider it, as the model would manifest in code written for a traditional database. It might appear data definition statements in SQL, or as a [Hibernate][hibernate-orm] schema in XML, or as POJO with [JPA or JDO][jpa-v-jdo] annotations.

- Table: Movies
    - Columns: (ID, Title)
    - ID is the primary key
    - Title has a secondary index
- Table: Actors
    - Columns (ID, Name)
    - ID is the primary key
    - Name has a secondary index
- Table: Roles
    - Columns: (MovieID, ActorID, role)
    - (MovieID, ActorID) is the primary key
    - A many-to-many relationship between movies and actors
    - Role is the character's name


### Display Model

The [DisplayModel][display-model] contains Scala case-classes that describe the JSON which the server provides and accepts. We use the [Jackson ScalaModule][jackson-scala-module] to transparently marshall data between JSON text and the case-classes. 

For movies, the server can GET, PUT and POST objects like this:

    {   "id": 1,
        "title": "Star Wars",
        "cast": [
            { "actorId": 1, "actor": "Mark Hamill", "role": "Luke Skywalker" },
            { "actorId": 2, "actor": "Harrison Ford", "role": "Han Solo" },
            { "actorId": 3, "actor": "Carrie Fisher", "role": "Princess Leia Organa" }
        ] }
        
For actors, the server can handle JSON objects like this:
        
     {  "id": 1,
        "name": "Mark Hamill",
        "roles":  [
            { "movieId": 1, "title": "Star Wars", "role": "Luke Skywalker" },
            { "movieId": 2, "title": "Star Wars: The Empire Strikes Back", "role": "Luke Skywalker" },
            { "movieId": 3, "title": "Star Wars: Return of the Jedi", "role": "Luke Skywalker" }
        ]}

        
### Physical Model

The [PhysicalModel][physical-model] contains Scala case-classes that describe what we store and retrieve from the database. We use the [Jackson ScalaModule][jackson-scala-module] together with the [Smile dataformat][jackson-smile] to transparently marshall data between binary JSON and the case-classes.

We maintain 5 key-value tables:

<table>
  <thead>
    <tr>
      <th>Table</th>
      <th>Key Type</th>
      <th>Value Type</th>
    </tr>
  </thead>
  <tbody>
    <tr style="font-family: monospace">
      <td>MovieTable</td>
      <td>String (MovieID)</td>
      <td>
        case class Movie (title: String)
      </td>
    </tr>
    <tr style="font-family: monospace">
      <td>CastTable</td>
      <td>String (MovieID)</td>
      <td nowrap>
        case class CastMember (actorId: String, role: String)<br>
        case class Cast (members: Seq [CastMember])
      </td>
    </tr>
    <tr style="font-family: monospace">
      <td>ActorTable</td>
      <td>String (ActorID)</td>
      <td>
        case class Actor (name: String)
      </td>
    </tr>
    <tr style="font-family: monospace">
      <td>RolesTable</td>
      <td>Long (ActorID)</td>
      <td nowrap>
        case class Role (movieId: String, role: String)<br>
        case class Roles (roles: Seq [Role])
      </td>
    </tr>
    <tr style="font-family: monospace">
      <td>Index</td>
      <td>String</td>
      <td>
        case class IndexEntry (<br>
        &nbsp;&nbsp;movies: Set [String],<br>
        &nbsp;&nbsp;actors: Set [String])
      </td>
    </tr>
  </tbody>
</table>

The information about a movie is split across two tables: the `MovieTable` and the `CastTable`. The information about and actor is split in a similar way. The `CastTable` and `RolesTable` capture the many-to-many relationship between movies and actors, and they do so in a redundant way. This is to facilitate reads, which we believe will be more frequent than writes, but it complicate writes because they must maintain the duplicated information.

The index maps movie titles and actor names to matching IDs.



## RESTful API

The server will provide the following API:

<table>
  <thead>
    <tr>
      <th>Method</th>
      <th>Path</th>
      <th>Description</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>GET
      <td>/actor?q=<i>search</i></td>
      <td>Search for actors by name prefix.</td>
    </tr>
    <tr>
      <td>POST
      <td>/actor</i></td>
      <td>Create a new actor, server chooses ID.</td>
    </tr>
    <tr>
      <td>GET
      <td>/actor/<i>id</i></td>
      <td>Get the actor by ID.</td>
    </tr>
    <tr>
      <td>PUT
      <td>/actor/<i>id</i></td>
      <td>Set or create the actor by ID.</td>
    </tr>
    <tr>
      <td>GET
      <td>/movie?q=<i>search</i></td>
      <td>Search for movies by title prefix.</td>
    </tr>
    <tr>
      <td>POST
      <td>/movie</i></td>
      <td>Create a new movie, server chooses ID.</td>
    </tr>
    <tr>
      <td>GET
      <td>/movie/<i>id</i></td>
      <td>Get the movie by ID.</td>
    </tr>
    <tr>
      <td>PUT
      <td>/movie/<i>id</i></td>
      <td>Set or create the movie by ID.</td>
    </tr>
    <tr>
      <td>GET
      <td>/search?q=<i>search</i></td>
      <td>Search for actors or movies by prefix.</td>
    </tr>
  </tbody>
</table>

### Composing JSON for GET

When we want to `GET` the JSON object for a movie, we

1. Get the information from the `MovieTable` and the `CastTable`. We now have the information for the `title` field, but we have only part of the information for the `cast` field.

1. For every `actorId` in the cast, get the information from the `ActorTable`.

1. Compose the array for the `cast` field by traversing the cast information, and using the `actorId` there to lookup the actor's name.

The code to compose the JSON object appears across three classes:

- The [PhysicalModel][physical-model] contains code to fetch the required rows from the database. Look for methods called `fetchForDisplay`.

- The [DisplayModel][display-model] contains code to translate those rows into its case-classes. Look for Scala style constructors (that is, the `apply` method in the companion object).

- The [MovieStore][movie-store] contains code that ties the two together.  Look for the methods `readMovie` and `readActor`.

We work through a similar process to compose the JSON object for an actor.

For example, suppose we are getting the movie _Star Wars_:

1.  We find that the `MovieTable` and the `CastTable` contain:

        MovieTable, 1: { "title": "Star Wars" }
        
        CastTable, 1: { "cast": [ 
            { "actorId": 1, "role": "Luke Skywalker" },
            { "actorId": 2, "role": "Han Solo" },
            { "actorId": 3, "role": "Princess Leia Organa" }
        ] }
        
2.  We find that the `ActorTable` contains:

        ActorTable, 1: { "name": "Mark Hamill" }
        
        ActorTable, 2: { "name": "Harrison Ford" }
        
        ActorTable, 3: { "name": "Carrie Fisher" }
        
3. We join these pieces of information to create:

        {   "id": 1,
            "title": "Star Wars",
            "cast": [
                { "actorId": 1, "actor": "Mark Hamill", "role": "Luke Skywalker" },
                { "actorId": 2, "actor": "Harrison Ford", "role": "Han Solo" },
                { "actorId": 3, "actor": "Carrie Fisher", "role": "Princess Leia Organa" }
            ] }

### Decomposing JSON for POST / PUT

When we want to `PUT` a JSON object for a movie, we must be sure to update the role information that appears both in the `CastTable` and the `RolesTable`.

1. Get the old information from the `MovieTable`, the `CastTable` and the `Index`.

1. Get the information from the `RolesTable` for both the actors in the old cast and the actors in the new cast.


1. Update the information in the `MovieTable`, the `CastTable`, the `RolesTable` and the `Index`.

    - Some fields in the JSON object may be missing, and we fill those in from the prior values.

    - We can update the one row in the `CastTable` directly.
    
    - However we must merge information into multiple rows of the `RolesTable`.
    
    - We may need to move remove the ID from one `IndexEntry` and add them to another.
    
The code to decompose an object and make an update appears across a few places.
    
- The [PhysicalModel][physical-model] contains code to fetch the required rows from the database. Look for methods called `fetchForSave`.

- The [PhysicalModel][physical-model] contains code to compare information from the [DisplayModel][display-model] with that in the database, merge the changes if any, and save only the modified rows. This process starts with the methods `Movie.create` and `Movie.save` for movies, and it is similar for actors.

- The [MovieStore][movie-store] contains code that ties the two together.  Look for the methods `create` and `update`.

For example, suppose the user had created the movie _Star Wars_ with some typos (probably caused by of autocorrect):

    {   "id": 1,
        "title": "Star Wars",
        "cast": [
            { "actorId": 1, "actor": "Mark Hamill", "role": "Luke Skywriter" },
            { "actorId": 2, "actor": "Harrison Ford", "role": "Han Solo" },
            { "actorId": 4, "actor": "Carry Fisher", "role": "Princess Leia Organa"}
        ] }
        
Now the user is going to update _Star Wars_ with the two corrections:

    {   "id": 1,
        "title": "Star Wars",
        "cast": [
            { "actorId": 1, "actor": "Mark Hamill", "role": "Luke Skywalker" },
            { "actorId": 2, "actor": "Harrison Ford", "role": "Han Solo" },
            { "actorId": 3, "actor": "Carrie Fisher", "role": "Princess Leia Organa" }
        ] }
            
1.  We find that the `MovieTable` and the `CastTable` contain:

        MovieTable, 1: { "title": "Star Wars" }
        
        CastTable, 1: { "cast": [ 
            { "actorId": 1, "role": "Luke Skywriter" },
            { "actorId": 2, "role": "Han Solo" }
            { "actorId": 4, "role": "Princess Leia Organa" }
        ] }
        
The `CastTable` reflects both mistakes in the one row. It has the misspelling of _Luke Skywalker_, and it has the wrong `actorId` for _Princess Leia Organa_.
        
1.  For the actors in the old cast (1, 2, 4) and the actors in the new cast (1, 2, 3) find the the `RolesTable` contains:

        RolesTable, 1: { "roles": [ 
            { "movieId": 1, "role": "Luke Skywriter" },
            { "movieId": 2, "role": "Luke Skywalker" }
            { "movieId": 3, "role": "Luke Skywalker" }            
        ] }
        
        RolesTable, 2: { "roles": [ 
            { "movieId": 1, "role": "Han Solo" },
            { "movieId": 2, "role": "Han Solo" },
            { "movieId": 3, "role": "Han Solo" }
        ] }
        
        RolesTable, 3: { "roles": [ 
            { "movieId": 2, "role": "Princess Leia Organa" },
            { "movieId": 3, "role": "Princess Leia Organa" }
        ] }
        
        RolesTable, 4: { "roles": [ 
            { "movieId": 1, "role": "Princess Leia Organa" }
        ] }
        
The `RolesTable` reflects both mistakes, each in a different row.  This misspelling of _Luke Skywalker_ appears in row 1, and the incorrect actor for _Princess Leia Organa_ appears in both rows 3 and 4.

1.  We must update the `CastTable` for the movie, and we must update the `RolesTable` for two actors. When we update the roles table, we change only the one role that pertains to this movie.

        CastTable, 1: { "cast": [ 
            { "actorId": 1, "role": "Luke Skywalker" },
            { "actorId": 2, "role": "Han Solo" }
            { "actorId": 3, "role": "Princess Leia Organa" }
        ] }
        
        RolesTable, 1: { "roles": [ 
            { "movieId": 1, "role": "Luke Skywalker" },
            { "movieId": 2, "role": "Luke Skywalker" }
            { "movieId": 3, "role": "Luke Skywalker" }
        ] }
        
        RolesTable, 3: { "roles": [ 
            { "movieId": 1, "role": "Princess Leia Organa" },
            { "movieId": 2, "role": "Princess Leia Organa" },
            { "movieId": 3, "role": "Princess Leia Organa" }
        ] }
        
        RolesTable, 4: { "roles": [ ] }



## Deployment

The server for the movie repository is packaged into a single JAR, however the full application including the Web UI requires a more complex deployment, since it needs a webserver to server the HTML, CSS and JS. To avoid triggering Cross-Origin checks, we also configure that webserver to proxy requests for the repository.

To begin, you'll want a machine running Linux. Grab a server from a provider like [DigitalOcean][digital-ocean], or setup a virtual machine using a package like [VirtualBox][virtual-box]. Give yourself at least 20MB of disk space. The next instructions assume your using Debian packages, so choose an OS like Ubuntu.

    iptables -A INPUT -i lo -j ACCEPT
    iptables -A INPUT -m conntrack --ctstate ESTABLISHED,RELATED -j ACCEPT
    iptables -A INPUT -p tcp --dport ssh -j ACCEPT
    iptables -A INPUT -p tcp --dport 80 -j ACCEPT
    iptables -A INPUT -p tcp --dport 443 -j ACCEPT
    iptables -A INPUT -p icmp -m state --state NEW,ESTABLISHED,RELATED -j ACCEPT
    iptables -A INPUT -j DROP

    sudo apt-get update
    sudo apt-get install iptables-persistent nginx-full openjdk-7-jdk
    
Build the UI and copy it to the host machine.

    cd examples/movies/webui
    npm install
    grunt dev
    scp -r dev <yourhost>:.

Build the server and copy it to the host machine.

    cd examples/movies/server
    sbt assembly
    scp target/scala-2.10/server.jar <yourhost>:.
    
Run that server

    java -jar server.jar -solo -serve store.db
    
You may need to adjust that command depending on your setup with disks and peers. See the [tutorial][tutorial] for help.

Copy `config/nginx` from the git repository to `/etc/nginx/sites-available/movies` on your machine.
Update the `path-to-root` in that file so that it points to where you place the UI, and then

    rm /etc/nginx/sites-enabled/default
    ln -s /etc/nginx/sites-available/movies /etc/nginx/sites-enabled/movies
    service nginx restart

You should be good to go. Check what NGINX is serving on that host.




[digital-ocean]: //www.digitalocean.com "DigitalOcean cloud hosting"

[display-model]: //github.com/Treode/store/blob/examples/movies/src/main/scala/movies/DisplayModel.scala "DisplayModel"

[hibernate-orm]: http://hibernate.org/orm/ "Hibernate ORM"

[jackson-scala-module]: https://github.com/FasterXML/jackson-module-scala "FasterXML/jackson-module-scala on GitHub"

[jackson-smile]: https://github.com/FasterXML/jackson-dataformat-smile "FasterXML/jackson-dataformat-smile on GitHub"

[jpa-v-jdo]: https://db.apache.org/jdo/jdo_v_jpa.html "JDO .v. JPA"

[movie-store]: //github.com/Treode/store/blob/examples/movies/src/main/scala/movies/MovieStore.scala "MovieStore"

[physical-model]: //github.com/Treode/store/blob/examples/movies/src/main/scala/movies/PhysicalModel.scala "PhysicalModel"

[tutorial]: //treode.github.io/store "TreodeDB Tutorial"

[virtual-box]: //www.virtualbox.org "Oracle VM VirtualBox"