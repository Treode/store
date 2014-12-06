TreodeDB
========

TreodeDB is a distributed database that provides multirow atomic writes, and it's designed for RESTful services. It is packaged as a library for building storage servers.

TreodeDB

- is a key-value store
- offers replication for fault tolerance
- offers sharding for scalability
- supports transactions to provide consistency
- tracks versioned data to support cacheing
- is an embedded database

TreodeDB is a library for building a peer in a storage network.  It offers a Scala API for read, write and scan; you add layers for data-modeling, client protocol and security.

![Architecture][arch]


## Development Setup

The libraries for TreodeDB are available Scala 2.10 and 2.11.

Notice the uncommon notation for the dependency; this gives you stubs for testing.


```
resolvers += Resolver.url (
    "treode-oss",
    new URL ("https://oss.treode.com/ivy")) (Resolver.ivyStylePatterns)

libraryDependencies += 
    "com.treode" %% "store" % "0.1.0" % "compile;test->stubs"
```


## Documentation

- [User Docs][user-docs]
- [API Docs][api-docs]
- [Presentation at Box, Dec 8 2014][presentation-2014-12-08]


## Getting in Touch

- [Online Forum][online-forum] on Discourse
- [Chat Room][online-chat] on HipChat
- \#treode on StackOverflow: 
  [Browse questions asked][stackoverflow-read] or [post a new one][stackoverflow-ask]
- [Email](mailto:questions@treode.com)


[api-docs]: http://oss.treode.com/docs/scala/store/0.1.0 "API Docs"

[arch]: architecture.png "Architecture"

[presentation-2014-12-08]: http://goo.gl/HwZ81X "Presentation at Box, Dec 8 2014"

[online-chat]: http://www.hipchat.com/giwb5oIkz "Chat Room for Treode Users and Developers"

[online-forum]: https://forum.treode.com "Forum for Treode Users and Developers"

[stackoverflow-read]: http://stackoverflow.com/questions/tagged/treode "Read questions on Stack Overflow tagged with treode"

[stackoverflow-ask]: http://stackoverflow.com/questions/ask?tags=treode "Post a question on Stack Overflow tagged with treode"

[user-docs]: http://treode.github.io "TreodeDB Walkthroughs"