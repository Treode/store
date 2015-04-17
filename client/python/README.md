# Treode Python 2.7 Client

This client supports a Transactional interface to TreodeDB.  

It take advantage of Treode's batch write functionality and maintains a local LRU cache.

Users can also leverage intermediate HTTP caches.

# Dependencies

Library: urllib3, llist, functools

Tests: mock

# Testing

python -m unittest discover 