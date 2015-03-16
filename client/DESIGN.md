# Treode Client Library Interface

The two major components of the Treode client library interface are the [Cache](#Cache) and the [Transaction](#Transaction).  

Caches store key-value pairs retrieved from the server, as well as the read and value timestamps associated with each pair that Treode provides.  Caches are responsible for maintaining a connection to the DB server.

Transactions allows users to locally perform a sequence of database operations and, when finished, optimistically commit the result to the server by writing through the cache.

<a name="Cache"></a>
# Cache

We provide an overview of the Cache class below, including its primary internal data structure, its public methods, and the form of its HTTP requests and responses.

- Parameters: 
    1. server: string (required)
    2. port: string (port=80)
    3. max\_age: int (max\_age=None)
    4. no\_cache: bool (no\_cache=False)
- Public Methods: 
    1. read(table: string, key: string, max\_age=None, no\_cache=False): value (JSON)
    2. write(table: string, op\_list: list): status (string), throws StaleException
- Internal Objects: 
    1. HTTP library for establishing/managing connection to server (in Python, urllib3)
    2. map for maintaining cache
- Exceptions: 
    1. StaleException
    2. StaleException(Value_time: int)

## Cache Structure Description

The cache internally maintains (1) a connection to the DB server and (2) a map of cache entries.  

### Database Connection

The cache should utilize an HTTP Request/Response library (e.g., in Python, urllib3) to establish a connection with the DB server and read and write values through its course of use.  We describe in the HTTP Message Formatting section the expected format for DB requests and responses.

[Here are examples](https://urllib3.readthedocs.org/en/latest/) of using the urllib3 library in Python to open and use a connection.

### Map of Cache Entries

We propose two possible designs for implementing the cache; the preferable of the two designs depends largely on the existing libraries available in the application language.

#### Two-Level Map Implementation

The cache is a two-level map of the following type:

    table : key -> value-time -> value : read-time

In Python, the top-level map is a standard Python dictionary, and the inner map is simply an array of `(value-time, value : read-time)` tuples.

Specifically, the `table : key` keys are composed of the concatenation of the table identifier string and the key string.  Each `table:key` maps to an arraylist with entries of the following type:

    (value-time, value : read-time)   

The cache entries themselves are stored in these arraylists.  The values are tuples derived from the value timestamps and `value : read-time` concatenated strings.  

If the arraylists are unsorted, then `add` is *O(1)*, and `find` (floor), `replace`, and `remove` are each *O(n)*, where `n` is the length of the arraylist.  If the arraylists are sorted (which takes *O(nlogn)*), all the operations are *O(n)*.  Thus, the arraylists do not need to be sorted, as sorting would not improve the asymptotic runtimes of these operations.

Note that the cache can indicate the DB did not have a value for a certain key at a given time by storing None in Python (null in other languages) in the `(value, read-time)` entry. 

#### Skiplist Implementation

If an appropriate skiplist library is available, the cache can be implemented as a skiplist which supports `floor` and `ceiling` operations.  The cache is then a single-level map and has the following type:

    table : key : (Long.Max - ValueTime) -> (value, read-time)

Here is an example of a [skiplist](https://github.com/sepeth/python-skiplist) library in Python, though it does not support floor and ceiling operations.

## Cache Public Method Descriptions

### read
    read(
        table: string, 
        key: string,
        max_age=None, 
        no_cache=False): value (JSON)  

The read method first checks the cache.  If the entry is missing, or if it has been too long since we read that entry, it sends a read request to the DB for the given table and key, along with the specified cache parameters, if any.  It returns the value object if the DB provides a value; otherwise, it returns None.

In the case read returns None (value not in database), we still update our cache with the value entry None for the specified key using the read\_timestamp.  The None value may represent an update to the key.

Note that the max\_age, and no\_cache parameters can be specified by the user in at least three separate locations: (1) in the Cache constructor, (2) in the Cache read method, and (3) in the Transaction read method.  The cache read method determines the most restrictive set of parameters based on these three inputs and sends this most restrictive parameter set to the DB.

Here is how we define the most restrictive parameter set:

* **max\_age**: The most restrictive max\_age is the minimum of all given max\_age values (which we formalize [here](#minRt)).

* **no\_cache**: The most restrictive no\_cache value is the disjunction (boolean "or") of all given no\_cache values.  (If no\_cache == True, the cache must verify with the server by checking the value timestamp it has the most up-to-date value.)

Note that these max\_age and no\_cache values also appear in Cache read method, though users cannot control their values through this method.

### write
    write(
        table: string, 
        op_List: list): value (JSON) throws StaleException  

The write method sends the op\_List to the DB server as a batch write.  

If the server accepts the batch write, the write method also updates the cache to reflect the newly written values.

Otherwise, the write method throws a stale exception, including the value timestamp in the DB that caused the write reject if it is available.

## HTTP Message Formatting
First, we specify the default and custom headers our HTTP messages utilize.  Then we describe the expected forms of requests/responses between the cache and TreodeDB server.

### Use of HTTP Headers and Directives

In our use of HTTP directives, the term *age* refers to time since the user-specified read\_timestamp.  Specifically, every TreodeDB entry has both a *value timestamp (V_t)* and *read timestamp (R_t)*.  We maintain as an invariant that the given key contained the entry's value between [V\_t, R\_t].

We use the following HTTP directives (under the HTTP header Cache-Control):

* **max-age**: The max number of seconds between our given read\_timestamp and the read\_timestamp of the entry, i.e., (input read\_timestamp) - (entry read\_timestamp) <= max-age.  In other words, we are willing to read a value that could be stale by at most max-age seconds. 

* **no-cache**: Forces cache to send a request to the DB server to retrieve the requested value.  The result cannot just be returned from cache.

An example of usage is below:

    GET /[table_id_string]/[key_string] HTTP/1.1
    Cache-Control: max-age=3, no-cache

In addition, we use the following custom HTTP header: 

* **Read-TxClock**: Contains the user's specified read\_timestamp.  The value returned by the DB server will be the most recent value older than the specified read\_timestamp value.  Note that Read-TxClock is an long (64 bits).

Read-TxClock is specified in microseconds to clarify that it should **not** be *manipulated* as a date.  While applications may display Read-TxClock to users in Date format for clarity, the application must internally maintain Read-TxClock as a long (64 bits) to avoid rounding to seconds. 

Additionally, note that max-age and no-cache are existing HTTP Caching headers specified in [RFC7234](http://tools.ietf.org/html/rfc7234#section-5.2.1), while the read\_timestamp is sent through the Read-TxClock header, a custom HTTP header.

Below are the expected forms of requests/responses between the cache and TreodeDB server:

### Requests

We give examples of HTTP requests below.  For additional explanation, please see the source of these examples [OMVCC and Caching](https://forum.treode.com/t/omvcc-and-http-caching/62).

#### Read Requests  

The cache message to the DB includes the table id, the desired key, and headers restricting the timestamps of the value.  (We describe in the Use of HTTP Headers section how each header restricts the value we read.)

HTTP read requests should follow this format:

    GET /[table_id_string]/[key_string] HTTP/1.1
    Cache-Control: max-age=[max_age_in_seconds], no-cache
    Read-TxClock: [read_timestamp_in_microseconds_long_64_bits]

Here is an example of an HTTP read request:

    GET /movie/star-wars HTTP/1.1
    Cache-Control: max-age=300, no-cache
    Read-TxClock: 1421236153024853

The user is requesting the value of key "star-wars" in the table "movie".  Since this key's value may have changed over time, the user gives a time range for the value they want.  

Specifically, the user wants a value that was written to the DB by time 1421236153024853 microseconds (where time 0 is some arbitrary starting point in the past).  However, the user does not want a value that was written to the DB more than 300 seconds before time 1421236153024853.  Thus, the time range is 

    [1421236153024853 - 300, 1421236153024853].  

Finally, the "no-cache" indicates that the cache *must* send a read request to the DB server to retrieve this value, even if the cache already has a value satisfying the user's requirements.

#### Write Requests  

The cache message to the DB includes the table id and a list of operations to perform on the table.  The operations should be sent to the DB server as a Treode batch write operation.

The HTTP request for write should be similar to the following:

    PUT /table_id_string/key_string HTTP/1.1
    Condition-TxClock: [condition_timestamp_in_milliseconds]
    {
        [JSON_value]
    }

Here is an example of the HTTP write request:

    PUT /movie/star-wars HTTP/1.1
    Condition-TxClock: 1368151663681367
    {   "title": "Star Wars",
        "cast": [
            { "actor": "Mark Hamill", "role": "Luke Skywalker" },
            { "actor": "Carrie Fisher", "role": "Princess Leia Organa" },
            { "actor": "Harrison Ford", "role": "Han Solo" } ] 
    }

We write the given JSON content to key "star-wars" in table "movie", provided that the current value associated with the "star-wars" key has remained unchanged since the time 1368151663681367 milliseconds.

In addition, the DB server supports batch writes, and the cache will in prctice use batch writes instead of the traditional write format described above.

There are several batch write formats the DB server accepts:

##### JSON Array
Simply wrap the multiple write requests in a JSON array, as shown below.

    [ { "op": "create", "table": "movie", "key": "star-wars", value={ /* movie object */ } },
      { "op": "delete", "table": "movie", "key": "stars-war" } } ]

The rest of the HTTP request remains as described above.

##### Request Pipelining

Send each item in the batch write to the server as a separate HTTP request.  We indicate in the requests that they are part of a batch write with the item header as shown below in the example two-part batch write:

    PUT /movie/star-wars HTTP/1.1
    X-Item: 1/2

    { /* movie object */ }

    DELETE /movie/stars-war HTTP/1.1
    X-Item: 2/2

Unlike the JSON array batch write method, this Request Pipelining method will allow intermediate HTTP proxies to see that their cache entries for each item in the batch write are invalid.

* **Question(1)**: I don't think Item is a real HTTP header (at least, not in Wikipedia...).  Can we just define a custom header as something like above?

### Responses
As with the Requests section, please see more information in [OMVCC and Caching](https://forum.treode.com/t/omvcc-and-http-caching/62).

#### Read Respones

The DB will respond to read requests with a message containing the desired value or an indication the value does not exist in the table. 

In the success case, HTTP response for read should be similar to the following:

    HTTP/1.1 200 Okay
    Date: Wed, 14 Jan 2015 11:49:13 GMT
    Last-Modified: Fri, 10 May 2013 02:07:43 GMT
    Read-TxClock: 1421236153024853
    Value-TxClock: 1368151663681367
    Vary: Request-TxClock
    {   "title": "Star Wars",
        "cast": [
            { "actor": "Mark Hamill", "role": "Luke Skywalker" },
            { "actor": "Carrie Fisher", "role": "Princess Leia Organa" } ] }

There are several cases in which the HTTP response does not include the JSON-formatted requested value.  Here are those cases:

- **HTTP/1.1 304 Not Modified**: The requested value has not changed since the last request.  Note this response is still valuable to the cache, as it allows the cache to update the read time (R_t) for this entry to the read\_timestamp of this request.  (Between when the value was written and this read\_timestamp, we know the value has not changed.)

- **HTTP/1.1 404 Not Found**: The requested value is not in the table.  This response is also valuable to the cache, as the cache can store None (in Python, possibly null in other languages) in the JSON entry for this entry at the given read\_timestamp.  Then future requests see the key had no value at this time without going to the DB server.

#### Write Responses

The DB will respond with either a success or rejection indicator.  If the DB rejects, the cache write method should throw an Stale Exception, indicating to the caller to retry the transaction with more up-to-date values from the DB.

The HTTP request for write should be similar to the following:

    HTTP/1.1 200 Okay
    Value-TxClock: 1421236393816753

However, it is also possible the DB server will be unable to complete the write because another client has written new values in the meantime.  In this case, the HTTP write resposne will be of the following form:

    HTTP/1.1 412 Precondition Failed 

In this case, the the client should retry the transaction with a smaller value for the min-age header (e.g., min-age=0) to ensure the values it receives from the DB are not stale.  

<a name="Transaction"></a>
# Transaction

- Parameters: 
    1. cache: Cache (required)
    2. read\_timestamp: long (64 bits) (read\_timestamp=None)
    3. max\_age: int (max\_age=None)
    4. no\_cache: bool (no\_cache=False)
- Public Methods: 
    1. read(table: string, key: string, max\_age=None, no\_cache=False) returns value (JSON) or None
    2. write(table: string, key: string, value: JSON) returns None (always succeeds)
    3. delete(table: string, key: string) returns None
    4. commit() returns None, throws StaleException
- Internal Objects: 
    1. min\_Rt: int (The minimum read time the Transaction has seen so far, based on values read)
    2. max\_Vt: int (The maximum value time the Transaction has seen so far, based on values read)
    3. view: map of type table:key -> (op, value)  (All the operations performed by the user during the Transaction)
- Exceptions: 
    1. StaleException
    2. StaleException(Value_time: int)

* **Question (2)**: I made many of the values above ints, but should they actually be longs (64 bit) to store values derived from the read\_timestamp?

## Transaction Internal Structure Description

First, we must understand the purpose of the Transaction's min\_Rt (min read time) and max\_Vt (max value time).  A Transaction consists of a set of updated database entries.  The database will not allow any Transaction to commit a result derived from an inconsistent snapshot of the database values.  

Specifically, every entry in a snapshot of the Treode database has both read and value timestamps.  A snapshot is consistent if and only if (maximum value timestamp) < (minimum read timestamp).  (See [this explanation](https://forum.treode.com/t/eventual-consistency-and-transactions-working-together/36).)

Then, by the invariant established previously that each entry is guaranteed to be the value in the DB between [V\_t, R\_t], it follows that there was a point in time when the DB contained all the values in the snapshot simultaneously.

The Transaction uses min\_Rt and max\_Vt to maintain this invariant and avoid wasting network time sending commits to the DB server guaranteed to be rejected.  If any Transaction read causes (max\_Vt > min\_Rt), the read method throws a StaleException(Vt), including the value time that caused the violation.  

Note that only bad value times can cause Stale Exceptions.  We prevent bad read times by setting max-age=0, thereby requiring we get the current value.

<a name="minRt"></a>
### min\_Rt 

The Transaction maintains the minimum read time it has seen so far.  On every read, if the read time is less than min\_Rt, we update min\_Rt.  

Note that the read time is defined to be the most recent time we verified the entry as current in the database.  (It does not depend on when we read the value from Cache.)

We can ensure min\_Rt is greater than our max\_Vt (described next), thereby avoiding bad read times, by adjusting the max-age HTTP directive.  We adjust the max-age down if the user specifies a max-age allowing max\_Vt >= min\_Rt.  Specifically, we define the concept of a rolling max-age: 

1. We initialize max-age to the value the user specifies in the Transaction's constructor.
2. In this step, we have two parameters.  (a) The Transaction object has a `read_timestamp` specified in the constructor.  (b)  Each time we call read, we get back an entry with a value time `V_t`.  Using these parameters, we define the rolling max age: 
    

    rolling max age = min(max-age, read\_timestamp - V_t)

We set the max-age to be

    min(rolling max age, max age parameter to Transaction's read method)

Since the max age is at most `read_timestamp - max_Vt` for all values read so far, we ensure that the min(R\_t) > max\_Vt.

### max\_Vt

The Transaction maintains the maximum value time it has seen so far.  On every read, if the value time is greater than max\_Vt, we update max\_Vt.

Note that updating max\_Vt can cause the invariant violation described above, causing the Transaction read method to throw StaleException(Vt).  Intuitively, when reading an entry from the DB, we have no idea when it was last updated until we actually read it.  As a result, we cannot avoid the possibility of violating our consistent snapshot invariant.

### view 

The view maintains the map of operations performed throughout the Transaction.  This map has type table:key -> (op, value).  When the user commits the transactions, each updated value in the view is sent to the cache, which writes the changes through to the DB server via batch write.  (If the view's values are out-of-date with the DB, write will throw a StaleException.)

Note that every value read from the cache during the Transaction is stored in this map.  Even values that do not change within the Transaction are stored in the map with the *hold* operation, as we explain below.

Unlike the Cache, the Transaction does not maintain a history of values.  Rather, it stores the user's operations on each entry.

Consider an arbitrary entry in the DB of the form 
    table:key -> (op, value)  
The possible ops in the map are the following:

#### create
Add a new entry to the DB with key *key* and value *value*.

#### hold
Maintain the current value in the DB.  The *hold* operation does not require a value, i.e., `table:key -> (hold, None)`, because we are not changing the value.

The hold operation exists because the computed output of our Transaction may be dependent on any value that we read from Cache during the Transaction.  Therefore, the success of the Transaction depends on these values having NOT changed in the DB between the time we read and commit them.  If these value have changed, the Transaction is stale and should throw a StaleException.  The *hold* operation allows us to maintain this invariant.

#### update
Replace the given key's old value with the new value provided.

#### delete
Remove the given key from the database.  The *delete* operation does not require a value, i.e., `table:key -> (delete, None)`, because we are not changing the value.

## Transaction Public Methods

Here is the hierarchy of cache-lookup on a user request.  Each of the methods below (1) first looks to the view for the values it needs to complete its operation.  (2) If the values are not in the view, it attempts to contact the cache to complete its operation.  (3) If the cache does not contain the necessary information, the cache makes the HTTP request to the HTTP server.  (The Transaction does not directly make HTTP requests.)

### read
    read(
        table: string, 
        key: string, 
        max_age=None, 
        no_cache=False) returns value (JSON) or None

The read method first attempts to read the specified key from the view, then from cache, and finally from the DB server.  If read finds the requested value, it stores it in the view; otherwise, it returns None and stores the key had no value (in Python, None) for that read_timestamp satisfying the given parameters.

Note that all Transaction reads have the same read timestamp, which is specified in the constructor (unlike the Cache read method).

### write
    write(
        table: string, 
        key: string, 
        value: JSON) returns None (always succeeds)

The write method updates the Transaction view to reflect the specified write.  (They key immediately has its "updated" status set to True.)  Note that writes never immediately fail (are never immediately rejected) because they are only reflected in the view until we commit the Transaction.

Write adds the new value to a map of type table:key -> (op, value).  In this case, the op is *write*, and the value is whatever the user provides.

When the user eventually commits the Transaction, we pass this map to the Cache write method.  The Cache's write method iterates through the values in the map and sends a batch write operation to the DB server, where the batch write is generally a list of the form [(table, key, op, value)].  (Note that some operations, such as delete, do not require values.)

### delete
    delete(
        table: string, 
        key: string) returns None

If the key exists in the Transaction view, set its "deleted" boolean to True.  Otherwise, add the key to the view with "deleted" set to True and the value None.

### commit
    commit() returns None, throws StaleException

As described above, the view maintains a map of type table:key -> (op, value), which stores all operations the user performs on the DB via the Transaction. 

To commit, we simply pass this map to the Cache write method, which iterates through the map entries and constructs a batch write composed of all its entries.  Finally, it sends this batch write to the DB server.