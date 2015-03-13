# Treode Client Library Interface

The two major components of the Treode client library interface are the [Cache](#Cache) and the [Transaction](#Transaction).  

Caches store key-value pairs retrieved from the server, as well as the read and value timestamps associated with each pair that Treode provides.  Caches are responsible for maintaining a connection to the DB server.  Users may create multiple caches to work on disjoint sections of the database simultaneously.  

Transactions allows users to locally perform a sequence of database operations and, when finished, optimistically commit the result to the server by writing through the cache.  Users can choose upon creating a Transaction which cache to associate it with.

<a name="Cache"></a>
# Cache

We provide an overview of the Cache class below, including its primary internal data structure, its public methods, and the form of its HTTP requests and responses.

- Parameters: 
    1. server: string (required)
    2. port: string (port=80)
    3. max\_age: int (max\_age=None)
    4. no\_cache: bool (no\_cache=False)
- Public Methods: 
    1. read(table: string, key: string, read\_timestamp=currentTime, max\_age=None, no\_cache=False): value (JSON)
    2. write(table: string, op\_list: list): status (string), throws StaleException
- Internal Objects: 
    1. HTTP library for establishing/managing connection to server (in Python, urllib3)
    2. sorted map for maintaining cache (in Python, [skiplist](https://github.com/sepeth/python-skiplist))
- Exceptions: 
    1. StaleException
    2. StaleException(Value_time: int)

## Cache Structure Description

The cache internally maintains (1) a connection to the DB server and (2) a sorted key-value map of cache entries.  

### Database Connection

The cache should utilize an HTTP Request/Response library (e.g., in Python, urllib3) to establish a connection with the DB server and read and write values through its course of use.  We describe in the HTTP Message Formatting section the expected format for DB requests and responses.

[Here are examples](https://urllib3.readthedocs.org/en/latest/) of using the urllib3 library in Python to open and use a connection.

### Sorted Key-Value Map of Cache Entries

The cache is a ("table:key" map) of (sorted value maps), which we explain below.

The keys of the "table:key" map are composed of the concatenation of the table identifier string and the key string; each "table:key" maps to a (sorted value map).  (Note that the cache may store entries from multiple tables.)  

The cache entries themselves are stored in (sorted value maps).  The keys of the (sorted value maps) are derived from the read timestamps of each entry.  Since we want the entries sorted from most-recent to oldest read timestamps, the actual keys are longs with the value 

key = (LONG\_MAX - read\_timestamp)

For example, in Python, the outer "table:key" map can be implemented as a standard Python dictionary, and we just use string concatenation (a+b) to form the keys.  For the inner sorted value maps, we can use a SortedDict implementation based on [skiplists](https://github.com/sepeth/python-skiplist).

* **Question (1):** We said Tuesday we just need a single-level map, but why don't we need at least the two levels I describe here?

## Cache Public Method Descriptions

### read
    read(
        table: string, 
        key: string, 
        read_timestamp=currentTime, 
        max_age=None, 
        no_cache=False): value (JSON)  

The read method sends a read request to the DB for the given table and key, along with the specified cache parameters, if any.  It returns the value object if the DB provides a value; otherwise, it returns None.

In the case read returns None (value not in database), we still update our cache with the value entry None for the specified key at the read\_timestamp.  The None value may represent an update to the key.

Note that the max\_age, and no\_cache parameters can be specified in at least three separate locations: (1) in the Cache constructor, (2) in the Cache read method, and (3) in the Transaction read method.  The cache read method determines the most restrictive set of parameters based on these three inputs and sends this most restrictive parameter set to the DB.  With respect to the read\_timestamp parameter, the DB server's response to every query includes the read\_timestamp it utilized to fulfill the request; we use the read\_timestamp the server returns to us to update the cache.

Here is the method for determining the most restrictive parameter set:

* **read\_timestamp**: The most restritive read\_timestamp is the minimum of the given read\_timestamp values (i.e., min(cache, transaction) read\_timestamps)
* **max\_age**: The most restrictive max\_age is the minimum of all given max\_age values
* **no\_cache**: The most restrictive no\_cache value is the disjunction (boolean "or") of all given no\_cache values.  (If no\_cache == True, the cache must verify with the server by checking the value timestamp it has the most up-to-date value.)

### write
    write(
        table: string, 
        op_List: list): value (JSON) throws StaleException  

The write method sends the op\_List to the DB server as a batch write.  

If the server accepts the batch write, the write method also updates the cache to reflect the newly written values.

* **Question (2)**: What is the expected format of the op\_List?  (What are the possible types of its elements?)

* **Question (3)**: Should the write method update the cache upon a successful write (with the newly written values)?

Otherwise, the write method throws a stale exception, including the value timestamp in the DB that caused the write reject if it is available.

## HTTP Message Formatting
First, we specify the default and custom headers our HTTP messages utilize.  Then we describe the expected forms of requests/responses between the cache and TreodeDB server.

### Use of HTTP Headers

In our use of HTTP headers, the term *age* refers to time since the user-specified read\_timestamp.  Specifically, every TreodeDB entry has both a *value timestamp (V_t)* and *read timestamp (R_t)*.  We maintain as an invariant that the given key contained the entry's value between [V\_t, R\_t].

We use the following HTTP headers:

* **max-age**: The max number of seconds between our given read\_timestamp and the read\_timestamp of the entry, i.e., (input read\_timestamp) - (entry read\_timestamp) <= max-age.  In other words, we are willing to read a value that could be stale by at most max-age seconds. 
* **no-cache**: Forces cache to send a request to the DB server to retrieve the requested value.  The result cannot just be returned from cache.
* **Read-TxClock**: Contains the user's specified read\_timestamp.  The value returned by the DB server will be the most recent value older than the specified read\_timestamp value.  Note that Read-TxClock is an integer; it is specified in milliseconds to clarify that it should **not** be manipulated as a date.

Additionally, note that max-age and no-cache are existing HTTP Caching headers specified in [RFC7234](http://tools.ietf.org/html/rfc7234#section-5.2.1), while the read\_timestamp is sent through the Read-TxClock header, a custom HTTP header.

Below are the expected forms of requests/responses between the cache and TreodeDB server:

### Requests

We give examples of HTTP requests below.  For additional explanation, please see the source of these examples [OMVCC and Caching](https://forum.treode.com/t/omvcc-and-http-caching/62).

#### Read Requests  

The cache message to the DB includes the table id, the desired key, and headers restricting the timestamps of the value.  (We describe in the Use of HTTP Headers section how each header restricts the value we read.)

HTTP read requests should follow this format:

    GET /table_id_string/key_string HTTP/1.1
    Cache-Control: max-age=[max_age], no-cache
    Read-TxClock: [read_timestamp_in_milliseconds]

* **Quetion (4)**: Did I get the format for these messages (read request, write request, read response, write response) exactly correct?

Here is an example of an HTTP read request:

    GET /movie/star-wars HTTP/1.1
    Cache-Control: max-age=300, no-cache
    Read-TxClock: 1421236153024853

In this example, the user is requesting the value of key "star-wars" in the table "movie".  The value should be written before (or on) time 1421236153024853 milliseconds but not more than 300 seconds before time 1421236153024853.  "no-cache" indicates that the cache MUST make a request to the DB server to retrieve this value, even if it has a value satisfying the requirements.

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

* **Question(5)**: How do we format batch writes as an HTTP request?

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
    2. read\_timestamp: int (read\_timestamp=None)
    3. max\_age: int (max\_age=None)
    4. no\_cache: bool (no\_cache=False)
- Public Methods: 
    1. read(table: string, key: string, read\_timestamp=currentTime, max\_age=None, no\_cache=False) returns value (JSON) or None
    2. write(table: string, key: string, value: JSON) returns None (always succeeds)
    3. delete(table: string, key: string) returns None
    4. commit() returns None, throws StaleException
- Internal Objects: 
    1. min\_Rt: int (The minimum read time the Transaction has seen so far, based on values read)
    2. max\_Vt: int (The maximum value time the Transaction has seen so far, based on values read)
    3. view: list (The time-ordered list of operations performed within the Transaction to be committed to the DB server.)
- Exceptions: 
    1. StaleException
    2. StaleException(Value_time: int)

## Transaction Internal Structure Description

First, we must understand the purpose of the Transaction's min\_Rt (min read time) and max\_Vt (max value time).  A Transaction consists of a set of updated database entries.  The database will not allow any Transaction to commit a result derived from an inconsistent snapshot of the database values.  

Specifically, every entry in a snapshot of the Treode database has both read and value timestamps.  A snapshot is consistent if and only if (maximum value timestamp) < (minimum read timestamp).  (See [this explanation](https://forum.treode.com/t/eventual-consistency-and-transactions-working-together/36).)

Then, by the invariant established previously that each entry is guaranteed to be the value in the DB between [V\_t, R\_t], it follows that there was a point in time when the DB contained all the values in the snapshot simultaneously.

The Transaction uses min\_Rt and max\_Vt to maintain this invariant and avoid wasting network time sending commits to the DB server guaranteed to be rejected.  If any Transaction read causes (max\_Vt > min\_Rt), the read method throws a StaleException(Vt), including the value time that caused the violation.  (Note that a read time should not cause a violation because we can simply set min-age=0. Contrarily, we cannot control the value times through our read query parameters.) 

### min\_Rt 

The Transaction maintains the minimum read time it has seen so far.  On every read, if the read time is less than min\_Rt, we update min\_Rt.  

Note that this read time is NOT the time the Transaction read the entry.  Transaction reads from cache do not update entry read times; cache reads from the DB server do update entry read times. 

### max\_Vt

The Transaction maintains the maximum value time it has seen so far.  On every read, if the value time is greater than max\_Vt, we update max\_Vt.

Note that updating max\_Vt can cause the invariant violation described above, causing the Transaction read method to throw StaleException(Vt).  Intuitively, when reading an entry from the DB, we have no idea when it was last updated until we actually read it.  As a result, we cannot avoid the possibility of violating our consistent snapshot invariant.

* **Question (6):** If the user fails to specify max\_age correctly, should we throw a StaleException?

### view 

The view maintains the set of operations performed throughout the Transaction.  When the user commits the transactions, each updated value in the view is sent to the cache, which writes the changes through to the DB server.  (If the view's values are out-of-date with the DB, write will throw a StaleException.)

Like the cache, the view is structured as a ("table:key" map) of (sorted value maps).  The "table:key" map has keys composed of the concatenation of (table identifier) and (key) strings; its values are the (sorted value maps) described next.  The (sorted value maps) are from (LONG\_MAX - read_timestamp) keys to values (JSON), as in the cache.  (Note, as mentioned above, the keys are (LONG\_MAX - read\_timestamp) rather than just (read\_timestamp) because we want the table's entries to be sorted in reverse, from most recent to oldest timestamp.)  

Additionally, every "table:key" key in the view also stores two booleans called "updated" and "deleted" alongside its sorted map.  The "updated" boolean is False if the Transaction has only read the value of that key and True if the Transaction has written an update to that key's value.  The "deleted" boolean is True if and only if the user has called the delete method on its key during this Transaction.

Upon commit, we search through the map keys for updated values (updated==True) and deleted values (deleted==True) and construct a set of op\_lists from the most recent value (sorted by (LONG\_MAX - read\_timestamps)) in each updated key's sorted map.  Note the op\_list is a parameter to Cache's write method, and each table needs its own op\_list.

## Transaction Public Methods

Note that each of the methods below first attempts to contact the cache to complete its operation.  If the cache does not contain the necessary information, the cache makes the HTTP request to the HTTP server.  (The Transaction does not directly make HTTP requests.)

### read
    read(
        table: string, 
        key: string, 
        read_timestamp=currentTime, 
        max_age=None, 
        no_cache=False) returns value (JSON) or None

The read method first attempts to read the specified key from the view, then from cache, and finally from the DB server.  If read finds the requested value, it stores it in the view; otherwise, it returns None and stores the key had no value (in Python, None) for that read_timestamp satisfying the given parameters.

### write
    write(
        table: string, 
        key: string, 
        value: JSON) returns None (always succeeds)

The write method updates the Transaction view to reflect the specified write.  (They key immediately has its "updated" status set to True.)  Note that writes never immediately fail (are never immediately rejected) because they are only reflected in the view until we commit the Transaction.

Write adds the new value to its sorted map with the current time as both the read and value timestamp.

* **Question (7)**: Should we set both the read and value timestamps of new entries to the current time?

### delete
    delete(
        table: string, 
        key: string) returns None

If the key exists in the Transaction view, set its "deleted" boolean to True.  Otherwise, add the key to the view with "deleted" set to True and the value None.

### commit
    commit() returns None, throws StaleException

Initialize an operation dictionary D mapping (table identifier strings) to (operation lists).  Iterate through keys in the view.  For each "table:key" key in the view, consider its sorted map value, along with its "updated" and "deleted" flags.  

If the "updated" or "deleted" flag is set to True, add the corresponding DB operation to the op list for that table in D.  Next, iterate through the keys in D and call the cache's write method on each of the Transaction's (table, op\_List) pairs.