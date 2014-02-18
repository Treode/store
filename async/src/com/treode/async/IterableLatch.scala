package com.treode.async

class IterableLatch [A] private [async] (iter: Iterable [A]) {

  private def run [B] (f: A => Async [B], cb: Callback [B]): Unit =
    iter foreach (x => f (x) run (cb))

  def array [B] (f: A => Async [(Int, B)]) (implicit m: Manifest [B]): Async [Array [B]] =
    Async.async { cb =>
      run (f, new ArrayLatch (iter.size, cb))
    }

  def map [K, V] (f: A => Async [(K, V)]): Async [Map [K, V]] =
    Async.async { cb =>
      run (f, new MapLatch [K, V] (iter.size, cb))
    }

  def seq [B] (f: A => Async [B]) (implicit m: Manifest [B]): Async [Seq [B]] =
    Async.async { cb =>
      run (f, new SeqLatch [B] (iter.size, cb))
    }

  def unit (f: A => Async [Unit]): Async [Unit] =
    Async.async { cb =>
      run (f, new CountingLatch (iter.size, cb))
    }}
