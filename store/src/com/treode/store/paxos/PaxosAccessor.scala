package com.treode.store.paxos

import com.treode.async.{Callback, callback, defer}
import com.treode.pickle.Pickler
import com.treode.store.Bytes

trait PaxosAccessor [K, V] {

  def lead (key: K, value: V, cb: Callback [V]) (implicit paxos: Paxos)
  def propose (key: K, value: V, cb: Callback [V]) (implicit paxos: Paxos)
}

object PaxosAccessor {

  def apply [K, V] (pk: Pickler [K], pv: Pickler [V]): PaxosAccessor [K, V] =
    new PaxosAccessor [K, V] {

      def lead (key: K, value: V, cb: Callback [V]) (implicit paxos: Paxos): Unit =
        defer (cb) {
          paxos.lead (Bytes (pk, key), Bytes (pv, value), callback (cb) (_.unpickle (pv)))
        }

      def propose (key: K, value: V, cb: Callback [V]) (implicit paxos: Paxos): Unit =
        defer (cb) {
          paxos.propose (Bytes (pk, key), Bytes (pv, value), callback (cb) (_.unpickle (pv)))
      }}

  def apply(): PaxosAccessor [Bytes, Bytes] =
    new PaxosAccessor [Bytes, Bytes] {

      def lead (key: Bytes, value: Bytes, cb: Callback [Bytes]) (implicit paxos: Paxos): Unit =
        paxos.lead (key, value, cb)

      def propose (key: Bytes, value: Bytes, cb: Callback [Bytes]) (implicit paxos: Paxos): Unit =
        paxos.propose (key, value, cb)
  }

  def key [K] (pk: Pickler [K]): PaxosAccessor [K, Bytes] =
    new PaxosAccessor [K, Bytes] {

      def lead (key: K, value: Bytes, cb: Callback [Bytes]) (implicit paxos: Paxos): Unit =
        defer (cb) {
          paxos.lead (Bytes (pk, key), value, cb)
        }

      def propose (key: K, value: Bytes, cb: Callback [Bytes]) (implicit paxos: Paxos): Unit =
        defer (cb) {
          paxos.propose (Bytes (pk, key), value, cb)
        }}

  def value [V] (pv: Pickler [V]): PaxosAccessor [Bytes, V] =
    new PaxosAccessor [Bytes, V] {

      def lead (key: Bytes, value: V, cb: Callback [V]) (implicit paxos: Paxos): Unit =
        defer (cb) {
          paxos.lead (key, Bytes (pv, value), callback (cb) (_.unpickle (pv)))
        }

      def propose (key: Bytes, value: V, cb: Callback [V]) (implicit paxos: Paxos): Unit =
        defer (cb) {
          paxos.propose (key, Bytes (pv, value), callback (cb) (_.unpickle (pv)))
        }}}
