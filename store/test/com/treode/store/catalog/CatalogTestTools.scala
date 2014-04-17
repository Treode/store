package com.treode.store.catalog

import com.treode.store.StoreTestTools

private object CatalogTestTools extends StoreTestTools {

  implicit class TestableAcceptor (a: Acceptor) {

    def isDeliberating = a.state.isInstanceOf  [Acceptor#Deliberating]
    def isClosed = a.state.isInstanceOf [Acceptor#Closed]

    def getChosen: Option [Int] = {
      if (isClosed)
        Some (a.state.asInstanceOf [Acceptor#Closed] .chosen.checksum)
      else if (isDeliberating)
        a.state.asInstanceOf [Acceptor#Deliberating] .proposal.map (_._2.checksum)
      else
        None
    }}}
