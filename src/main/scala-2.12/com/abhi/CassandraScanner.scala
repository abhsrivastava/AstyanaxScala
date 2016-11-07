package com.abhi

/**
  * Created by abhsrivastava on 11/6/16.
  */

import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger

import com.netflix.astyanax.recipes.reader.AllRowsReader
import com.netflix.astyanax.model.Row
object CassandraScanner extends App with CassandraHelper {
   val context = getContext("movielens_small")
   val cf = getColumnFamily()
   val keyspace = context.getClient
   var count : AtomicInteger = new AtomicInteger(0)
   val allReader = new AllRowsReader.Builder(keyspace, cf)
      .withPageSize(100)
      .withConcurrencyLevel(10)
      .withPartitioner(null)
      .forEachRow { case row : Row[UUID, String] =>
            val cols = row.getColumns
            val movieName = cols.getColumnByName("name")
            val movieNameVal = movieName.getStringValue
            count.incrementAndGet()
            true
      }
      .build()
      .call()
   println(s"Total value ${count.get()}")
}
