package com.abhi

/**
  * Created by abhsrivastava on 11/6/16.
  */

import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger

import com.netflix.astyanax.recipes.reader.AllRowsReader
import com.netflix.astyanax.model.{ColumnFamily, Row}
import com.netflix.astyanax.serializers.{SetSerializer, StringSerializer, UUIDSerializer}
import org.apache.cassandra.db.marshal.UTF8Type
object CassandraScanner extends App with CassandraHelper {
   val context = getContext("movielens_small")
   val cf = new ColumnFamily[UUID, String]("movies", UUIDSerializer.get, StringSerializer.get)
   val setSer = new SetSerializer[String](UTF8Type.instance)
   val keyspace = context.getClient
   var count : AtomicInteger = new AtomicInteger(0)
   val allReader = new AllRowsReader.Builder(keyspace, cf)
      .withPageSize(100)
      .withConcurrencyLevel(10)
      .withPartitioner(null)
      .forEachRow { case row : Row[UUID, String] =>
         val cols = row.getColumns
         println(cols.getColumnByIndex(3).getStringValue.trim)
         if (cols.getColumnByIndex(2).hasValue) {
            println(cols.getColumnByIndex(2).getValue(setSer))
         }
         count.incrementAndGet()
         true
      }
      .build()
      .call()
   println(s"Total value ${count.get()}")
   context.shutdown()
}
