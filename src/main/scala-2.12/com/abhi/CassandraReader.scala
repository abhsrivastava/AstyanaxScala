package com.abhi

import java.nio.CharBuffer
import java.util.UUID

import com.netflix.astyanax.model.ColumnFamily
import com.netflix.astyanax.serializers.{SetSerializer, StringSerializer, UUIDSerializer}
import org.apache.cassandra.db.marshal
import org.apache.cassandra.db.marshal.UTF8Type

import scala.collection.JavaConversions._

/**
  * Created by abhsrivastava on 11/3/16.
  */
object CassandraReader extends App with CassandraHelper {
   val context = getContext("movielens_small")
   val keyspace = context.getClient()
   val cf = new ColumnFamily[UUID, String]("movies", UUIDSerializer.get(), StringSerializer.get)
   val result = keyspace.prepareQuery(cf).withCql("select name, avg_rating, genres from movies").execute()
   val data = result.getResult.getRows()
   println("size: " + data.size())
   for {
      row <- data
      col = row.getColumns
   } {
      val name = col.getColumnByName("name")
      val avgRating = col.getColumnByName("avg_rating")
      val setSer = new SetSerializer[String](UTF8Type.instance)
      val genres = col.getColumnByName("genres")
      val buf = genres.getByteArrayValue
      val genValue = setSer.fromBytes(buf)
      val genValue1 = col.getValue("genre", new SetSerializer[String](UTF8Type.instance), new java.util.HashSet[String]()).toSet
      println(s"${name.getStringValue} rating: ${avgRating.getFloatValue} genres: ${genValue}")
   }
   context.shutdown()
}
