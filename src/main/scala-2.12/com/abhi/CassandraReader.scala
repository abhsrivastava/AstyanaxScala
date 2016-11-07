package com.abhi

import com.netflix.astyanax.serializers._
import org.apache.cassandra.db.marshal.UTF8Type
import scala.collection.JavaConversions._

/**
  * Created by abhsrivastava on 11/3/16.
  */
object CassandraReader extends App with CassandraHelper {
   val context = getContext("movielens_small")
   val keyspace = context.getClient()
   val setSer = new SetSerializer[String](UTF8Type.instance)
   val cf = getColumnFamily()
   val result = keyspace.prepareQuery(cf).withCql("select name, avg_rating from movies").execute()
   val data = result.getResult.getRows()
   println("size: " + data.size())
   for {
      row <- data
      col = row.getColumns
   } {
      val name = col.getColumnByName("name")
      val avgRating = col.getColumnByName("avg_rating")
//      val genres = col.getColumnByName("genres")
//      val genValue = genres.getValue(setSer).toSet
      println(s"${name.getStringValue} rating: ${avgRating.getFloatValue}")
   }
   context.shutdown()
}
