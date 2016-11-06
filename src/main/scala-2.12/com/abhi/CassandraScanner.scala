package com.abhi

import java.util.UUID

import com.netflix.astyanax.connectionpool.NodeDiscoveryType
import com.netflix.astyanax.connectionpool.impl.{ConnectionPoolConfigurationImpl, CountingConnectionPoolMonitor, SimpleAuthenticationCredentials}
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl
import com.netflix.astyanax.AstyanaxContext
import com.netflix.astyanax.model.ColumnFamily
import com.netflix.astyanax.serializers.UUIDSerializer
import com.netflix.astyanax.serializers.StringSerializer
import com.netflix.astyanax.serializers.FloatSerializer
import com.netflix.astyanax.thrift.ThriftFamilyFactory

import scala.collection.JavaConversions._

/**
  * Created by abhsrivastava on 11/3/16.
  */
object CassandraScanner extends App {

   val configImpl = new AstyanaxConfigurationImpl()
   configImpl.setDiscoveryType(NodeDiscoveryType.NONE)
   configImpl.setCqlVersion("3.4.2")
   configImpl.setTargetCassandraVersion("3.7")
   val poolConfig = new ConnectionPoolConfigurationImpl("MyConnectionPool")
   poolConfig.setPort(9160)
   poolConfig.setMaxConnsPerHost(1)
   poolConfig.setSeeds("192.168.1.169:9160")

   val context = new AstyanaxContext.Builder()
      .forCluster("localhost")
      .forKeyspace("movielens_small")
      .withAstyanaxConfiguration(configImpl)
      .withConnectionPoolConfiguration(poolConfig)
      .withConnectionPoolMonitor(new CountingConnectionPoolMonitor())
      .buildKeyspace(ThriftFamilyFactory.getInstance())
   context.start()
   val keyspace = context.getClient()
   val cf = new ColumnFamily[UUID, String]("cf", UUIDSerializer.get, StringSerializer.get)
   val result = keyspace.prepareQuery(cf).withCql("select name, avg_rating from movies").execute()
   val data = result.getResult.getRows()
   for {
      row <- data
      col = row.getColumns
   } {
      val name = col.getColumnByName("name")
      val avgRating = col.getColumnByName("avg_rating")
      println(s"${name.getStringValue} rating: ${avgRating.getFloatValue}")
   }
   context.shutdown()
}
