package com.abhi

import java.util.UUID

import com.netflix.astyanax.{AstyanaxContext, Keyspace}
import com.netflix.astyanax.connectionpool.NodeDiscoveryType
import com.netflix.astyanax.connectionpool.impl.{ConnectionPoolConfigurationImpl, CountingConnectionPoolMonitor}
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl
import com.netflix.astyanax.model.ColumnFamily
import com.netflix.astyanax.serializers.{StringSerializer, UUIDSerializer}
import com.netflix.astyanax.thrift.ThriftFamilyFactory
import com.typesafe.config.ConfigFactory

/**
  * Created by abhsrivastava on 11/6/16.
  */
trait CassandraHelper {
   def getContext(tableName : String) : AstyanaxContext[Keyspace] = {
      val config = ConfigFactory.load()
      val configImpl = new AstyanaxConfigurationImpl()
      configImpl.setDiscoveryType(NodeDiscoveryType.NONE)
      configImpl.setCqlVersion(config.getString("cqlVersion"))
      configImpl.setTargetCassandraVersion(config.getString("cassandraVersion"))
      val poolConfig = new ConnectionPoolConfigurationImpl(config.getString("connectionPoolName"))
      poolConfig.setPort(config.getInt("cassandraThriftPort"))
      poolConfig.setMaxConnsPerHost(config.getInt("maxConnectionsPerHost"))
      poolConfig.setSeeds(config.getString("cassandraSeeds"))

      val context = new AstyanaxContext.Builder()
         .forCluster("localhost")
         .forKeyspace(tableName)
         .withAstyanaxConfiguration(configImpl)
         .withConnectionPoolConfiguration(poolConfig)
         .withConnectionPoolMonitor(new CountingConnectionPoolMonitor())
         .buildKeyspace(ThriftFamilyFactory.getInstance())
      context.start()
      context
   }

   def getColumnFamily() : ColumnFamily[UUID, String] = {
      new ColumnFamily[UUID, String]("movielens_small", UUIDSerializer.get, StringSerializer.get)
   }
}
