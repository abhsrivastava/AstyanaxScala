name := "AstyanaxScala"

version := "1.0"

scalaVersion := "2.12.0"

libraryDependencies ++= Seq (
   "com.netflix.astyanax" % "astyanax-core" % "3.9.0",
   "com.netflix.astyanax" % "astyanax-cassandra" % "3.9.0",
   "com.netflix.astyanax" % "astyanax-thrift" % "3.9.0",
   "com.netflix.astyanax" % "astyanax-recipes" % "3.9.0",
   "com.typesafe" % "config" % "1.3.1"
)