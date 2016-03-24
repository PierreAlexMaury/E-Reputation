lazy val root = (project in file(".")).settings(
  name := "E-Reputation",
  version := "1.0-SNAPSHOT",
  libraryDependencies ++= Seq(
    "org.apache.spark"  % "spark-core_2.10"              % "1.6.0" % "provided",
    "org.apache.spark"  % "spark-streaming-twitter_2.10" % "1.6.0",
    "org.apache.spark"  % "spark-sql_2.10"               % "1.6.0",
    "org.apache.spark" % "spark-streaming_2.10" % "1.6.0" % "provided",
    "it.nerdammer.bigdata" % "spark-hbase-connector_2.10" % "1.0.2",
    "joda-time" % "joda-time" % "2.9.2"
  )
)

resolvers ++= Seq(
  "Akka Repository" at "http://repo.akka.io/releases/",
  "Concurrent Maven Repo" at "http://conjars.org/repo/",
  "Spray Repository" at "http://repo.spray.cc/")

assemblyMergeStrategy in assembly :=  {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

/*assemblyMergeStrategy in assembly := {
  case PathList("javax", "xml", xs @ _*) => MergeStrategy.last
  case PathList("org", "apache", xs @ _*) => MergeStrategy.last
  case PathList("com", "google", xs @ _*) => MergeStrategy.last
  case PathList("com", "esotericsoftware", xs @ _*) => MergeStrategy.last
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}*/
