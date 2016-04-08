lazy val root = (project in file(".")).settings(
  name := "E-Reputation",
  version := "1.0-SNAPSHOT",
  libraryDependencies ++= Seq(
    "org.apache.spark"  % "spark-core_2.10"              % "1.5.2",
    "org.apache.spark"  % "spark-streaming-twitter_2.10" % "1.6.0",
    "org.apache.spark"  % "spark-sql_2.10"               % "1.5.2",
    "org.apache.spark" % "spark-streaming_2.10" % "1.5.2",
    "org.apache.hbase" % "hbase-client" % "1.1.3" excludeAll(
      ExclusionRule(organization = "javax.servlet", name="javax.servlet-api"),
      ExclusionRule(organization = "org.mortbay.jetty", name="jetty"),
      ExclusionRule(organization = "org.mortbay.jetty", name="servlet-api-2.5")
      ),
    "org.apache.hbase" % "hbase-common" % "1.1.3" excludeAll(
      ExclusionRule(organization = "javax.servlet", name="javax.servlet-api"),
      ExclusionRule(organization = "org.mortbay.jetty", name="jetty"),
      ExclusionRule(organization = "org.mortbay.jetty", name="servlet-api-2.5")
      ),
    "org.apache.hbase" % "hbase-server" % "1.1.3" excludeAll(
      ExclusionRule(organization = "javax.servlet", name="javax.servlet-api"),
      ExclusionRule(organization = "org.mortbay.jetty", name="jetty"),
      ExclusionRule(organization = "org.mortbay.jetty", name="servlet-api-2.5")
      ),
    "joda-time" % "joda-time" % "2.9.2"
  )
)

resolvers ++= Seq(
  "Akka Repository" at "http://repo.akka.io/releases/",
  "Concurrent Maven Repo" at "http://conjars.org/repo/",
  "Spray Repository" at "http://repo.spray.cc/")

/*assemblyMergeStrategy in assembly :=  {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}*/

assemblyMergeStrategy in assembly := {
  case PathList("javax", "xml", xs @ _*) => MergeStrategy.last
  case PathList("org", "apache", xs @ _*) => MergeStrategy.last
  case PathList("com", "google", xs @ _*) => MergeStrategy.last
  case PathList("com", "esotericsoftware", xs @ _*) => MergeStrategy.last
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}
