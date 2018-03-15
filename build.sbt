lazy val scalautils = RootProject(uri("https://lipm-gitlab.toulouse.inra.fr/llegrand/scala-utils.git"))

lazy val DSparK = (project in file("."))
  .settings(
    inThisBuild(List(
      organization := "com.example",
      scalaVersion := "2.11.6",
      version := "0.1"
    )
  ), name := "DSparK",
     description := "A kmer counting program, using Spark",
     libraryDependencies += "org.apache.spark" %% "spark-core" % "2.1.0",
     libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.1.0",
     libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.1.0"
).dependsOn(scalautils)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
