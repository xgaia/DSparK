lazy val root = (project in file(".")) // your project
  .settings ( // Definition of your project
  inThisBuild( List(
    organization := "fr.inra.lipm",
    scalaVersion := "2.11.6", // As scala-utils IS 2.11.6. Note: going to 2.11.8 ?
    version := "0.1"
  )),
  //libraryDependencies := scalaTest % Test
  libraryDependencies += "org.apache.spark" %% "spark-core" % "2.1.0"
)
  .dependsOn( scalautils ) // Depends of the Project which is from scala-utils git
lazy val scalautils = RootProject ( uri( "https://lipm-gitlab.toulouse.inra.fr/llegrand/scala-utils.git" ) )

