// give the user a nice default project!

lazy val root = (project in file(".")).

  settings(
    inThisBuild(List(
      organization := "com.hashmapinc",
      scalaVersion := "2.11.9"
    )),
    name := "telecom-data-analysis",
    version := "0.0.1",

    sparkVersion := "2.3.0",
    sparkComponents := Seq(),

    javacOptions ++= Seq("-source", "1.8", "-target", "1.8"),
    javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled"),
    //scalacOptions ++= Seq("-deprecation", "-unchecked"),
    parallelExecution in Test := false,
    fork := true,

    coverageHighlighting := true,

    //partialUnificationModule := "com.milessabin" % "si2712fix-plugin" % "1.2.0",

    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-streaming" % "2.3.0",
      "org.apache.spark" %% "spark-sql" % "2.3.0",
      "org.apache.spark" %% "spark-mllib" % "2.3.0",
      "org.typelevel" %% "frameless-dataset" % "0.7.0",
      "org.typelevel" %% "cats-core" % "1.6.0",
      "io.monix" %% "monix" % "3.0.0-8084549",
      "org.typelevel" %% "cats-effect" % "1.2.0",

      "org.scalatest" %% "scalatest" % "3.0.1" % "test",
      "org.scalacheck" %% "scalacheck" % "1.13.4" % "test",
      "com.holdenkarau" %% "spark-testing-base" % "2.3.0_0.9.0" % "test" 
    ),

    // uses compile classpath for the run task, including "provided" jar (cf http://stackoverflow.com/a/21803413/3827)
    run in Compile := Defaults.runTask(fullClasspath in Compile, mainClass in (Compile, run), runner in (Compile, run)).evaluated,

    scalacOptions ++= Seq(
      "-deprecation",
      "-unchecked",
      "-language:_",
      "-Ypartial-unification",
      "-Xfatal-warnings"),

    //pomIncludeRepository := { x => false },

   resolvers ++= Seq(
      "sonatype-releases" at "https://oss.sonatype.org/content/repositories/releases/",
      "Typesafe repository" at "http://repo.typesafe.com/typesafe/releases/",
      "Second Typesafe repo" at "http://repo.typesafe.com/typesafe/maven-releases/",
      Resolver.sonatypeRepo("public")
    ),

    addCompilerPlugin("org.spire-math" %% "kind-projector" % "0.9.7"),
    //addCompilerPlugin("org.scalamacros" % "paradise" % "2.1.1" cross CrossVersion.full),

    pomIncludeRepository := { x => false }

    // publish settings
    /*publishTo := {
      val nexus = "https://oss.sonatype.org/"
      if (isSnapshot.value)
        Some("snapshots" at nexus + "content/repositories/snapshots")
      else
        Some("releases"  at nexus + "service/local/staging/deploy/maven2")
    }*/
  )
