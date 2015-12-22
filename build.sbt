lazy val root = (project in file(".")).
    settings(
		organization := "com.res",
        name := "janus",
        version := "1.0",
        scalaVersion := "2.10.5",
        libraryDependencies  ++= Seq(
            "org.scalanlp" %% "breeze" % "0.11.2",
            // native libraries are not included by default. add this if you want them (as of 0.7)
            // native libraries greatly improve performance, but increase jar sizes.
            "org.scalanlp" %% "breeze-natives" % "0.11.2",
            "org.apache.spark" %% "spark-core" % "1.5.2",
            "org.apache.spark" %% "spark-graphx" % "1.5.2",
            // test libraries
			"com.novocode" % "junit-interface" % "0.11" % "test"
        ),
        resolvers ++= Seq(
            // other resolvers here
            "Sonatype Releases" at "https://oss.sonatype.org/content/repositories/releases/"
        )
        // , testOptions += Tests.Argument(TestFrameworks.JUnit, "-q", "-v")
        ,javaOptions += "-Xss16m -Dsun.io.serialization.extendedDebugInfo=true"
    )