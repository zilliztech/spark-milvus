addSbtPlugin("com.thesamet" % "sbt-protoc" % "1.0.7")
addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "2.1.1")
addSbtPlugin("org.xerial.sbt" % "sbt-sonatype" % "3.12.2")
addSbtPlugin("com.github.sbt" % "sbt-pgp" % "2.1.2")
addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.5.2")
// Generates the CALL grammar's parser once per Spark line, at the line's
// antlr version (decision 17).
addSbtPlugin("com.simplytyped" % "sbt-antlr4" % "0.8.3")

libraryDependencies += "com.thesamet.scalapb" %% "compilerplugin" % "0.11.3"
