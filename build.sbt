name := "Test Scala Project"

version := "1.0"

scalaVersion := "2.11.7"

libraryDependencies ++= Seq (
          "org.apache.spark" %% "spark-core" % "2.1.0" withSources() withJavadoc(),
          "org.apache.spark" %% "spark-sql" % "2.1.0" withSources() withJavadoc(),
          //"com.databricks" %% "spark-xml" % "0.4.1" withSources() withJavadoc(),          
          "org.mockito" % "mockito-core" % "1.9.5" % "test" withSources() withJavadoc(),
          "junit" % "junit" % "4.11" % "test" withSources() withJavadoc(),
          "com.databricks" %% "spark-xml" % "0.4.1" withSources() withJavadoc(),
          "org.json" % "json" % "20160810"
          //"com.mapr.db" % "maprdb" % "5.1.0-mapr" withSources() withJavadoc()
          //"com.databricks" %% "spark-csv" % "1.4.0"      
)