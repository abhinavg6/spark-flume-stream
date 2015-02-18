spark-flume-stream
==================

A spark streaming program to process events from a flume agent and store output data to text files. 
* The [Flume Event Producer](https://github.com/abhinavg6/flume-avro-java-client) reads a csv file and sends US consumer complaints events to a locally setup flume agent. 
* The spark program sources the events from flume agent (as avro sink), transforms the events into a process-able format, maintains a running/rolling list of event counts per product and state, and appends the running counts to a text file specific to a product and state (for every streaming batch window of 2 sec).

For each project, please download latest version of maven to run mvn commands from command-line, or import it as a maven project in your IDE (provided maven plug-in is present). Please run **mvn clean install** and **mvn eclipse:eclipse** if you're running from a command line, and then import the project in your IDE.

Following setup will need to be done on local machine to run these projects:
* Install Apache Flume - [Visit Flume](https://flume.apache.org/download.html)
* Install Spark - [Visit Spark](http://spark.apache.org/docs/latest/index.html) (For mac users, it's also available via brew)
* Install Python - [Visit Python](https://www.python.org/downloads/). Python is required to run Python specific examples provided in Spark distribution.
* Flume agent config - The program runs as a flume avro sink for a configured flume agent. The avro source for the flume agent is the flume event producer. The following configuration for flume agent can be stored at FLUME_HOME/conf/flume-spark-conf.properties:

   - agent.sources = javaavrorpc
   - agent.channels = memoryChannel
   - agent.sinks = sparkstreaming
   - agent.sources.javaavrorpc.type = avro
   - agent.sources.javaavrorpc.bind = localhost
   - agent.sources.javaavrorpc.port = 42222
   - agent.sources.javaavrorpc.channels = memoryChannel
   - agent.sinks.sparkstreaming.type = avro
   - agent.sinks.sparkstreaming.hostname = localhost
   - agent.sinks.sparkstreaming.port = 43333
   - agent.sinks.sparkstreaming.channel = memoryChannel
   - agent.channels.memoryChannel.type = memory
   - agent.channels.memoryChannel.capacity = 10000
   - agent.channels.memoryChannel.transactionCapacity = 1000

* Flume agent startup - From **FLUME_HOME/bin**, try starting the flume agent instance by running **flume-ng agent -n agent -c ../conf -f ../conf/flume-spark-conf.properties**. Check if it's running as a Java process.
* Spark standalone cluster startup - From **SPARK_HOME/sbin**, start the cluster master as a background process by running **start-master.sh**. Check if it's running as a Java process. Go to http://localhost:8080 and check the URL for master. Then from same command line, start one of the worker's daemon as **spark-class org.apache.spark.deploy.worker.Worker \<spark master URL\> -c 1 -m 2G -d \<a directory to store logs\>**. Then from a different command line, start second worker's daemon with the same command. There should be a total of 4 Java processes running now - flume agent, cluster master, and two worker daemons.
* Spark program submission - Once you've done **mvn clean install** and generated the project uber-JAR (including all dependencies), submit the program from command line by running **spark-submit --class \<main class fully qualified name\> --master \<spark master URL\> \<complete path to uber JAR\> \<complete path to a local checkpoint directory\> \<complete path to a local output directory\>**.
    * eg: `--class com.sapient.stream.process.ConsCompEventStream`
* Event submission to Flume agent - From your IDE in project [Flume Event Producer](https://github.com/abhinavg6/flume-avro-java-client), run the main class **FlumeEventSender**. It'll submit all records in the CSV file as events to flume agent. There're 3 files available in resources, and the file name can be changed in class **ConsCompFlumeClient** to send less/more events. The spark program will start processing the flume event stream, and creating output files at the local output directory.
* Program monitoring - In your browser, go to http//localhost:8080. It should show the program as running application. Now you can play around with it.
