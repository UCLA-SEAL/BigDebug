
# BigDebug: Debugging Primitives for Interactive Big Data Processing in Spark

## Summary of BigDebug 
Developers use cloud computing platforms to process a large quantity of data in parallel when developing big data analytics. Debugging the massive parallel computations that run in today's datacenters is time consuming and error-prone. To address this challenge, we design a set of interactive, real-time debugging primitives for big data processing in Apache Spark, the next generation data-intensive scalable cloud computing platform. This requires rethinking the notion of step-through debugging in a traditional debugger such as gdb, because pausing the entire computation across distributed worker nodes causes significant delay and naively inspecting millions of records using a watchpoint is too time consuming for an end user.
First, BigDebug's simulated breakpoints and on-demand watchpoints allow users to selectively examine distributed, intermediate data on the cloud with little overhead. Second, a user can also pinpoint a crash-inducing record and selectively resume relevant sub-computations after a quick fix. Third, a user can determine the root causes of errors (or delays) at the level of individual records through a fine-grained data provenance capability. Our evaluation shows that BigDebug scales to terabytes and its record-level tracing incurs less than 25% overhead on average. It determines crash culprits orders of magnitude more accurately and provides up to 100% time saving compared to the baseline replay debugger. The results show that BigDebug supports debugging at interactive speeds with minimal performance impact.

## Team 

This project was done in collaboration with Professor Condie, Kim and Millstein's group at UCLA. If you encounter any problems, please open an issue or feel free to contact us:

[Matteo Interlandi](https://interesaaat.github.io): was a postdoc at UCLA and now a Senior Scientist at Microsoft; 

[Muhammad Ali Gulzar](https://people.cs.vt.edu/~gulzar/): was a PhD student at UCLA, now an Assistant Professor at Virginia Tech, gulzar@cs.vt.edu;

[Tyson Condie](https://samueli.ucla.edu/people/tyson-condie/): was an Assistant Professor at UCLA, now at Microsoft 

[Miryung Kim](http://web.cs.ucla.edu/~miryung/): Professor at UCLA, miryung@cs.ucla.edu;


## How to cite 
Please refer to our ICSE'16 paper, [BigDebug: debugging primitives for interactive big data processing in spark
](http://web.cs.ucla.edu/~miryung/Publications/icse2016-gulzar-bigdebug.pdf) for more details. 
### Bibtex  
@inproceedings{10.1145/2884781.2884813,
author = {Gulzar, Muhammad Ali and Interlandi, Matteo and Yoo, Seunghyun and Tetali, Sai Deep and Condie, Tyson and Millstein, Todd and Kim, Miryung},
title = {BigDebug: Debugging Primitives for Interactive Big Data Processing in Spark},
year = {2016},
isbn = {9781450339001},
publisher = {Association for Computing Machinery},
address = {New York, NY, USA},
url = {https://doi.org/10.1145/2884781.2884813},
doi = {10.1145/2884781.2884813},
booktitle = {Proceedings of the 38th International Conference on Software Engineering},
pages = {784–795},
numpages = {12},
keywords = {interactive tools, debugging, big data analytics, data-intensive scalable computing (DISC), fault localization and recovery},
location = {Austin, Texas},
series = {ICSE '16}
}
[DOI Link](https://doi.org/10.1145/2884781.2884813)

This branch contains the source code for [BigDebug: Debugging Primitives for Interactive Big Data Processing in Spark at ICSE 2016](http://web.cs.ucla.edu/~miryung/Publications/icse2016-gulzar-bigdebug.pdf)

The demo papers of this work are available here [FSE 2016](http://web.cs.ucla.edu/~miryung/Publications/fse2016demo-bigdebug.pdf)
and [SIGMOD 2017](http://web.cs.ucla.edu/~miryung/Publications/sigmod2017-bigdebugdemo.pdf)

The demo videos are available here [Spark Summit 2017](https://www.youtube.com/watch?v=_HR3VJ2dPbE) and [Tutorial](https://www.youtube.com/watch?v=aZ91EyC5-Yc)

The preliminary usage instructions are available at the BigDebug [website](https://sites.google.com/site/sparkbigdebug/)


# Apache Spark

Spark is a fast and general cluster computing system for Big Data. It provides
high-level APIs in Scala, Java, Python, and R, and an optimized engine that
supports general computation graphs for data analysis. It also supports a
rich set of higher-level tools including Spark SQL for SQL and DataFrames,
MLlib for machine learning, GraphX for graph processing,
and Spark Streaming for stream processing.

<http://spark.apache.org/>


## Online Documentation

You can find the latest Spark documentation, including a programming
guide, on the [project web page](http://spark.apache.org/documentation.html).
This README file only contains basic setup instructions.

## Building Spark

Spark is built using [Apache Maven](http://maven.apache.org/).
To build Spark and its example programs, run:

    build/mvn -DskipTests clean package

(You do not need to do this if you downloaded a pre-built package.)

You can build Spark using more than one thread by using the -T option with Maven, see ["Parallel builds in Maven 3"](https://cwiki.apache.org/confluence/display/MAVEN/Parallel+builds+in+Maven+3).
More detailed documentation is available from the project site, at
["Building Spark"](http://spark.apache.org/docs/latest/building-spark.html).

For general development tips, including info on developing Spark using an IDE, see 
[http://spark.apache.org/developer-tools.html](the Useful Developer Tools page).

## Interactive Scala Shell

The easiest way to start using Spark is through the Scala shell:

    ./bin/spark-shell

Try the following command, which should return 1000:

    scala> sc.parallelize(1 to 1000).count()

## Interactive Python Shell

Alternatively, if you prefer Python, you can use the Python shell:

    ./bin/pyspark

And run the following command, which should also return 1000:

    >>> sc.parallelize(range(1000)).count()

## Example Programs

Spark also comes with several sample programs in the `examples` directory.
To run one of them, use `./bin/run-example <class> [params]`. For example:

    ./bin/run-example SparkPi

will run the Pi example locally.

You can set the MASTER environment variable when running examples to submit
examples to a cluster. This can be a mesos:// or spark:// URL,
"yarn" to run on YARN, and "local" to run
locally with one thread, or "local[N]" to run locally with N threads. You
can also use an abbreviated class name if the class is in the `examples`
package. For instance:

    MASTER=spark://host:7077 ./bin/run-example SparkPi

Many of the example programs print usage help if no params are given.

## Running Tests

Testing first requires [building Spark](#building-spark). Once Spark is built, tests
can be run using:

    ./dev/run-tests

Please see the guidance on how to
[run tests for a module, or individual tests](http://spark.apache.org/developer-tools.html#individual-tests).

## A Note About Hadoop Versions

Spark uses the Hadoop core library to talk to HDFS and other Hadoop-supported
storage systems. Because the protocols have changed in different versions of
Hadoop, you must build Spark against the same version that your cluster runs.

Please refer to the build documentation at
["Specifying the Hadoop Version"](http://spark.apache.org/docs/latest/building-spark.html#specifying-the-hadoop-version)
for detailed guidance on building for a particular distribution of Hadoop, including
building for particular Hive and Hive Thriftserver distributions.

## Configuration

Please refer to the [Configuration Guide](http://spark.apache.org/docs/latest/configuration.html)
in the online documentation for an overview on how to configure Spark.

## Contributing

Please review the [Contribution to Spark guide](http://spark.apache.org/contributing.html)
for information on how to get started contributing to the project.
