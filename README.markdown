#Patho Structure of the project

The Menthor project is structured into several sub-projects:

* `libmenthor` contains the main framework source files
* `libmenthor-akka` contains the main framework source files using the Akka
  actors
* `examples` contains the sub-projects for the various examples using Menthor
  (with or without Akka actors)
    - `clustering` contains the source files of the hierarchical clustering
      example
    - `pagerank` contains the source files of the Wikipedia page rank example
    - `pagerank-akka` contains the source files of the Wikipedia page rank
      example using Akka actors
    - `sssp` contains the source files of the single source shortest paths
      example

With an interactive SBT session, you can list the projects by using the
`projects` command and switch to a project by using the `project <name>`
command.

# Building the framework/examples using SBT

To download the external dependencies of the framework and the examples, run
the following command from the project's root directory:

    $ sbt update

You can build the complete project, including the examples by running the
command:

    $ sbt compile

SBT will put the generated class files in the
`<subproject>/target/scala_2.8.1/classes` directories.

If you only want to launch a command for a specific sub-project, run from an
interactive SBT session:

    > project <name>
    > <command>

This is the same as running (spaces inside name and command must be escaped):

    $ sbt project <name> <command>

# Running the examples

To run an example, you first have to switch to the project from an interactive
SBT session by using the command given above. Afterward, you just need to run
the following command:

    > run <arguments>

## Pagerank

The Pagerank application takes 3 arguments:

1. The number of iterations
2. The path of the directory containing the data
3. The number of pages (?)

For example, you can run Pagerank with the following commands and get the
given result:

    > project pagerank
    > run 10 data/ 2000
    …
    [info] == Wikipedia Pagerank / run ==
    [info] Running processing.parallel.PageRank 10 data/ 2000
    Reading wikipedia graph from file...
    #vertices: 14672
    Building page title map...
    A_rod has rank 9.53280821060678E-4
    A_roads_in_Zone_9_of_the_Great_Britain_numbering_scheme has rank 8.582660229728799E-4
    A_roads_in_Zone_8_of_the_Great_Britain_numbering_scheme has rank 7.289154217330095E-4
    no_title[1480822] has rank 2.3315882292802617E-4
    ABQ-2 has rank 2.2965968836554816E-4
    no_title[505945] has rank 1.95595527320922E-4
    American_Broadcasting_Company_(ABC) has rank 1.7042608200493645E-4
    Actor-Lab has rank 1.3479787359654108E-4
    ABX has rank 1.3406944845278912E-4
    no_title[644327] has rank 1.1435584816855263E-4
    [info] == Wikipedia Pagerank / run ==
    [success] Successful.

## Pagerank (Akka)

    $ mkdir results
    $ sbt 'project pagerank-akka' 'eval System.setProperty("akka.mode", "serv1")' 'run-main menthor.cluster.ClusterService'
    $ sbt 'project pagerank-akka' 'eval System.setProperty("akka.mode", "serv2")' 'run-main menthor.cluster.ClusterService'
    $ sbt 'project pagerank-akka' 'run data/links-sorted-small.txt data/titles-sorted-small.txt results/ <pages> [iterations]'

## Hierarchical Clustering

The Hierarchical Clustering application takes 4 arguments:

1. Whether to run the parallel ("par") or sequential implementation
2. Enable or not the debug messages
3. If running the parallel implementation, the path of the data file
4. If running the parallel implementation, the number of lines to use as input
   from the data file

You use the following commands to run the application:

    > project hierarchical-clustering
    > run <par|seq> <true|false> <data file> <input size>

For example

    > run par false data/blogdata.txt 100

## Single Source Shortest Paths

The SSSP application takes 1 argument, the number of nodes in a generated
binary tree with non-weighted directed edges and uses the root of the tree as
the source.

    > project sssp
    > run <number of nodes>
