# grafalgo

Grafalgo is a graph processor that provides Distributed Graph Analytics (DGA) written for Big graph on top of a Spark cluster.

Currently, Grafalgo supports the following analytics:
- Louvain Modularity 
- Page Rank
- Eigen Vector Centrality
- Degree
- High Betweenness Set Extraction
- Giant Connected Components

## Features
- Algorithms that **scale**.
- Grafalgo can support any kind of graph (directed, undirected, bipartite etc..). Just implement your own custom Graph such as the Author or Ego network example. 
- Graph can be imported and exported in **JSON, CSV and even Gephi format**.
- Compatible with **Elastic search index**. 

## Getting started

* Configure your network conf file

```
network.type=AuthorNetwork
network.connectionString="myelasticsearch/index;?q=*:*"
network.outputPath="hdfs://master-1.local:8020/user/mypath/"
network.partition.number=10
network.sampleData=false
network.metrics="MODULARITY, EIGENCENTRALITY, WEIGHTEDDEGREES"
```
* Submit your spark job

```
spark-submit --class com.grafalgo.graph.spi.NetworkJobLauncher --master spark://master-1.local:7077 --conf spark.es.nodes=<elastic_search_node>  --conf spark.config.file=./authnetwork.conf --conf spark.eventLog.enabled=false ../my-jar-job.jar
```












