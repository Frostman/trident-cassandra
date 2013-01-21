# trident-cassandra

Cassandra state implementation for Twitter Storm Trident API.

All 3 state types are working good (non-transactional, opaque-transactional and transactional).

# Maven

https://clojars.org/trident-cassandra

```xml
<dependency>
  <groupId>trident-cassandra</groupId>
  <artifactId>trident-cassandra</artifactId>
  <!-- for storm 0.8.1 use wip1 version -->
  <version>0.0.1-wip2</version>
</dependency>
```

## Usage

Use static factory methods in `trident.cassandra.CassandraState`.

Word count topology sample:

```java
StateFactory cassandraStateFactory = CassandraState.transactional("localhost");

TridentState wordCounts = topology.newStream("spout1", spout)
                                  .each(new Fields("sentence"), new Split(), new Fields("word"))
                                  .groupBy(new Fields("word"))
                                  .persistentAggregate(cassandraStateFactory, new Count(), new Fields("count"))
                                  .parallelismHint(6);
```

CassandraState.Options, that could be passed to factory methods:

```java
public static class Options<T> implements Serializable {
    public int localCacheSize = 5000;
    public String globalKey = "$__GLOBAL_KEY__$";
    public Serializer<T> serializer = null;
    public String clusterName = "trident-state";
    public int replicationFactor = 1;
    public String keyspace = "test";
    public String columnFamily = "column_family";
    public String rowKey = "row_key";
}
```

## License

Copyright (C) 2012 Sergey Lukjanov

Distributed under the Eclipse Public License, the same as Clojure.

(License is under discussions, it may be changed soon)
