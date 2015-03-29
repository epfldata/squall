
<!--
[alt text][logo]
[logo]: https://raw.githubusercontent.com/epfldata/squall/master/resources/graphics/logo.jpg "Logo Title Text 2"
-->

#Squall
Squall is an online query processing engine built on top of [Storm](https://storm.apache.org/). Similar to how Hive provides SQL syntax on top of Hadoop for doing batch processing, Squall executes SQL queries on top of Storm for doing online processing. Squall supports a wide class of SQL analytics ranging from simple aggregations to more advanced UDF join predicates and adaptive rebalancing of load. It is being actively developed by several contributors from the [EPFL DATA](http://data.epfl.ch/) lab. Squall is undergoing a continuous process of development, currently it supports the following:

- [x] SQL (Select-Project-Join) query processing over continuous streams of data.
- [x] Full fledged & full-history stateful computation essential for approximate query processing, e.g. [Online Aggregation](http://en.wikipedia.org/wiki/Online_aggregation).
- [x] Time based Window Semantics for infinite data streams.
- [x] Theta Joins: complex join predicates, including inequality, band, and arbitrary UDF join predicates. This gives a more comprehensive support and flexibility to data analytics. For example, [Hive plans](https://cwiki.apache.org/confluence/display/Hive/Theta+Join) to support theta joins in response to user requests.
- [ ] Continuous load balance and adaptation to data skew.
- [x] Usage: An API for arbitrary SQL query processing or a frontend query processor that parses SQL to a storm topology.
- [x] Throughput rates of upto Millions of tuples/second and latencies of milliseconds on a 16 machine cluster. Scalable to large cluster settings.
- [x] Out-of-Core Processing: Can operate efficiently under limited memory resources through efficient disk based datastructures and indexes.
- [x] Guarantees: At least-once or at most-once semantics. No support for exactly-once semantics yet, however it is planned for.
- [ ] Elasticity: Scaling out according to the load.
- [ ] DashBoard: Integrating support for real time visualizations.

### Example:
Consider the following SQL query:
```sql
SELECT C_MKTSEGMENT, COUNT(O_ORDERKEY)
FROM CUSTOMER join ORDERS on C_CUSTKEY = O_CUSTKEY
GROUP BY C_MKTSEGMENT
```

We provide several interfaces for running this query:

1. A **Declarative** interface that directly parses this SQL query and creates an efficient storm Topology. This module is implicitly equipped with a cost-based optimizer.
2. A **Functional** Scala-interface that leverages the brevity, productivity, convenience, and syntactic sugar of functional programming. For example the previous query is represented ([full code](https://github.com/epfldata/squall/blob/master/frontend/src/main/scala/frontend/functional/scala/queries/ScalaHyracksPlan.scala)) as follows:

```scala
    val customers = Source[customer]("customer").map { t => Tuple2(t._1, t._7) }
    val orders = Source[orders]("orders").map { t => t._2 }
    val join = customers.join(orders)(k1=> k1._1)(k2 => k2) 
    val agg = join.groupByKey(x => 1, k => k._1._2)
    agg.execute(conf)
```


3. An **Imperative** Java-interface that facilitates design and construction of online distributed query plans. For example the previous query is represented ([full code](https://github.com/epfldata/squall/blob/master/core/src/main/java/ch/epfl/data/plan_runner/query_plans/HyracksPlan.java)) as follows:

```java
Component customer = new DataSourceComponent("customer", conf)
                            .add(new ProjectOperator(0, 6));
Component orders = new DataSourceComponent("orders", conf)
                            .add(new ProjectOperator(1));
Component custOrders = new EquiJoinComponent(customer, 0, orders, 0)
                            .add(new AggregateCountOperator(conf).setGroupByColumns(1));
```


Queries are mapped to operator trees in the spirit of the query plans
of relational database systems.
These are are in turn mapped to Storm workers. (There is a parallel
implementation of each operator, so in general an operator is processed
by multiple workers).
Some operations of relational algebra, such as selections and projections,
are quite simple, and assigning them to separate workers is inefficient.
Rather than requiring the predecessor operator to send its output over the
network to the workers implementing these simple operations,
the simple operations can be integrated into the predecessor operators
and postprocess the output there. This is typically also done in
classical relational database systems, but in a distributed environment,
the benefits are even greater.
In the Squall API, query plans are built bottom-up from 
operators (called components or super-operators)
such as data source scans and joins; 
these components can then be extended by postprocessing operators such as
projections.



### Documentation
Detailed documentation can be found on the [Squall wiki](http://github.com/epfldata/squall/wiki).

### Contributing to Squall
We'd love to have your help in making Squall better. If you're interested, please communicate with us your suggestions and get your name to the [Contributors](https://github.com/epfldata/squall/wiki/Contributors) list.

### License
Squall is licensed under [Apache License v2.0](http://www.apache.org/licenses/LICENSE-2.0.html).
