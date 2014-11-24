#Squall
Squall is an online query processing engine built on top of [Storm](https://github.com/nathanmarz/storm). Similar to how Hive provides SQL syntax on top of Hadoop for doing batch processing, Squall executes SQL queries on top of Storm for doing online processing. Squall supports a wide class of SQL analytics ranging from simple aggregations to more advanced UDF join predicates and adaptive rebalancing of load. It is being actively developed by several contributors from the [EPFL DATA](http://data.epfl.ch/) lab. Squall is undergoing a continuous process of development, currently it supports the following:

- [x] SQL (Select-Project-Join) query processing over continuous streams of data.
- [x] Full fledged & full-history stateful computation essential for approximate query processing, e.g. [Online Aggregation](http://en.wikipedia.org/wiki/Online_aggregation).
- [x] Time based Window Semantics for infinite data streams.
- [x] Theta Joins: complex join predicates, including inequality, band, and arbitrary UDF join predicates. This gives a more comprehensive support and flexibility to data analytics. For example, [Hive plans](https://cwiki.apache.org/confluence/display/Hive/Theta+Join) to support theta joins in response to user requests.
- [x] Continuous load balance and adaptation to data skew.
- [x] Usage: An API for arbitrary SQL query processing or a frontend query processor that parses SQL to a storm topology.
- [x] Throughput rates of upto Millions of tuples/second and latencies of milliseconds on a 16 machine cluster. Scalable to large cluster settings. Further details can be found in this [whitepaper](http://www.vldb.org/pvldb/vol7/p441-elseidy.pdf).
- [x] Out-of-Core Processing: Can operate efficiently under limited memory resources through efficient disk based datastructures and indexes.
- [ ] Elasticity: Scaling out according to the load.

### Example:
Consider the following SQL query:
```sql
SELECT C_MKTSEGMENT, COUNT(O_ORDERKEY)
FROM CUSTOMER join ORDERS on C_CUSTKEY = O_CUSTKEY
GROUP BY C_MKTSEGMENT
```
Through the Squall API, the online distributed query plan ([full code](https://github.com/epfldata/squall/blob/master/src/plan_runner/query_plans/HyracksPlan.java)) can be simply formulated as follows:

```java
ProjectOperator projectionCustomer = new ProjectOperator(new int[]{0, 6});
ArrayList<Integer> hashCustomer = new ArrayList<Integer>(Arrays.asList(0));
DataSourceComponent relationCustomer = new DataSourceComponent("CUSTOMER",dataPath + "customer" + extension,
                                                              _queryPlan).addOperator(projectionCustomer)
                                                              .setHashIndexes(hashCustomer);

ProjectOperator projectionOrders = new ProjectOperator(new int[]{1});
ArrayList<Integer> hashOrders = new ArrayList<Integer>(Arrays.asList(0));
DataSourceComponent relationOrders = new DataSourceComponent("ORDERS",dataPath + "orders" + extension,
                                                            _queryPlan).addOperator(projectionOrders)
                                                            .setHashIndexes(hashOrders);

ArrayList<Integer> hashIndexes = new ArrayList<Integer>(Arrays.asList(1));
EquiJoinComponent CUSTOMER_ORDERSjoin = new EquiJoinComponent(relationCustomer,relationOrders,
                                                             _queryPlan).setHashIndexes(hashIndexes);

AggregateCountOperator agg = new AggregateCountOperator().setGroupByColumns(Arrays.asList(1));
OperatorComponent oc = new OperatorComponent(CUSTOMER_ORDERSjoin, "COUNTAGG", _queryPlan).setOperator(agg);
```


### Documentation
Detailed documentation can be found on the [Squall wiki](http://github.com/epfldata/squall/wiki).

### Contributing to Squall
We'd love to have your help in making Squall better. If you're interested, please communicate with us your suggestions and get your name to the [Contributors](https://github.com/epfldata/squall/wiki/Contributors) list.

### License
Squall is licensed under [Apache License v2.0](http://www.apache.org/licenses/LICENSE-2.0.html).
