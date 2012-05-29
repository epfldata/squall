//{d '2001-12-23'} for DateValue
        //DISTINCT is allowed as an aggregation in SELECT clause,
        //  i.e. DISTINCT ON (R.A) R.B
        //  COUNT(DISTINCT expr) is supported with a single expression
        String sql = "SELECT * FROM MY_TABLE1, MY_TABLE2, (SELECT * FROM MY_TABLE3) LEFT OUTER JOIN MY_TABLE4 "+
          " WHERE ID = (SELECT MAX(ID) FROM MY_TABLE5) AND ID2 IN (SELECT * FROM MY_TABLE6)" ;
        sql = "SELECT TABLE1.suma FROM TABLE1, TABLE2, TABLE3 WHERE TABLE1.id = TABLE2.id AND TABLE2.s = TABLE3.s";

        sql = "SELECT R.A*T.D FROM R, S, T WHERE R.B=S.B and S.C=T.C and R.A > 100";
        sql = "SELECT * FROM R inner join S on R.B = S.B";
        sql = "SELECT SUM(S.B * R.A) FROM R inner join S on R.B=S.B inner join T on S.C=T.C WHERE R.A > 100";
        sql = "SELECT COUNT(DISTINCT (R.A)) FROM R inner join S on R.B=S.B inner join T on S.C=T.C WHERE R.A > 100";

        sql = "SELECT N1.NAME, N2.NAME, EXTRACT_YEAR(LINEITEM.SHIPDATE), LINEITEM.EXTENDEDPRICE * (1-LINEITEM.DISCOUNT) "
                + " FROM SUPPLIER "
                + " inner join LINEITEM on SUPPLIER.SUPPKEY = LINEITEM.SUPPKEY "
                + " inner join NATION N1 on SUPPLIER.NATIONKEY = N1.NATIONKEY "
                + " inner join ORDERS on ORDERS.ORDERKEY = LINEITEM.ORDERKEY "
                + " inner join CUSTOMER on CUSTOMER.CUSTKEY = ORDERS.CUSTKEY "
                + " inner join NATION N2 on CUSTOMER.NATIONKEY = N2.NATIONKEY "
                + " WHERE (N1.REGIONKEY<3) and (CUSTOMER.CUSTKEY='ee' OR ORDERS.ORDERKEY>2)";

        sql = "SELECT SUM(LINEITEM.EXTENDEDPRICE) "
                + " FROM SUPPLIER "
                + " inner join LINEITEM on SUPPLIER.SUPPKEY = LINEITEM.SUPPKEY "
                + " inner join NATION N1 on SUPPLIER.NATIONKEY = N1.NATIONKEY "
                + " inner join ORDERS on ORDERS.ORDERKEY = LINEITEM.ORDERKEY "
                + " inner join CUSTOMER on CUSTOMER.CUSTKEY = ORDERS.CUSTKEY "
                + " inner join NATION N2 on CUSTOMER.NATIONKEY = N2.NATIONKEY ";

        sql = "SELECT SUM(ORDERS.ORDERKEY) FROM PART "
                + "inner join LINEITEM on PART.PARTKEY = LINEITEM.PARTKEY "
                + "inner join SUPPLIER on SUPPLIER.SUPPKEY = LINEITEM.SUPPKEY "
                + "inner join NATION N2 on SUPPLIER.NATIONKEY = N2.NATIONKEY "
                + "inner join ORDERS on LINEITEM.ORDERKEY = ORDERS.ORDERKEY ";

         sql = "SELECT N2.NAME, ORDERS.ORDERDATE, SUM(N2.NATIONKEY) "
                 + "FROM NATION N2 inner join ORDERS ON N2.NATIONKEY = ORDERS.ORDERKEY "
                + "GROUP BY N2.NAME, ORDERS.ORDERDATE";  

         sql = "SELECT EXTRACT_YEAR(ORDERS.ORDERDATE), SUM(ORDERS.ORDERKEY) FROM ORDERS";
