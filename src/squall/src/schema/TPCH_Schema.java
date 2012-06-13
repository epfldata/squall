/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package schema;

import conversion.DateConversion;
import conversion.DoubleConversion;
import conversion.LongConversion;
import conversion.StringConversion;
import conversion.TypeConversion;
import expressions.DateSum;
import expressions.ValueSpecification;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

/*
 * This class is used only from parser. Because of JSQL limitations, wherever int is required, we use long.
 */
public class TPCH_Schema extends Schema {
    private static final TypeConversion lc = new LongConversion();
    private static final TypeConversion dbc = new DoubleConversion();
    private static final TypeConversion sc = new StringConversion();
    private static final TypeConversion dtc = new DateConversion();

    public static final List<ColumnNameType> orders = Arrays.asList(
                new ColumnNameType("ORDERKEY", lc),
                new ColumnNameType("CUSTKEY", lc),
                new ColumnNameType("ORDERSTATUS", sc),
                new ColumnNameType("TOTALPRICE", dbc),
                new ColumnNameType("ORDERDATE", dtc),
                new ColumnNameType("ORDERPRIORITY", sc),
                new ColumnNameType("CLERK", sc),
                new ColumnNameType("SHIPPRIORITY", lc),
                new ColumnNameType("COMMENT", sc)
                );

    public static final List<ColumnNameType> lineitem = Arrays.asList(
                new ColumnNameType("ORDERKEY", lc),
                new ColumnNameType("PARTKEY", lc),
                new ColumnNameType("SUPPKEY", lc),
                new ColumnNameType("LINENUMBER", lc),
                new ColumnNameType("QUANTITY", dbc),
                new ColumnNameType("EXTENDEDPRICE", dbc),
                new ColumnNameType("DISCOUNT", dbc),
                new ColumnNameType("TAX", dbc),
                new ColumnNameType("RETURNFLAG", sc),
                new ColumnNameType("LINESTATUS", sc),
                new ColumnNameType("SHIPDATE", dtc),
                new ColumnNameType("COMMITDATE", dtc),
                new ColumnNameType("RECEIPTDATE", dtc),
                new ColumnNameType("SHIPINSTRUCT", sc),
                new ColumnNameType("SHIPMODE", sc),
                new ColumnNameType("COMMENT", sc)
                );

    public static final List<ColumnNameType> customer = Arrays.asList(
                new ColumnNameType("CUSTKEY", lc),
                new ColumnNameType("NAME", sc),
                new ColumnNameType("ADDRESS", sc),
                new ColumnNameType("NATIONKEY", lc),
                new ColumnNameType("PHONE", sc),
                new ColumnNameType("ACCTBAL", dbc),
                new ColumnNameType("MKTSEGMENT", sc),
                new ColumnNameType("COMMENT", sc)
                );

    public static final List<ColumnNameType> nation = Arrays.asList(
                new ColumnNameType("NATIONKEY", lc),
                new ColumnNameType("NAME", sc),
                new ColumnNameType("REGIONKEY", lc),
                new ColumnNameType("COMMENT", sc)
                );

    public static final List<ColumnNameType> supplier = Arrays.asList(
                new ColumnNameType("SUPPKEY", lc),
                new ColumnNameType("NAME", sc),
                new ColumnNameType("ADDRESS", sc),
                new ColumnNameType("NATIONKEY", lc),
                new ColumnNameType("PHONE", sc),
                new ColumnNameType("ACCTBAL", dbc),
                new ColumnNameType("COMMENT", sc)
                );

    public static final List<ColumnNameType> region = Arrays.asList(
                new ColumnNameType("REGIONKEY", lc),
                new ColumnNameType("NAME", sc),
                new ColumnNameType("COMMENT", sc)
                );

    public static final List<ColumnNameType> part = Arrays.asList(
                new ColumnNameType("PARTKEY", lc),
                new ColumnNameType("NAME", sc),
                new ColumnNameType("MFGR", sc),
                new ColumnNameType("BRAND", sc),
                new ColumnNameType("TYPE", sc),
                new ColumnNameType("SIZE", lc),
                new ColumnNameType("CONTAINER", sc),
                new ColumnNameType("RETAILPRICE", dbc),
                new ColumnNameType("COMMENT", sc)
                );

    public static final List<ColumnNameType> partsupp = Arrays.asList(
                new ColumnNameType("PARTKEY", lc),
                new ColumnNameType("SUPPKEY", lc),
                new ColumnNameType("AVAILQTY", lc),
                new ColumnNameType("SUPPLYCOST", dbc),
                new ColumnNameType("COMMENT", sc)
                );

    public TPCH_Schema(double scallingFactor){
        //tables
        tables.put("ORDERS", orders);
        tables.put("LINEITEM", lineitem);
        tables.put("CUSTOMER", customer);
        tables.put("NATION", nation);
        tables.put("SUPPLIER", supplier);
        tables.put("REGION", region);
        tables.put("PART", part);
        tables.put("PARTSUPP", partsupp);

        //table sizes
        tableSize.put("REGION", 5);
        tableSize.put("NATION", 25);
        tableSize.put("SUPPLIER", (int)(10000 * scallingFactor));
        tableSize.put("CUSTOMER", (int)(150000 * scallingFactor));
        tableSize.put("PART", (int)(200000 * scallingFactor));
        tableSize.put("PARTSUPP", (int)(800000 * scallingFactor));
        tableSize.put("ORDERS", (int)(1500000 * scallingFactor));
        tableSize.put("LINEITEM", (int)(6000000 * scallingFactor));

        //columnDistinctValues
        _columnDistinctValues.put("ORDERS.ORDERKEY", (int)(scallingFactor * 1500000 * 4));
        _columnDistinctValues.put("ORDERS.CUSTKEY", (int)(scallingFactor * 150000));
        _columnDistinctValues.put("ORDERS.ORDERSTATUS", 3);
        _columnDistinctValues.put("ORDERS.ORDERDATE", 7 * 364 - 151);
        _columnDistinctValues.put("ORDERS.ORDERPRIORITY", 5);
        _columnDistinctValues.put("ORDERS.CLERK", (int)(scallingFactor * 1000));
        _columnDistinctValues.put("ORDERS.SHIPPRIORITY", 1);

        _columnDistinctValues.put("LINEITEM.ORDERKEY", (int)(scallingFactor * 1500000 * 4));
        _columnDistinctValues.put("LINEITEM.PARTKEY", (int)(scallingFactor * 200000));
        _columnDistinctValues.put("LINEITEM.SUPPKEY", (int)(scallingFactor * 10000));
        _columnDistinctValues.put("LINEITEM.QUANTITY", 50);
        _columnDistinctValues.put("LINEITEM.RETURNFLAG", 3); //need histogram, skew
        _columnDistinctValues.put("LINEITEM.LINESTATUS", 2); //need histogram, skew
        _columnDistinctValues.put("LINEITEM.SHIPDATE", 7 * 364 - 151 + 121);
        _columnDistinctValues.put("LINEITEM.COMMITDATE", 7 * 364 - 151 + 121 - 30+ 90);
        _columnDistinctValues.put("LINEITEM.RECEIPTDATE", 7 * 364);
        _columnDistinctValues.put("LINEITEM.SHIPINSTRUCT", 4);
        _columnDistinctValues.put("LINEITEM.SHIPMODE", 7);

        _columnDistinctValues.put("CUSTOMER.CUSTKEY", (int)(scallingFactor * 150000));
        _columnDistinctValues.put("CUSTOMER.NAME", (int)(scallingFactor * 150000));
        _columnDistinctValues.put("CUSTOMER.NATIONKEY", 25);
        _columnDistinctValues.put("CUSTOMER.MKTSEGMENT", 5);

        _columnDistinctValues.put("NATION.NATIONKEY", 25);
        _columnDistinctValues.put("NATION.NAME", 25);
        _columnDistinctValues.put("NATION.REGIONKEY", 5);

        _columnDistinctValues.put("SUPPLIER.SUPPKEY", (int)(scallingFactor * 10000));
        _columnDistinctValues.put("SUPPLIER.NAME", (int)(scallingFactor * 10000));
        _columnDistinctValues.put("SUPPLIER.NATIONKEY", 25);

        _columnDistinctValues.put("REGION.REGIONKEY", 5);
        _columnDistinctValues.put("REGION.NAME", 5);
        
        _columnDistinctValues.put("PART.PARTKEY", (int)(scallingFactor * 200000));
        _columnDistinctValues.put("PART.MFGR", 5);
        _columnDistinctValues.put("PART.BRAND", 5);
        _columnDistinctValues.put("PART.TYPE", 150);
        _columnDistinctValues.put("PART.SIZE", 50);
        _columnDistinctValues.put("PART.CONTAINER", 40);

        _columnDistinctValues.put("PARTSUPP.PARTKEY", (int)(scallingFactor * 200000));
        _columnDistinctValues.put("PARTSUPP.SUPPKEY", (int)(scallingFactor * 10000));
        _columnDistinctValues.put("PARTSUPP.AVAILQTY", 10000);

        //columnRanges
        TypeConversion<Date> _dateConv = new DateConversion();
        Date START_DATE = _dateConv.fromString("1992-01-01");
        Date END_DATE = _dateConv.fromString("1998-12-31");

        Date orderDateEnd = new DateSum(new ValueSpecification(_dateConv, END_DATE), Calendar.DATE, -151).eval(null);
        _columnRanges.put("ORDERS.ORDERDATE", new Range(START_DATE, orderDateEnd));

        Date lineitemShipDateEnd = new DateSum(new ValueSpecification(_dateConv, END_DATE), Calendar.DATE, -30).eval(null);
        _columnRanges.put("LINEITEM.SHIPDATE", new Range(START_DATE, lineitemShipDateEnd));

        Date lineitemCommitDateEnd = new DateSum(new ValueSpecification(_dateConv, END_DATE), Calendar.DATE, -61).eval(null);
        _columnRanges.put("LINEITEM.COMMITDATE", new Range(START_DATE, lineitemCommitDateEnd));

        Date lineitemReceiptDateEnd = new DateSum(new ValueSpecification(_dateConv, END_DATE), Calendar.DATE, -121).eval(null);
        _columnRanges.put("LINEITEM.RECEIPTDATE", new Range(START_DATE, lineitemReceiptDateEnd));

    }

     public TPCH_Schema(){
       this(1); // default scalling factor is 1GB
    }

}
