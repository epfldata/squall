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
import java.util.ArrayList;
import java.util.Arrays;
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
                new ColumnNameType("NAME fixed", sc),
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
    }

     public TPCH_Schema(){
       this(1); // default scalling factor is 1GB
    }

}
