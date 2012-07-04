package sql.schema;

import java.util.Arrays;
import java.util.List;
import plan_runner.conversion.IntegerConversion;
import plan_runner.conversion.TypeConversion;


public class RST_Schema extends Schema {
    private static final TypeConversion ic = new IntegerConversion();

    public static final List<ColumnNameType> R = Arrays.asList(
            new ColumnNameType("A", ic),
            new ColumnNameType("B", ic)
            );

    public static final List<ColumnNameType> S = Arrays.asList(
            new ColumnNameType("B", ic),
            new ColumnNameType("C", ic)
            );

    public static final List<ColumnNameType> T = Arrays.asList(
            new ColumnNameType("C", ic),
            new ColumnNameType("D", ic)
            );

    public RST_Schema(){
        //tables
        tables.put("R", R);
        tables.put("S", S);
        tables.put("T", T);

        //table sizes
        tableSize.put("R", 10000);
        tableSize.put("S", 10000);
        tableSize.put("T", 10000);
    }

}