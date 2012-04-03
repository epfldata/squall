/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package schema;

import conversion.IntegerConversion;
import conversion.TypeConversion;
import java.util.ArrayList;
import java.util.Arrays;


public class RST_Schema extends Schema {
    private static final TypeConversion ic = new IntegerConversion();

    public static final ArrayList<ColumnNameType> R = new ArrayList<ColumnNameType>(Arrays.asList(
            new ColumnNameType("A", ic),
            new ColumnNameType("B", ic)
            ));

    public static final ArrayList<ColumnNameType> S = new ArrayList<ColumnNameType>(Arrays.asList(
            new ColumnNameType("B", ic),
            new ColumnNameType("C", ic)
            ));

    public static final ArrayList<ColumnNameType> T = new ArrayList<ColumnNameType>(Arrays.asList(
            new ColumnNameType("C", ic),
            new ColumnNameType("D", ic)
            ));

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