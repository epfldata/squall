package sql.schema;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import org.junit.Test;
import plan_runner.conversion.*;
import sql.util.ParserUtil;

/**
 *
 * @author vitorovi
 */
public class SchemaTest {
    private Schema _schema;
    
    private TypeConversion _lc = new LongConversion();
    private TypeConversion _sc = new StringConversion();
    private TypeConversion _dbc = new DoubleConversion();
    private DateConversion _dtc = new DateConversion();
    
    public SchemaTest() {
        String path = "../test/squall/schemas/tpch.txt";
        double scallingFactor = 1;
        _schema = new Schema(path, scallingFactor);
    }

    @Test
    public void testGetTableSchema() {
        List<ColumnNameType> cnts = _schema.getTableSchema("NATION");
        
        List<ColumnNameType> expected = new ArrayList<ColumnNameType>();
        expected.add(new ColumnNameType("NATIONKEY", _lc));
        expected.add(new ColumnNameType("NAME", _sc));
        expected.add(new ColumnNameType("REGIONKEY", _lc));
        expected.add(new ColumnNameType("COMMENT", _sc));
        
        assertEquals(expected, cnts);
    }

    @Test
    public void testGetType() {
        TypeConversion tc = _schema.getType("LINEITEM.EXTENDEDPRICE");
        assertEquals(_dbc.toString(), tc.toString());
    }

    @Test
    public void testContains() {
        assertTrue(_schema.contains("ORDERS.CUSTKEY"));
        assertFalse(_schema.contains("ORDERS.PARTKEY"));
    }

    @Test
    public void testGetTableSize() {
        assertEquals(6000000, _schema.getTableSize("LINEITEM"));
        assertEquals(5, _schema.getTableSize("REGION"));
    }

    @Test
    public void testGetNumDistinctValues() {
        assertEquals(25, _schema.getNumDistinctValues("SUPPLIER.NATIONKEY"));
    }

    @Test(expected=RuntimeException.class)
    public void testGetNumDistinctValues2() {
        _schema.getNumDistinctValues("CUSTOMER.PHONE");
    }    
    
    @Test
    public void testGetRange() {
        Date min = (Date) _schema.getRange("LINEITEM.SHIPDATE").getMin();
        assertEquals(_dtc.fromString("1992-01-01"), min);
        
        Date max = (Date) _schema.getRange("ORDERS.ORDERDATE").getMax();
        assertEquals(_dtc.fromString("1998-08-02"), max); 
    }

    @Test
    public void testGetRatio() {
        assertEquals(4.0, _schema.getRatio("ORDERS", "LINEITEM"), 0);
    }
}
