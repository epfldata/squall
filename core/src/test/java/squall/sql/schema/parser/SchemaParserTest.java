package sql.schema.parser;

import java.util.Map;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import ch.epfl.data.plan_runner.utilities.MyUtilities;
import ch.epfl.data.sql.schema.parser.SchemaParser;
import ch.epfl.data.sql.schema.parser.SchemaParser.TableInfo;

/**
 *
 * @author vitorovi
 */
public class SchemaParserTest {
    
    private static final String SCHEMA_PATH = SchemaParserTest.class.getResource("/squall/schemas/").getPath();
    private static final String SCHEMA_EXT = ".txt";
    private static final String RESULT_PATH = SchemaParserTest.class.getResource("/squall/printouts/schemas/").getPath();
    private static final String RESULT_EXT = ".result";
    
    public SchemaParserTest() {
    }

    @Test
    public void testExample1() throws Exception {
        String fileName = "Ex1";
        double scallingFactor = 10;
        
        check(fileName, scallingFactor);
    }

    @Test
    public void testExample2() throws Exception {
        String fileName = "Ex2";
        double scallingFactor = 0.1;
        
        check(fileName, scallingFactor);
    }

    @Test
    public void testTPCH() throws Exception {
        String fileName = "tpch";
        double scallingFactor = 0.1;
        
        check(fileName, scallingFactor);
    }    
    
    private void check(String fileName, double scallingFactor) throws Exception {
        Map<String, TableInfo> tables = SchemaParser.getSchemaInfo(SCHEMA_PATH + fileName + SCHEMA_EXT, scallingFactor);
        String result = SchemaParser.getParsedString(tables);
        //System.out.print(result);
        
        String expected = MyUtilities.readFile(RESULT_PATH + "/" + fileName + RESULT_EXT);
        assertEquals(expected, result);
    }
}
