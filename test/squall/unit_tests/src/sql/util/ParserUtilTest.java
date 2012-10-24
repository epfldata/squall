package sql.util;

import net.sf.jsqlparser.schema.Column;
import static org.junit.Assert.assertEquals;
import org.junit.Test;

/**
 *
 * @author vitorovi
 */
public class ParserUtilTest {
    
    public ParserUtilTest() {
    }
    
    @Test
    public void testNameToColumn() {
        String name = "N1.NATIONNAME";
        Column column = ParserUtil.nameToColumn(name);    
        assertEquals(name, ParserUtil.getStringExpr(column));
    }
}
