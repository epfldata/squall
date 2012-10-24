package estimators;

import java.util.Calendar;
import java.util.Date;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.Expression;
import static org.junit.Assert.assertEquals;
import org.junit.BeforeClass;
import org.junit.Test;
import plan_runner.conversion.DateConversion;
import plan_runner.expressions.DateSum;
import plan_runner.expressions.ValueSpecification;
import sql.estimators.JSQLTypeConverter;

/**
 *
 * @author vitorovi
 */
public class JSQLTypeConverterTest {
    private static JSQLTypeConverter _visitor;
    private static DateConversion _dtc = new DateConversion();
    

    @BeforeClass
    public static void setUpClass() throws Exception {
        _visitor = new JSQLTypeConverter();
    }

    /**
     * Test of visit method, of class JSQLTypeConverter.
     */
    @Test
    public void testVisit_DateValue() {
        System.out.println("visit(DateValue):");
        String dateStr = "1995-01-01";
        Date squallDate = new DateConversion().fromString(dateStr);
        Expression exp = new DateValue(" " + dateStr + " ");
        exp.accept(_visitor);
        Date jsqlDate = (Date) _visitor.getResult();
        assertEquals(jsqlDate, squallDate);
    }
    
    @Test
    public void testVisit_DateOp1() {
        System.out.println("visit(DateOp1):");
        Date END_DATE = _dtc.fromString("1998-12-31");
        Date orderDateEnd = new DateSum(new ValueSpecification(_dtc, END_DATE), Calendar.DATE, -151).eval(null);
        
        String dateExp = "1998-08-02";
        Expression exp = new DateValue(" " + dateExp + " ");
        exp.accept(_visitor);
        Date jsqlDate = (Date) _visitor.getResult();
        assertEquals(jsqlDate, orderDateEnd);
    }
    
    @Test
    public void testVisit_DateOp2() {
        System.out.println("visit(DateOp2):");
        Date END_DATE = _dtc.fromString("1998-12-31");
        Date orderDateEnd = new DateSum(new ValueSpecification(_dtc, END_DATE), Calendar.DATE, -30).eval(null);
        
        String dateExp = "1998-12-01";
        Expression exp = new DateValue(" " + dateExp + " ");
        exp.accept(_visitor);
        Date jsqlDate = (Date) _visitor.getResult();
        assertEquals(jsqlDate, orderDateEnd);
    }    
    
    @Test
    public void testVisit_DateOp3() {
        System.out.println("visit(DateOp3):");
        Date END_DATE = _dtc.fromString("1998-12-31");
        Date orderDateEnd = new DateSum(new ValueSpecification(_dtc, END_DATE), Calendar.DATE, -61).eval(null);
        
        String dateExp = "1998-10-31";
        Expression exp = new DateValue(" " + dateExp + " ");
        exp.accept(_visitor);
        Date jsqlDate = (Date) _visitor.getResult();
        assertEquals(jsqlDate, orderDateEnd);
    }  
    
    @Test
    public void testVisit_DateOp4() {
        System.out.println("visit(DateOp4):");
        Date END_DATE = _dtc.fromString("1998-12-31");
        Date orderDateEnd = new DateSum(new ValueSpecification(_dtc, END_DATE), Calendar.DATE, -121).eval(null);
        
        String dateExp = "1998-09-01";
        Expression exp = new DateValue(" " + dateExp + " ");
        exp.accept(_visitor);
        Date jsqlDate = (Date) _visitor.getResult();
        assertEquals(jsqlDate, orderDateEnd);
    }    
}