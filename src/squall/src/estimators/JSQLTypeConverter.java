/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package estimators;

import java.util.Date;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;

/*
 * this class extracts Java types objects out of JSQL wrapper objects
 * TODO fix this with proper visitor (fixed in 1.7.0)
 */
public class JSQLTypeConverter {

    public Date convert(DateValue dv){
        return dv.getValue();
    }
            
    public double convert(DoubleValue dv){
        return dv.getValue();
    }

    public long convert(LongValue lv){
        return lv.getValue();
    }

    public String convert(StringValue sv){
        return sv.getValue();
    }

    public Object convert(Expression expr) {
        if (expr instanceof DateValue){
            return convert((DateValue)expr);
        }else if (expr instanceof DoubleValue){
            return convert((DoubleValue)expr);
        }else if (expr instanceof LongValue){
            return convert((LongValue)expr);
        }else if (expr instanceof StringValue){
            return convert((StringValue)expr);
        }
        throw new UnsupportedOperationException("Should not be here!");
    }
}
