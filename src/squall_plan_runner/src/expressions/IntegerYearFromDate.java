/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package expressions;

import conversion.TypeConversion;
import conversion.IntegerConversion;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import visitors.ValueExpressionVisitor;


public class IntegerYearFromDate implements ValueExpression<Integer> {
    private static final long serialVersionUID = 1L;

    private ValueExpression<Date> _veDate;
    private TypeConversion<Integer> _wrapper = new IntegerConversion();

    public IntegerYearFromDate(ValueExpression<Date> veDate){
        _veDate = veDate;
    }

	@Override
	public Integer eval(List<String> tuple) {
        Date date = _veDate.eval(tuple);
        
        Calendar c = Calendar.getInstance();
        c.setTime(date);
        int year = c.get(Calendar.YEAR);

        /* Alternative approach:
        SimpleDateFormat formatNowYear = new SimpleDateFormat("yyyy");
        String currentYear = formatNowYear.format(date); // = '2006'
        */

        return year;
    }
    
	/*@Override
	public Integer eval(List<String> firstTuple, List<String> secondTuple) {
        Date date = _veDate.eval(firstTuple, secondTuple);
        
        Calendar c = Calendar.getInstance();
        c.setTime(date);
        int year = c.get(Calendar.YEAR);

        // Alternative approach:
        //SimpleDateFormat formatNowYear = new SimpleDateFormat("yyyy");
        //String currentYear = formatNowYear.format(date); // = '2006'
        

        return year;
    }*/

    @Override
    public String evalString(List<String> tuple) {
        int result = eval(tuple);
        return _wrapper.toString(result);
    }

    @Override
    public TypeConversion getType(){
        return _wrapper;
    }

    @Override
    public void accept(ValueExpressionVisitor vev) {
        vev.visit(this);
    }

    @Override
    public List<ValueExpression> getInnerExpressions() {
        List<ValueExpression> result = new ArrayList<ValueExpression>();
        result.add(_veDate);
        return result;
    }

    @Override
    public String toString(){
        StringBuilder sb = new StringBuilder();
        sb.append("IntegerYearFromDate ").append(_veDate.toString());
        return sb.toString();
    }

    @Override
	public void changeValues(int i, ValueExpression<Integer> newExpr) {
		//nothing
		
	}

	@Override
	public void inverseNumber() {
		//nothing
		
	}

	@Override
	public boolean isNegative() {
		return false;
	}
    
    

}