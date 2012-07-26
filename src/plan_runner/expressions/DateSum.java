/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package plan_runner.expressions;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import plan_runner.conversion.DateConversion;
import plan_runner.conversion.TypeConversion;
import plan_runner.visitors.ValueExpressionVisitor;


public class DateSum implements ValueExpression<Date>{
    private TypeConversion<Date> _dc = new DateConversion();

    private ValueExpression<Date> _ve;
    private int _interval, _unit;

    public DateSum(ValueExpression<Date> ve, int unit, int interval){
        _ve = ve;
        _unit = unit;
        _interval = interval;
    }

    @Override
    public Date eval(List<String> tuple) {
        Date base = _ve.eval(tuple);
        Calendar c = Calendar.getInstance();
        c.setTime(base);
        c.add(_unit, _interval);
        return c.getTime();
    }
    
   /* @Override
    public Date eval(List<String> firstTuple, List<String> secondTuple) {
        Date base = _ve.eval(firstTuple, secondTuple);
        Calendar c = Calendar.getInstance();
        c.setTime(base);
        c.add(_unit, _interval);
        return c.getTime();
    }*/

    @Override
    public String evalString(List<String> tuple) {
        return _dc.toString(eval(tuple));
    }

    @Override
    public TypeConversion getType(){
        return _dc;
    }

    @Override
    public void accept(ValueExpressionVisitor vev) {
        vev.visit(this);
    }

    @Override
    public List<ValueExpression> getInnerExpressions() {
        List<ValueExpression> result = new ArrayList<ValueExpression>();
        result.add(_ve);
        return result;
    }

    @Override
    public String toString(){
        StringBuilder sb = new StringBuilder();
        sb.append("DateSum ").append(_ve.toString());
        sb.append(" interval ").append(_interval);
        sb.append(" unit ").append(_unit);
        return sb.toString();
    }

	@Override
	public void changeValues(int i, ValueExpression<Date> newExpr) {
		//nothing
	}

	@Override
	public void inverseNumber() {
		//nothing	
	}

	@Override
	public boolean isNegative() {
		// nothing
		return false;
	}


}