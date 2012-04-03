/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package expressions;

import conversion.StringConversion;
import conversion.TypeConversion;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import utilities.MyUtilities;
import visitors.ValueExpressionVisitor;

public class StringConcatenate implements ValueExpression<String> {
    private static final long serialVersionUID = 1L;

    private ArrayList<ValueExpression<String>> _strList = new ArrayList<ValueExpression<String>>();

    public StringConcatenate(ValueExpression<String> str1, ValueExpression<String> str2,
            ValueExpression<String>... strArray){
        _strList.add(str1);
        _strList.add(str2);
        _strList.addAll(Arrays.asList(strArray));
    }

    @Override
    public String eval(List<String> tuple){
        String result = "";
        for(ValueExpression<String> str: _strList){
            result += str;
        }
        return result;
    }

    @Override
    public String evalString(List<String> tuple) {
        return eval(tuple);
    }

    @Override
    public TypeConversion getType(){
        return new StringConversion();
    }

    @Override
    public void accept(ValueExpressionVisitor vev) {
        vev.visit(this);
    }

    @Override
    public List<ValueExpression> getInnerExpressions() {
        return MyUtilities.listTypeErasure(_strList);
    }

    @Override
    public String toString(){
        StringBuilder sb = new StringBuilder();
        for(int i=0; i<_strList.size(); i++){
            sb.append("(").append(_strList.get(i)).append(")");
            if(i!=_strList.size()-1){
                sb.append(" STR_CONCAT ");
            }
        }
        return sb.toString();
    }

}