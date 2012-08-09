/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package plan_runner.conversion;


public class IntegerConversion implements NumericConversion<Integer> {
    private static final long serialVersionUID = 1L;

    @Override
    public Integer fromString(String str) {
        return Integer.valueOf(str);
    }

    @Override
    public String toString(Integer obj) {
        return obj.toString();
    }

    @Override
    public Integer fromDouble(double d) {
        return (int)d;
    }

    @Override
    public double toDouble(Integer obj) {
        int value = (Integer)obj;
        return (double)value;
    }

    @Override
    public Integer getInitialValue() {
        return new Integer(0);
    }

    @Override
    public double getDistance(Integer bigger, Integer smaller) {
        return bigger - smaller;
    }
    
    //for printing(debugging) purposes
    @Override
    public String toString(){
        return  "INTEGER";
    }    
}
