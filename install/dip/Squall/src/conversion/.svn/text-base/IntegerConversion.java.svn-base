/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package conversion;


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
}
