/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package conversion;

public class LongConversion implements NumericConversion<Long> {
    private static final long serialVersionUID = 1L;

    @Override
    public Long fromString(String str) {
        return Long.valueOf(str);
    }

    @Override
    public String toString(Long obj) {
        return obj.toString();
    }

    @Override
    public Long fromDouble(double d) {
        return (long)d;
    }

    @Override
    public double toDouble(Long obj) {
        long value = (Long)obj;
        return (double)value;
    }

    @Override
    public Long getInitialValue() {
        return new Long(0);
    }
}