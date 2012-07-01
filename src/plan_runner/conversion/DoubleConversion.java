/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package plan_runner.conversion;


public class DoubleConversion implements NumericConversion<Double> {
    private static final long serialVersionUID = 1L;

    @Override
    public Double fromString(String str) {
        return Double.valueOf(str);
    }

    @Override
    public String toString(Double obj) {
        return obj.toString();
    }

    @Override
    public Double fromDouble(double d) {
        return d;
    }

    @Override
    public double toDouble(Double obj) {
        return (Double) obj;
    }

    @Override
    public Double getInitialValue() {
        return new Double(0.0);
    }

    @Override
    public double getDistance(Double bigger, Double smaller) {
        return bigger - smaller;
    }
}