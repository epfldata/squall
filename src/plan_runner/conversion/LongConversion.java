package plan_runner.conversion;

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
    public double toDouble(Object obj) {
        long value = (Long)obj;
        return (double)value;
    }

    @Override
    public Long getInitialValue() {
        return new Long(0);
    }

    @Override
    public double getDistance(Long bigger, Long smaller) {
        return bigger - smaller;
    }
    
    //for printing(debugging) purposes
    @Override
    public String toString(){
        return  "LONG";
    }        
}