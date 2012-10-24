package plan_runner.conversion;

public class SumCountConversion implements TypeConversion<SumCount> {
    private static final long serialVersionUID = 1L;

    @Override
    public String toString(SumCount sc){
        return sc.getSum() + ":" + sc.getCount();
    }
            
    @Override
    public SumCount fromString(String sc){
        String parts[] = sc.split("\\:");
        Double sum = Double.valueOf(parts[0]);
        Long count = Long.valueOf(parts[1]);
        return new SumCount(sum, count);
    }

    @Override
    public SumCount getInitialValue() {
        return new SumCount(0.0, 0L);
    }

    @Override
    public double getDistance(SumCount bigger, SumCount smaller) {
        return bigger.getAvg() - smaller.getAvg();
    }
    
    //for printing(debugging) purposes
    @Override
    public String toString(){
        return  "SUM_COUNT";
    }    
    
}