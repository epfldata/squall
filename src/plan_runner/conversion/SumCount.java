
package plan_runner.conversion;


public class SumCount{
    private Double _sum;
    private Long _count;

    public SumCount(Double sum, Long count){
        _sum = sum;
        _count = count;
    }
 
    public Double getSum() {
        return _sum;
    }

    public void setSum(Double sum) {
        _sum = sum;
    }
     
    public Long getCount() {
        return _count;
    }

    public void setCount(Long count) {
        _count = count;
    }

    public double getAvg(){
        return _sum/_count;
    }

    @Override
    public boolean equals(Object obj){
        if(this == obj){
            return true;
        }
        if (!(obj instanceof SumCount)){
            return false;
        }
        SumCount otherSumCount = (SumCount) obj;
        return getAvg() == otherSumCount.getAvg();
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 89 * hash + (_sum != null ? _sum.hashCode() : 0);
        hash = 89 * hash + (_count != null ? _count.hashCode() : 0);
        return hash;
    }
}