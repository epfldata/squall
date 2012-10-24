
package plan_runner.conversion;

import java.io.Serializable;


public class SumCount implements Comparable<SumCount>, Serializable{
    private static final long serialVersionUID = 1L;
    
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
    
    @Override
    public int compareTo(SumCount other) {
        if(getAvg() > other.getAvg()){
            return 1;
        }else if(getAvg() < other.getAvg()){
            return -1;
        }else{
            return 0;
        }
    }    
}