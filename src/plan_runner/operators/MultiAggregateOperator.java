package plan_runner.operators;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import plan_runner.conversion.TypeConversion;
import plan_runner.storage.BasicStore;
import plan_runner.visitors.OperatorVisitor;

public class MultiAggregateOperator implements AggregateOperator {

    private List<AggregateOperator> _opList;
    private Map _map;

    public MultiAggregateOperator(List<AggregateOperator> opList, Map map){
        _opList = opList;
        _map = map;
    }

    @Override
    public List<String> process(List<String> tuple) {
        //this will work only if they have the same groupBy
        //otherwise the result of process is not important at all
        List<String> result = new ArrayList<String>();
        int i=0;
        for(AggregateOperator agg:_opList){
            List<String> current = agg.process(tuple);
            if(i==0){
                result.addAll(current);
            }else{
                //for all beside the first result we exclude the groupBy columns
                //because of preaggragations, there might be multiple groupBy columns (it's not A-B|res, but A|B|res)
                //we know that all of them are at the beginning
                int numGB = getNumGroupByColumns(agg);
                result.addAll(current.subList(numGB, current.size()));
            }
            i++;
        }
        return result;
    }
    
    private int getNumGroupByColumns(AggregateOperator agg){
        int result = 0;
        List<Integer> groupByColumns = agg.getGroupByColumns();
        if(groupByColumns != null){
            result += groupByColumns.size();
        }
        ProjectOperator groupByProjection = agg.getGroupByProjection();
        if(groupByProjection != null){
            result += groupByProjection.getExpressions().size();
        }
        return result;
    }


    @Override
    public boolean isBlocking() {
        return true;
    }

    @Override
    public String printContent() {
        StringBuilder sb = new StringBuilder();
        int i=0;
        for(AggregateOperator agg:_opList){
            sb.append("\nAggregation ").append(i).append("\n").append(agg.printContent());
            i++;
        }
        return sb.toString();
    }

    @Override
    public List<String> getContent() {
        throw new RuntimeException("Preaggregation with MultiAggregateOperator does not work yet.");
    }

    @Override
    public int getNumTuplesProcessed() {
        for(AggregateOperator agg:_opList){
            return agg.getNumTuplesProcessed();
            //the result of the first operator, but this is the same for all the AggregateOperators
        }
        return 0;
    }

    @Override
    public void accept(OperatorVisitor ov){
        ov.visit(this);
    }

    @Override
    public AggregateOperator setGroupByColumns(List groupByColumns) {
        throw new UnsupportedOperationException("You are not supposed to call this method from MultiAggregateOperator.");
    }

    @Override
    public List getGroupByColumns() {
        throw new UnsupportedOperationException("You are not supposed to call this method from MultiAggregateOperator.");
    }

    @Override
    public AggregateOperator setGroupByProjection(ProjectOperator projection) {
        throw new UnsupportedOperationException("You are not supposed to call this method from MultiAggregateOperator.");
    }

    @Override
    public ProjectOperator getGroupByProjection() {
        throw new UnsupportedOperationException("You are not supposed to call this method from MultiAggregateOperator.");
    }

    @Override
    public AggregateOperator setDistinct(DistinctOperator distinct) {
        throw new UnsupportedOperationException("You are not supposed to call this method from MultiAggregateOperator.");
    }

    @Override
    public DistinctOperator getDistinct() {
        throw new UnsupportedOperationException("You are not supposed to call this method from MultiAggregateOperator.");
    }

    @Override
    public List getExpressions() {
        throw new UnsupportedOperationException("You are not supposed to call this method from MultiAggregateOperator.");
    }

    @Override
    public BasicStore getStorage() {
        throw new UnsupportedOperationException("You are not supposed to call this method from MultiAggregateOperator.");
    }

    @Override
    public void clearStorage(){
        throw new UnsupportedOperationException("You are not supposed to call this method from MultiAggregateOperator.");
    }

    @Override
    public Object runAggregateFunction(Object value, List tuple) {
        throw new UnsupportedOperationException("You are not supposed to call this method from MultiAggregateOperator.");
    }

    @Override
    public Object runAggregateFunction(Object value1, Object value2) {
        throw new UnsupportedOperationException("You are not supposed to call this method from MultiAggregateOperator.");
    }
    
    @Override
    public TypeConversion getType(){
        throw new UnsupportedOperationException("You are not supposed to call this method from MultiAggregateOperator.");
    }    
    
    @Override
    public boolean hasGroupBy(){
        throw new UnsupportedOperationException("You are not supposed to call this method from MultiAggregateOperator.");
    }    
}
