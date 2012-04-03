package stormComponents;

import expressions.ValueExpression;
import java.util.List;


public interface StormEmitter{
    public String getName();
    public int[] getEmitterIDs();

    public List<Integer> getHashIndexes();
    public List<ValueExpression> getHashExpressions();
    public String getInfoID();
}