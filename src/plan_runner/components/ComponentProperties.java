package plan_runner.components;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import plan_runner.expressions.ValueExpression;
import plan_runner.operators.ChainOperator;
import plan_runner.storage.BasicStore;


public interface ComponentProperties extends Serializable{

    public String getName();
    public String getInfoID();

    public List<Integer> getHashIndexes();
    public List<ValueExpression> getHashExpressions();
    public List<String> getFullHashList();
    public ChainOperator getChainOperator(); //contains all the previously added operators

    public boolean getPrintOut();
    public long getBatchOutputMillis();

    public Component[] getParents();
    //TODO: problem when having multiple children (sharing scenarios)
    public Component getChild();
    
    public List<DataSourceComponent> getAncestorDataSources();

}
