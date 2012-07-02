
package components;

import expressions.ValueExpression;
import java.io.Serializable;
import java.util.List;
import operators.ChainOperator;


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
    public Component getChild();
    public List<DataSourceComponent> getAncestorDataSources();

}
