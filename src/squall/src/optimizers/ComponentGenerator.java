/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package optimizers;

import components.Component;
import components.DataSourceComponent;
import components.JoinComponent;
import expressions.ValueExpression;
import java.util.ArrayList;
import java.util.List;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.Join;
import queryPlans.QueryPlan;
import schema.ColumnNameType;
import schema.Schema;
import util.ParserUtil;
import util.TableAliasName;
import visitors.squall.JoinHashVisitor;


public class ComponentGenerator {
    private Schema _schema;
    private String _dataPath;
    private String _extension;

    private QueryPlan _queryPlan = new QueryPlan();

    //List of JoinComponent and OperatorComponent
    private List<Component> _subPlans = new ArrayList<Component>();

    private TableAliasName _tan;

    public ComponentGenerator(Schema schema, TableAliasName tan, String dataPath, String extension){
        _schema = schema;
        _tan = tan;
        _dataPath = dataPath;
        _extension = extension;
    }

    public QueryPlan getQueryPlan(){
        return _queryPlan;
    }

    public List<Component> getSubPlans(){
        return _subPlans;
    }

    //two tables
    public Component generateSubplan(Table leftTable, Table rightTable, Join join) {
        List<Expression> listExp = ParserUtil.createListExp(join.getOnExpression());
        return generateSubplan(leftTable, rightTable, listExp);
    }

    public Component generateSubplan(Table leftTable, Table rightTable, List<Expression> listExp) {
        DataSourceComponent leftDSC = generateDataSource(leftTable);
        DataSourceComponent rightDSC = generateDataSource(rightTable);
        JoinComponent lastComponent = generateJoinComponent(leftDSC, rightDSC, listExp);
        _subPlans.add(lastComponent);
        return lastComponent;
    }

    //table and Component
    public Component generateSubplan(Component component, Table table, Join join){
        List<Expression> listExp = ParserUtil.createListExp(join.getOnExpression());
        return generateSubplan(component, table, listExp);
    }

    public Component generateSubplan(Component component, Table table, List<Expression> listExp){
        DataSourceComponent dataSourceComp = generateDataSource(table);
        JoinComponent lastComponent = generateJoinComponent(component, dataSourceComp, listExp);
        _subPlans.remove(component);
        _subPlans.add(lastComponent);
        return lastComponent;
    }

    //two Components
    public Component generateSubplan(Component left, Component right, Join join){
        List<Expression> listExp = ParserUtil.createListExp(join.getOnExpression());
        return generateSubplan(left, right, listExp);
    }

    public Component generateSubplan(Component left, Component right, List<Expression> listExp){
        JoinComponent lastComponent = generateJoinComponent(left, right, listExp);
        _subPlans.remove(left);
        _subPlans.remove(right);
        _subPlans.add(lastComponent);
        return lastComponent;
    }

    //generate Squall components from JSQL structures
    public DataSourceComponent generateDataSource(Table table){
        String tableSchemaName = _tan.getSchemaName(ParserUtil.getComponentName(table));
        String tableCompName = ParserUtil.getComponentName(table);
        String sourceFile = tableSchemaName.toLowerCase();
        List<ColumnNameType> tableSchema = _schema.getTableSchema(tableSchemaName);

        DataSourceComponent relation = new DataSourceComponent(
                                        tableCompName,
                                        _dataPath + sourceFile + _extension,
                                        tableSchema,
                                        _queryPlan);
        return relation;
    }

    public JoinComponent generateJoinComponent(Component leftParent, Component rightParent, Join join){
       List<Expression> listExp = ParserUtil.createListExp(join.getOnExpression());
       return generateJoinComponent(leftParent, rightParent, listExp);
    }

    private JoinComponent generateJoinComponent(Component leftParent, Component rightParent, List<Expression> listExp) {
        JoinComponent joinComponent = new JoinComponent(
                    leftParent,
                    rightParent,
                    _queryPlan);

        //set hashes for two parents
        setHash(leftParent, listExp);
        setHash(rightParent, listExp);

        return joinComponent;
    }

    //HELPER methods
    //this is used from SimpleOpt - parser tree is exactly the same as the generated Squall tree
    public void setHash(Component component, Join nextJoin){
        if(nextJoin!=null){
            //if there is next level, we set hash indexes
            List<Expression> listExp = ParserUtil.createListExp(nextJoin.getOnExpression());
            setHash(component, listExp);
        }
    }

    //joinCondition from next level join
    //Conditions are related only to parents of join,
    //  but we have to filter who belongs to my branch in JoinHashVisitor.
    //We don't want to hash on something which will be used to join with same later component in the hierarchy.
    private void setHash(Component component, List<Expression> listExp) {
            JoinHashVisitor joinOn = new JoinHashVisitor(_schema, _queryPlan, component, _tan);
            for(Expression exp: listExp){
                exp.accept(joinOn);
            }
            List<ValueExpression> hashExpressions = joinOn.getExpressions();

            if(!joinOn.isComplexCondition()){
                //all the join conditions are represented through columns, no ValueExpression (neither in joined component)
                //guaranteed that both joined components will have joined columns visited in the same order
                //i.e R.A =S.A and R.B=S.B, the columns are (R.A, R.B), (S.A, S.B), respectively
                List<Integer> hashIndexes = ParserUtil.extractColumnIndexes(hashExpressions);

                //set hashColumn for this joinComponent
                component.setHashIndexes(hashIndexes);
            }else{
                //ValueExpression in join condition
                component.setHashExpressions(hashExpressions);
            }
    }

}