/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package optimizers;

import optimizers.ruleBased.ParallelismAssigner;
import components.Component;
import java.util.List;
import java.util.Map;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.SelectItem;
import schema.Schema;
import util.TableAliasName;
import visitors.squall.SelectItemsVisitor;
import visitors.squall.WhereVisitor;


public class SimpleOpt implements Optimizer {
    private Schema _schema;
    private String _dataPath;
    private String _extension;
    private TableAliasName _tan;
    private OptimizerTranslator _ot;
    private Map _map;
    
    public SimpleOpt(Schema schema, TableAliasName tan, String dataPath, String extension, OptimizerTranslator ot, Map map){
        _schema = schema;
        _tan = tan;
        _dataPath = dataPath;
        _extension = extension;
        _ot = ot;
        _map = map;
    }

    @Override
    public ComponentGenerator generate(List<Table> tableList, List<Join> joinList, List<SelectItem> selectItems, Expression whereExpr){
        ComponentGenerator cg = generateTableJoins(tableList, joinList);

        //selectItems might add OperatorComponent, this is why it goes first
        selectItemsVisitor(selectItems, cg);
        whereVisitor(whereExpr, cg);

        ParallelismAssigner parAssign = new ParallelismAssigner(cg.getQueryPlan(), _tan, _schema, _map);
        parAssign.assignPar();

        return cg;
    }

    private ComponentGenerator generateTableJoins(List<Table> tableList, List<Join> joinList) {
        ComponentGenerator cg = new ComponentGenerator(_schema, _tan, _ot, _dataPath, _extension);

        //special case
        if(tableList.size()==1){
            cg.generateDataSource(tableList.get(0));
            return cg;
        }

        // This generates a lefty query plan.
        Table firstTable = tableList.get(0);
        Table secondTable = tableList.get(1);
        Join firstJoin = joinList.get(0);
        Component comp = cg.generateSubplan(firstTable, secondTable, firstJoin);

        for(int i=1; i<joinList.size(); i++){
            Table currentTable = tableList.get(i+1);
            Join currentJoin = joinList.get(i);
            comp = cg.generateSubplan(comp, currentTable, currentJoin);
        }
        return cg;
    }


    private void selectItemsVisitor(List<SelectItem> selectItems, ComponentGenerator cg) {
        //TODO: take care in nested case
        SelectItemsVisitor selectVisitor = new SelectItemsVisitor(cg.getQueryPlan(), _schema, _tan, _ot, _map);
        for(SelectItem elem: selectItems){
            elem.accept(selectVisitor);
        }
        selectVisitor.doneVisiting();
    }

    private void whereVisitor(Expression whereExpr, ComponentGenerator cg) {
        // TODO: in non-nested case, there is a single Expression
        WhereVisitor whereVisitor = new WhereVisitor(cg.getQueryPlan(), _schema, _tan, _ot);
        if(whereExpr != null){
            whereExpr.accept(whereVisitor);
        }
        whereVisitor.doneVisiting();
    }

}
