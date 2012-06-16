package optimizers.rule;

import components.Component;
import components.DataSourceComponent;
import java.util.List;
import java.util.Map;
import queryPlans.QueryPlan;
import schema.Schema;
import util.TableAliasName;
import utilities.SystemParameters;

public class RuleParallelismAssigner {
    private int THRESHOLD_TUPLES = 100; // both nation and region has less number of tuples

    private QueryPlan _plan;
    private int _maxInputPar;
    private TableAliasName _tan;
    private Schema _schema;
    private Map _map;

    public RuleParallelismAssigner(QueryPlan plan, TableAliasName tan, Schema schema, Map map){
        _plan = plan;
        _tan = tan;
        _schema = schema;
        _map=map;
        _maxInputPar = SystemParameters.getInt(map, "DIP_MAX_SRC_PAR");
    }

    public void assignPar() {
        LevelAssigner topDown = new LevelAssigner(_plan.getLastComponent());
        List<DataSourceComponent> dsList = topDown.getSources();
        List<CompLevel>  clList = topDown.getNonSourceComponents();

        assignParDataSource(dsList);
        assignParNonDataSource(clList);
    }

    private void assignParDataSource(List<DataSourceComponent> sources) {
        for(DataSourceComponent source: sources){
            String compName = source.getName();
            String compMapStr = compName + "_PAR";
            if(getNumOfTuples(compName) > THRESHOLD_TUPLES){
                SystemParameters.putInMap(_map, compMapStr, _maxInputPar);
            }else{
                SystemParameters.putInMap(_map, compMapStr, 1);
            }
        }
    }

    private int getNumOfTuples(String compName){
        String schemaName = _tan.getSchemaName(compName);
        return _schema.getTableSize(schemaName);
    }

    private void assignParNonDataSource(List<CompLevel> clList) {
        for(CompLevel cl: clList){
            Component comp = cl.getComponent();
            String compName = comp.getName();
            String compMapStr = compName + "_PAR";
            int level = cl.getLevel();

            if(comp.getParents().length < 2){
                //an operatorComponent should have no more parallelism than its (only) parent
                level--;
            }

            //TODO: for the last operatorComponent, parallelism should be based on groupBy

            int parallelism = (int)(_maxInputPar * Math.pow(2, level - 2));
            if(parallelism < 1){
                //cannot be less than 1
                parallelism = 1;
            }

            SystemParameters.putInMap(_map, compMapStr, parallelism);
        }
    }
}