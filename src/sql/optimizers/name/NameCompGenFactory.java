package sql.optimizers.name;

import java.util.Map;
import plan_runner.utilities.SystemParameters;
import sql.schema.Schema;
import sql.schema.TPCH_Schema;

/*
 * It generates different NameCompGen for each partial query plan
 *   NameCompGen is responsible for attaching operators to components
 * Aggregation only on the last level.
 */
public class NameCompGenFactory {
    private Schema _schema;
    private Map _map; //map is updates in place
    
    private CostParallelismAssigner _parAssigner;
    
    /*
     * only plan, no parallelism
     */
    public NameCompGenFactory(Map map){
        _map = map;
        
        double scallingFactor = SystemParameters.getDouble(_map, "DIP_DB_SIZE");
        _schema = new TPCH_Schema(scallingFactor);
    }
    
    /*
     * generating plan + parallelism
     */
    public NameCompGenFactory(Map map, int totalSourcePar){
        this(map);
        setParAssignerMode(totalSourcePar);
    }
    
    public final void setParAssignerMode(int totalSourcePar){
        //in general there might be many NameComponentGenerators, 
        //  that's why CPA is computed before of NCG
        _parAssigner = new CostParallelismAssigner(_schema, _map);
        
        //for the same _parAssigner, we might try with different totalSourcePar
        _parAssigner.computeSourcePar(totalSourcePar);
    }
    
    public NameCompGen create(){        
        return new NameCompGen(_schema, _map, _parAssigner);
    }
    
    public CostParallelismAssigner getParAssigner(){
        return _parAssigner;
    }

}