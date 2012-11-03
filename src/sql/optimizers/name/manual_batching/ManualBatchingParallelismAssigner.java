package sql.optimizers.name.manual_batching;

import sql.optimizers.name.*;
import java.util.*;
import plan_runner.components.DataSourceComponent;
import plan_runner.components.EquiJoinComponent;
import plan_runner.components.OperatorComponent;
import sql.schema.Schema;
import sql.util.TableAliasName;


public class ManualBatchingParallelismAssigner extends CostParallelismAssigner {

    public ManualBatchingParallelismAssigner(Schema schema, TableAliasName tan, Map map) {
        super(schema, tan, map);
    }
    
    @Override
    protected void setBatchSize(DataSourceComponent source, Map<String, CostParams> _compCost) {
        throw new UnsupportedOperationException("Should not be called from NameCostLefty optimizer.");
    }
    
    @Override
    protected int parallelismFormula(CostParams leftParentParams, CostParams rightParentParams) {
        //TODO: this formula does not take into account when joinComponent send tuples further down
        double dblParallelism = leftParentParams.getSelectivity() * leftParentParams.getParallelism() +
                            rightParentParams.getSelectivity() * rightParentParams.getParallelism() +
                            1.0/8 * (leftParentParams.getParallelism() + rightParentParams.getParallelism());
        int parallelism = (int) dblParallelism;
        if(parallelism != dblParallelism){
            //parallelism is ceil of dblParallelism
            parallelism++;
        }
        return parallelism;
    }    
    
    @Override
    protected void setBatchSize(EquiJoinComponent joinComponent, Map<String, CostParams> _compCost) {
        throw new UnsupportedOperationException("Should not be called from NameCostLefty optimizer.");
    }
    
    @Override
    protected void setBatchSize(OperatorComponent operator, Map<String, CostParams> _compCost) {
        throw new UnsupportedOperationException("Should not be called from NameCostLefty optimizer.");
    }    

}