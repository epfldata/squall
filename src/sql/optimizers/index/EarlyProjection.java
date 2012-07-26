package sql.optimizers.index;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import plan_runner.components.Component;
import plan_runner.expressions.ColumnReference;
import plan_runner.expressions.ValueExpression;
import plan_runner.operators.AggregateOperator;
import plan_runner.operators.ProjectOperator;
import plan_runner.queryPlans.QueryPlan;
import sql.schema.Schema;
import sql.util.ParserUtil;
import sql.util.TableAliasName;
import sql.visitors.squall.VECollectVisitor;
     /*
     * Eliminating unncessary indexes - applying projections wherever possible.
      * Has two phases - bottom up (informing what child need)
      *                - top down (parent says what it has to send)
      * Note that parent has to send something which no descendant need (in order to perfrorm B):
      *                - Selection (A)
      *                - Projection
      *                - Aggregation, groupByColumns, HashIndexes, HashColumns (B)
      * Distinct and Project are not supported to appear in ChainOperators before we introduce them.
      * It is assumed that whatever Projection sends, it will be seen by the next level component.
     */
public class EarlyProjection {
    private Schema _schema;
    private TableAliasName _tan;

    private List<CompPackage> _cpList = new ArrayList<CompPackage>();
    //could go into cpList, because we need to access it from parent, and we don't have CompPackage parent property
    private HashMap<Component, List<Integer>> _compOldProj = new HashMap<Component, List<Integer>>();

    EarlyProjection(Schema schema, TableAliasName tan) {
        _schema = schema;
        _tan = tan;
    }

    public void operate(QueryPlan queryPlan){
        bottomUp(queryPlan);
        topDown();
    }

    private void bottomUp(QueryPlan queryPlan){
        List<Integer> inheritedUsed = new ArrayList<Integer>();
        bottomUp(queryPlan.getLastComponent(), inheritedUsed, 0);
    }

    private void bottomUp(Component component, List<Integer> inheritedUsed, int level){
        addToLevelCollection(component, level);

        List<Integer> directlyUsedIndexes = new ArrayList<Integer>();
        directlyUsedIndexes.addAll(getDirectlyUsedIndexes(component));
       
        List<ValueExpression> afterProjVE = getAfterProjVEs(component);
        List<ValueExpression> allVE = getAllVEs(component);

        List<Integer> allUsedIndexes = new ArrayList<Integer>();
        allUsedIndexes.addAll(inheritedUsed);
        allUsedIndexes.addAll(directlyUsedIndexes);
        allUsedIndexes.addAll(ParserUtil.getColumnRefIndexes(ParserUtil.getColumnRefFromVEs(allVE)));
        allUsedIndexes = sortElimDuplicates(allUsedIndexes);

        List<Integer> afterProjIndexes = new ArrayList<Integer>();
        afterProjIndexes.addAll(inheritedUsed);
        afterProjIndexes.addAll(directlyUsedIndexes);
        List<ColumnReference> afterProjColRefs = ParserUtil.getColumnRefFromVEs(afterProjVE);
        afterProjIndexes.addAll(ParserUtil.getColumnRefIndexes(afterProjColRefs));
        afterProjIndexes = sortElimDuplicates(afterProjIndexes);

        //set projection as if parent do not change
        ProjectOperator projection = new ProjectOperator(ParserUtil.listToArr(afterProjIndexes));
        component.addOperator(projection);

        //projection changed, everybody after it should notice that
        updateColumnRefs(afterProjColRefs, afterProjIndexes);
        updateIndexes(component, afterProjIndexes);

        //sending to parents
        Component[] parents = component.getParents();
        if(parents!=null){
            //left
            Component leftParent = parents[0];
            int leftParentSize = ParserUtil.getPreOpsOutputSize(leftParent, _schema, _tan);
            List<Integer> leftSentUsedIndexes = filterLess(allUsedIndexes, leftParentSize);
            bottomUp(leftParent, leftSentUsedIndexes, level + 1);

            //right
            if(parents.length == 2){
                Component rightParent = parents[1];
                List<Integer> rightUsedIndexes = filterEqualBigger(allUsedIndexes, leftParentSize);
                List<Integer> rightSentUsedIndexes = createRightSendIndexes(rightUsedIndexes, rightParent, leftParentSize);
                bottomUp(rightParent, rightSentUsedIndexes, level + 1);
            }
        }
    }

    private void addToLevelCollection(Component component, int level) {
        if(component.getParents() != null){
            CompPackage cp = new CompPackage(component, level);
            _cpList.add(cp);
        }
    }

    //in this method, only plain indexes are detected
    //agg.GroupBy and hashIndexes
    private List<Integer> getDirectlyUsedIndexes(Component component){
        List<Integer> result = new ArrayList<Integer>();
        
        //add integers: hashIndexes and groupBy in aggregation
        List<Integer> hashIndexes = component.getHashIndexes();
        if(hashIndexes != null){
            result.addAll(hashIndexes);
        }
        AggregateOperator agg = component.getChainOperator().getAggregation();
        if(agg!=null){
            List<Integer> groupBy = agg.getGroupByColumns();
            if(groupBy != null){
                result.addAll(groupBy);
            }
        }

        return result;
    }

    private List<ValueExpression> getAllVEs(Component component){
        VECollectVisitor veVisitor = new VECollectVisitor();
        veVisitor.visit(component);
        return veVisitor.getAllExpressions();
    }

    private List<ValueExpression> getBeforeProjVEs(Component component){
        VECollectVisitor veVisitor = new VECollectVisitor();
        veVisitor.visit(component);
        return veVisitor.getBeforeProjExpressions();
    }

    private List<ValueExpression> getAfterProjVEs(Component component){
        VECollectVisitor veVisitor = new VECollectVisitor();
        veVisitor.visit(component);
        return veVisitor.getAfterProjExpressions();
    }



    private List<Integer> sortElimDuplicates(List<Integer> indexes) {
        Collections.sort(indexes);
        List<Integer> result = new ArrayList<Integer>();

        int lastSeen = indexes.get(0);
        result.add(lastSeen);
        for(int i=1; i<indexes.size(); i++){
            int current = indexes.get(i);
            if(current!=lastSeen){
                lastSeen = current;
                result.add(lastSeen);
            }
        }
        return result;

        /* Shorter, but less efficient
        for(int index: indexes){
            if(!result.contains(index)){
                result.add(index);
            }
        }*/
    }

    private List<Integer> filterLess(List<Integer> indexes, int limit) {
        List<Integer> result = new ArrayList<Integer>();
        for(Integer index: indexes){
            if(index<limit){
                result.add(index);
            }
        }
        return result;
    }

    private List<Integer> filterEqualBigger(List<Integer> indexes, int limit) {
        List<Integer> result = new ArrayList<Integer>();
        for(Integer index: indexes){
            if(index >= limit){
                result.add(index);
            }
        }
        return result;
    }

    private void updateColumnRefs(List<ColumnReference> crList, List<Integer> filteredIndexList) {
        for(ColumnReference cr: crList){
            int oldIndex = cr.getColumnIndex();
            int newIndex = elemsBefore(oldIndex, filteredIndexList);
            cr.setColumnIndex(newIndex);
        }
    }
    
    //the same as in directlyIndexes: agg.groupBy and hashIndexes
    private void updateIndexes(Component component, List<Integer> filteredIndexList){
        List<Integer> oldHashIndexes = component.getHashIndexes();
        if(oldHashIndexes != null){
            List<Integer> newHashIndexes = elemsBefore(oldHashIndexes, filteredIndexList);
            component.setHashIndexes(newHashIndexes);
        }
        AggregateOperator agg = component.getChainOperator().getAggregation();
        if(agg!=null){
            List<Integer> oldGroupBy = agg.getGroupByColumns();
            if(oldGroupBy != null && !oldGroupBy.isEmpty()){
                List<Integer> newGroupBy = elemsBefore(oldGroupBy, filteredIndexList);
                agg.setGroupByColumns(newGroupBy);
            }
        }
    }

    //update indexes so that they represent position in filteredIndexList.
    private List<Integer> elemsBefore(List<Integer> old, List<Integer> filteredIndexList){
        List<Integer> result = new ArrayList<Integer>();
        for(int i: old){
            result.add(elemsBefore(i, filteredIndexList));
        }
        return result;
    }

    //elem must belong to intList
    private int elemsBefore(int elem, List<Integer> intList){
        if(!intList.contains(elem)){
            throw new RuntimeException("Developer error. elemsBefore: no element.");
        }
        return intList.indexOf(elem);
    }

    private List<Integer> createRightSendIndexes(List<Integer> rightUsedIndexes, Component rightParent, int leftParentSize) {
        List<Integer> result = new ArrayList<Integer>();

        for(Integer i: rightUsedIndexes){
            //first step is to normalize right indexes starting with 0
            int normalized = i - leftParentSize;
            int sent = positionListIngoreHash(normalized, rightParent.getHashIndexes());
            result.add(sent);
        }

        return result;
    }

    private int positionListIngoreHash(int normalized, List<Integer> hashIndexes) {
        int result = 0;
        int moves = 0;

        // take care of hashes which are on the continuous range from zero (0, 1, 2, 3 ...)
        for(int i=0; i<hashIndexes.size(); i++){
            if (hashIndexes.contains(i)){
                result++;
            }
        }

        while(moves < normalized){
            if(!hashIndexes.contains(result)){
                moves++;
            }
            result++;
        }

        //if we are positioned on the hash, we have to move on
        while(hashIndexes.contains(result)){
            result++;
        }
        
        return result;
    }

    private void topDown(){
        Collections.sort(_cpList);
        for(CompPackage cp: _cpList){
            List<Integer> fromParents = arrivedFromParents(cp);
            Component comp = cp.getComponent();

            //update Selection indexes
            List<ValueExpression> beforeProjVE = getBeforeProjVEs(comp);
            List<ColumnReference> beforeProjColRefs = ParserUtil.getColumnRefFromVEs(beforeProjVE);
            updateColumnRefs(beforeProjColRefs, fromParents);

            //update Projection indexes
            List<ValueExpression> projVE = comp.getChainOperator().getProjection().getExpressions();
            List<ColumnReference> projColRefs = ParserUtil.getColumnRefFromVEs(projVE);

            //after bottom-up: projection will be set, so it will contain all the necessary fields,
            //  but later it might be moved because of up projections (total number of projections does not change)
            List<Integer> oldProjIndexes = ParserUtil.getColumnRefIndexes(projColRefs);
            _compOldProj.put(comp, oldProjIndexes);
            updateColumnRefs(projColRefs, fromParents);

        }
    }

    private List<Integer> arrivedFromParents(CompPackage cp){
         Component comp = cp.getComponent();
         Component[] parents = comp.getParents();

         List<Integer> fromParents = new ArrayList<Integer>();
         //at least one parent
         Component leftParent = parents[0];
         fromParents.addAll(extractProjIndexesAfterBottomUp(leftParent));

         if(parents.length == 2){
             Component rightParent = parents[1];
             List<Integer> rightIndexes = extractProjIndexesAfterBottomUp(rightParent);

             //take into account neglected hash from rhs
             rightIndexes = filterHash(rightIndexes, rightParent.getHashIndexes());

             //fromParents contains leftParent, that's why we use its size as an offset
             rightIndexes = addOffset(rightIndexes, cp.getLeftParentOutputSize());
             fromParents.addAll(rightIndexes);
         }
         return fromParents;
    }

    private List<Integer> extractProjIndexesAfterBottomUp(Component comp){
        if(comp.getParents() == null){
            return ParserUtil.extractColumnIndexes(comp.getChainOperator().getProjection().getExpressions());
        }else{
            return _compOldProj.get(comp);
        }
    }

    // if old is [0 1 5 10] and hash is [2], the result is [0 1 9]
    private List<Integer> filterHash(List<Integer> old, List<Integer> hashes){
        List<Integer> result = new ArrayList<Integer>();
        int hashesBefore=0;
        for(int i=0; i<old.size(); i++){
            if(hashes.contains(i)){
                hashesBefore++;
            }else{
                int current = old.get(i);
                int newValue = current - hashesBefore;
                result.add(newValue);
            }
        }
        return result;
    }

    private List<Integer> addOffset(List<Integer> intList, int offset){
        List<Integer> result = new ArrayList<Integer>();
        for(int i=0; i < intList.size(); i++){
            int current = intList.get(i);
            int newValue = current + offset;
            result.add(newValue);
        }
        return result;
    }

    private class CompPackage implements Comparable<CompPackage> {
        private Component _component;
        private int _level;
        private int _leftParentOutputSize;
        //just after bottomUp

        public CompPackage(Component component, int level) {
            _component = component;
            _level = level;

            Component[] parents = component.getParents();
            if(parents!=null){
                Component leftParent = parents[0];
                _leftParentOutputSize = ParserUtil.getPreOpsOutputSize(leftParent, _schema, _tan);
            }
        }

        public Component getComponent(){
            return _component;
        }

        public int getLevel(){
            return _level;
        }

        public int getLeftParentOutputSize(){
            return _leftParentOutputSize;
        }

        //descending order
        @Override
        public int compareTo(CompPackage cp) {
            int otherLevel = cp.getLevel();
            return (new Integer(otherLevel)).compareTo(new Integer(_level));
        }
    }
}