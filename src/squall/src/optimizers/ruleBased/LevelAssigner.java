/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package optimizers.ruleBased;

import components.Component;
import components.DataSourceComponent;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;


public class LevelAssigner {
    private List<DataSourceComponent> _dsList = new ArrayList<DataSourceComponent>();
    private List<CompLevel> _clList = new ArrayList<CompLevel>(); // list of all Components which are not DataSourceComponent
    
    private int _maxLevel = 0;

    public LevelAssigner(Component lastComponent){
        visit(lastComponent, 0);
        orderComponents();
    }

    public List<DataSourceComponent> getDSComponents(){
        return _dsList;
    }

    public List<CompLevel> getNonDSComponents(){
        return _clList;
    }
    
    //level from the root
    private void visit(Component comp, int level){
        if(_maxLevel < level){
            _maxLevel = level;
        }
        
        if(comp instanceof DataSourceComponent){
            _dsList.add((DataSourceComponent)comp);
        }else{
            _clList.add(new CompLevel(comp, level));
        
            for(Component parent: comp.getParents()){
                visit(parent, level + 1);
            }
        }
    }

    //order them such that the first component after dataSource has height 1, ..., the root has the highest height
    private void orderComponents(){
        for(CompLevel cl: _clList){
            int level = cl.getLevel();
            int newLevel = _maxLevel - level;
            cl.setLevel(newLevel);
        }
        Collections.sort(_clList);
    }
}
