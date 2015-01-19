package ch.epfl.data.sql.optimizers.index;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import ch.epfl.data.plan_runner.components.Component;
import ch.epfl.data.plan_runner.components.DataSourceComponent;

public class LevelAssigner {
    private final List<DataSourceComponent> _dsList = new ArrayList<DataSourceComponent>();
    private final List<CompLevel> _clList = new ArrayList<CompLevel>(); // list
									// of
									// all
    // Components
    // which are
    // not
    // DataSourceComponent

    private int _maxLevel = 0;

    public LevelAssigner(Component lastComponent) {
	visit(lastComponent, 0);
	orderComponents();
    }

    public List<CompLevel> getNonSourceComponents() {
	return _clList;
    }

    public List<DataSourceComponent> getSources() {
	return _dsList;
    }

    // order them such that the first component after dataSource has height 1,
    // ..., the root has the highest height
    private void orderComponents() {
	for (final CompLevel cl : _clList) {
	    final int level = cl.getLevel();
	    final int newLevel = _maxLevel - level;
	    cl.setLevel(newLevel);
	}
	Collections.sort(_clList);
    }

    // level from the root
    private void visit(Component comp, int level) {
	if (_maxLevel < level)
	    _maxLevel = level;

	if (comp instanceof DataSourceComponent)
	    _dsList.add((DataSourceComponent) comp);
	else {
	    _clList.add(new CompLevel(comp, level));

	    for (final Component parent : comp.getParents())
		visit(parent, level + 1);
	}
    }
}
