package plan_runner.query_plans;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import plan_runner.components.Component;

public class QueryPlan implements Serializable {
	private static final long serialVersionUID = 1L;
	private final List<Component> _plan = new ArrayList<Component>();

	public void add(Component component) {
		_plan.add(component);
	}

	// Component names are unique - alias is used for tables
	public boolean contains(String name) {
		for (final Component component : _plan)
			if (component.getName().equals(name))
				return true;
		return false;
	}

	public Component getComponent(String name) {
		for (final Component component : _plan)
			if (component.getName().equals(name))
				return component;
		return null;
	}

	public List<String> getComponentNames() {
		final List<String> result = new ArrayList<String>();
		for (final Component component : _plan)
			result.add(component.getName());
		return result;
	}

	public Component getLastComponent() {
		return _plan.get(_plan.size() - 1);
	}

	public List<Component> getPlan() {
		return _plan;
	}

}
