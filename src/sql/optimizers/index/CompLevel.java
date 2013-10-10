package sql.optimizers.index;

import plan_runner.components.Component;

public class CompLevel implements Comparable<CompLevel> {
	private final Component _comp;
	private int _level;

	public CompLevel(Component comp, int level) {
		_comp = comp;
		_level = level;
	}

	@Override
	public int compareTo(CompLevel cl) {
		final int otherLevel = cl.getLevel();
		return (new Integer(_level)).compareTo(new Integer(otherLevel));
	}

	public Component getComponent() {
		return _comp;
	}

	public int getLevel() {
		return _level;
	}

	public void setLevel(int level) {
		_level = level;
	}
}
