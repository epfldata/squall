package ch.epfl.data.squall.utilities.thetajoin_dynamic;

import java.io.Serializable;

import org.apache.log4j.Logger;

public class ThetaState implements Serializable {
	public static enum state {
		NORMAL, FLUSHING, DATAMIGRATING
	}

	public static void printState(String component, state st) {
		switch (st) {
		case NORMAL:
			LOG.info(component + " currentState is Normal");
			break;
		case FLUSHING:
			LOG.info(component + " currentState is Flushing");
			break;
		case DATAMIGRATING:
			LOG.info(component + " currentState is Datamigrating");
			break;
		default:
			LOG.info(component + " currentState is not valid");
			break;
		}
	}

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;;

	private static Logger LOG = Logger.getLogger(ThetaState.class);
}
