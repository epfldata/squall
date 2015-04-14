/*
 * Copyright (c) 2011-2015 EPFL DATA Laboratory
 * Copyright (c) 2014-2015 The Squall Collaboration (see NOTICE)
 *
 * All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ch.epfl.data.squall.utilities.thetajoin.dynamic;

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
