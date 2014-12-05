package plan_runner.thetajoin.dynamic.advisor.tests;



import java.io.BufferedReader;

import java.io.IOException;
import java.io.InputStreamReader;

import plan_runner.thetajoin.dynamic.advisor.Action;
import plan_runner.thetajoin.dynamic.advisor.Advisor;
import plan_runner.thetajoin.dynamic.advisor.Maybe;


public class AdvisorMain {
	public static void main(String[] args) throws IOException {
		/*
		Advisor m = new Advisor(8, 4, 2);
	    BufferedReader stdin = new BufferedReader(new InputStreamReader(System.in));
	    while (true) {
	        String line = stdin.readLine();
	        line = line.trim();
	        if (line.length() == 0) break;
	        String[] parts = line.split(" ");
	        try {
	            m.updateTuples(Integer.parseInt(parts[0]), Integer.parseInt(parts[1]));
	        } catch (Exception e) {
	            System.err.println("Invalid input");
	            continue;
	        }
	        Maybe<Action> action = m.adviseAndUpdateDimensions();
	        if (!action.isNone()) {
	            LOG.info("Dimensions: " + action.get().getNewRows()
	            		+ " " + action.get().getNewColumns());
	        } else {
	            LOG.info("No change.");
	        }
	    }
		
		LOG.info(Advisor.isLeader(4, 2, 2, 6));
		*/
	}

}
