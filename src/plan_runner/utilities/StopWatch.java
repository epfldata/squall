package plan_runner.utilities;

import org.apache.log4j.Logger;

/*
 Copyright (c) 2005, Corey Goldberg

 StopWatch.java is free software; you can redistribute it and/or modify
 it under the terms of the GNU General Public License as published by
 the Free Software Foundation; either version 2 of the License, or
 (at your option) any later version.
 */

public class StopWatch {
	private static Logger LOG = Logger.getLogger(StopWatch.class);

	// sample usage
	public static void main(String[] args) {
		final StopWatch s = new StopWatch();
		s.start();
		// code you want to time goes here
		try {
			Thread.sleep(1000);
		} catch (final InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		// s.stop();
		LOG.info("elapsed time in milliseconds: " + s.getElapsedTime());
	}

	private long startTime = 0;
	private long stopTime = 0;

	private boolean running = false;

	// elaspsed time in milliseconds
	public long getElapsedTime() {
		long elapsed;
		if (running)
			elapsed = (System.currentTimeMillis() - startTime);
		else
			elapsed = (stopTime - startTime);
		return elapsed;
	}

	// elaspsed time in seconds
	public long getElapsedTimeSecs() {
		long elapsed;
		if (running)
			elapsed = ((System.currentTimeMillis() - startTime) / 1000);
		else
			elapsed = ((stopTime - startTime) / 1000);
		return elapsed;
	}

	public void start() {
		startTime = System.currentTimeMillis();
		running = true;
	}

	public void stop() {
		stopTime = System.currentTimeMillis();
		running = false;
	}
}