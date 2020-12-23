/*
 * Copyright (C) 2019 Universidade da Coruña
 *
 * This file is part of SparkMusket.
 *
 * SparkMusket is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * SparkMusket is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with SparkMusket. If not, see <http://www.gnu.org/licenses/>.
 */
package es.udc.gac.smusket.util;

public class Timer {

	public static final int MAX_TIMERS = 8;
	private static final double TENTO9 = Double.valueOf(1000000000L);
	private double startTime[] = new double[MAX_TIMERS];
	private double elapsedTime[] = new double[MAX_TIMERS];
	private double totalTime[] = new double[MAX_TIMERS];

	public Timer() {
		for(int i=0; i<MAX_TIMERS; i++) {
			startTime[i] = 0;
			elapsedTime[i] = 0;
			totalTime[i] = 0;
		}
	}

	public void start(int n) {
		startTime[n] = System.nanoTime();
	}

	public void stop(int n) {
		elapsedTime[n] = System.nanoTime() - startTime[n];
		totalTime[n] += elapsedTime[n];
	}

	public double readLast(int n) {
		return elapsedTime[n] / Timer.TENTO9;
	}

	public double readTotal(int n) {
		return totalTime[n] / Timer.TENTO9;
	}

	public void reset(int n) {
		elapsedTime[n]=startTime[n]=totalTime[n] = 0;
	}

	public void resetAll() {
		for(int i=0; i<MAX_TIMERS; i++)
			reset(i);
	}
}