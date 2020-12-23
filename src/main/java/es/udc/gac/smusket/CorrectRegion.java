/*
 * Copyright (C) 2021 Universidade da Coruña
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
package es.udc.gac.smusket;

public final class CorrectRegion {
	private int leftKmer;
	private int rightKmer;
	private int length;

	public CorrectRegion(int leftKmer, int rightKmer) {
		this.leftKmer = leftKmer;
		this.rightKmer = rightKmer;
		this.length = rightKmer - leftKmer + 1;
	}

	public int length() {
		return length;
	}

	public int getLeftKmer() {
		return leftKmer;
	}

	public int getRightKmer() {
		return rightKmer;
	}
}
