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
package es.udc.gac.smusket.util;

public final class Constants {
	public short K_SIZE;
	public int K_BYTES;
	public int K_BYTES_MINUS_1;
	public int K_BYTES_DIV_2;
	public int SHIFT;
	public int SHIFT_LEFT;
	public int K_MODMASK;
	public int MAX_K_DIV_4;

	public Constants(Options options) {
		this.K_SIZE = options.getKmerSize();
		this.K_BYTES = (K_SIZE + 3) >> 2;
		this.K_BYTES_MINUS_1 = K_BYTES - 1;
		this.K_BYTES_DIV_2 = K_BYTES >> 1;
		this.SHIFT = 2 * ((K_SIZE + 3) & 3);
		this.SHIFT_LEFT = 8 * K_BYTES - 2 * K_SIZE;
		this.K_MODMASK = (1 << (2 * (((K_SIZE & 3) > 0) ? K_SIZE & 3 : 4))) - 1;
		this.MAX_K_DIV_4 = (K_SIZE % 4 == 0)? (short) (K_SIZE / 4) : (short) ((K_SIZE / 4) + 1);
	}
}
