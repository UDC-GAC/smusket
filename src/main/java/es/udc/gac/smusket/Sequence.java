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
package es.udc.gac.smusket;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;

public class Sequence {

	public static final String FASTQ_COMMENT_LINE = "\n+\n";

	private StringBuilder sb;
	private byte[] bases;
	private byte[] quals;

	public Sequence(String name, int length) {
		this.sb = new StringBuilder(length);
		sb.append(name);
	}

	public void setBases(byte[] bases, int offset, int length) {
		this.bases = new byte[length];
		System.arraycopy(bases, offset, this.bases, 0, length);
	}

	public void setBases(ByteArrayOutputStream bases) {
		this.bases = bases.toByteArray();
	}

	public void setQuals(byte[] quals, int offset, int length) {
		this.quals = new byte[length];
		System.arraycopy(quals, offset, this.quals, 0, length);
	}

	public int getLength() {
		return bases.length;
	}

	public byte[] getBases() {
		return bases;
	}

	public byte[] getQuals() {
		return quals;
	}

	public String basesToString() {
		return new String(bases, StandardCharsets.UTF_8);
	}

	@Override
	public String toString() {

		char[] output = new char[bases.length];

		// Print bases
		decode(bases, output);
		sb.append(output);

		// Print quality scores if available
		if (quals != null) {
			decode(quals, output);
			sb.append(FASTQ_COMMENT_LINE).append(output);
		}

		return sb.toString();
	}

	public static void decode(byte[] input, char[] output) {
		for (int i = 0; i < input.length; i++)
			output[i] = (char) input[i];
	}
}
