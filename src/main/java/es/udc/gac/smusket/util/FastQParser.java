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

import java.nio.charset.StandardCharsets;

import es.udc.gac.smusket.Sequence;

public class FastQParser extends SequenceParser {

	@Override
	public Sequence parseSequence(byte[] bytes, int offset, int length) {

		// Read the sequence name
		if (!(offset < length)) {
			String read = new String(bytes, offset, length, StandardCharsets.UTF_8);
			throw new IllegalArgumentException("Wrong Sequence format for " + read 
					+ ": no name available");
		}

		int lineLength = nextToken(bytes, offset, length) + 1; //Include line feed
		Sequence seq = new Sequence(new String(bytes, offset, lineLength, StandardCharsets.UTF_8), length);
		offset += lineLength;

		// Read the sequence bases
		if (!(offset < length)) {
			String read = new String(bytes, offset, length, StandardCharsets.UTF_8);
			throw new IllegalArgumentException("Wrong Sequence format for " + read
					+ ": only name available");
		}

		lineLength = nextToken(bytes, offset, length);
		seq.setBases(bytes, offset, lineLength);
		offset += lineLength + Sequence.FASTQ_COMMENT_LINE.length(); // Skip line feed and comment line

		// Read the qualities
		if (!(offset < length)) {
			String read = new String(bytes, offset, length, StandardCharsets.UTF_8);
			throw new IllegalArgumentException("Wrong Sequence format for " + read
					+ ": no qualities available");
		}

		lineLength = nextToken(bytes, offset, length);
		seq.setQuals(bytes, offset, lineLength);

		return seq;
	}
}
