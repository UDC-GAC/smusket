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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.broadcast.Broadcast;

import es.udc.gac.smusket.util.Constants;
import scala.Tuple2;

public class KmerGeneratorSingle implements PairFlatMapFunction<Sequence, Long, Short> {

	/**
	 * 
	 */
	private static final long serialVersionUID = -8810510778920275591L;

	private Broadcast<Constants> constantsBC;

	public KmerGeneratorSingle(Broadcast<Constants> constantsBC) {
		this.constantsBC = constantsBC;
	}

	@Override
	public Iterator<Tuple2<Long, Short>> call(Sequence sequence) {
		Kmer.Constants = this.constantsBC.value();

		int nkmers = sequence.getLength() - constantsBC.value().K_SIZE;
		List<Tuple2<Long, Short>> kmers = new ArrayList<Tuple2<Long, Short>>(nkmers+1);

		// Generate all k-mers for this read
		KmerCounting.generateKmers(sequence, constantsBC.value().K_SIZE, new Kmer(), new Kmer(), nkmers, kmers);

		return kmers.iterator();
	}
}
