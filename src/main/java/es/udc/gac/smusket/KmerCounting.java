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

import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.storage.StorageLevel;

import es.udc.gac.smusket.util.Configuration;
import es.udc.gac.smusket.util.Constants;
import scala.Tuple2;

public final class KmerCounting {

	private static final Logger logger = LogManager.getLogger();
	private static final short DEFAULT_MAX_MULTI = 255;
	private static final double[] BUCKETS = new double[DEFAULT_MAX_MULTI + 2];

	static {
		for (int i = 0; i<BUCKETS.length; i++)
			BUCKETS[i]=i;
	}

	private KmerCounting() {}

	public static JavaPairRDD<Long,Short> runSingle(JavaRDD<Sequence> reads, Broadcast<Constants> constantsBC) {
		JavaPairRDD<Long, Short> kmersRDD = reads.flatMapToPair(new KmerGeneratorSingle(constantsBC))
				.reduceByKey((x, y) -> (short) ((x + y > DEFAULT_MAX_MULTI) ? DEFAULT_MAX_MULTI : x + y))
				.filter(kmer -> kmer._2 > 1);

		if (Configuration.SERIALIZED_RDD)
			kmersRDD.persist(StorageLevel.MEMORY_ONLY_SER());
		else
			kmersRDD.persist(StorageLevel.MEMORY_ONLY());

		return kmersRDD;
	}

	public static JavaPairRDD<Long,Short> runPaired(JavaPairRDD<Sequence, Sequence> pairedReads, Broadcast<Constants> constantsBC) {
		JavaPairRDD<Long, Short> kmersRDD = pairedReads.flatMapToPair(new KmerGeneratorPaired(constantsBC))
				.reduceByKey((x, y) -> (short) ((x + y > DEFAULT_MAX_MULTI) ? DEFAULT_MAX_MULTI : x + y))
				.filter(kmer -> kmer._2 > 1);

		if (Configuration.SERIALIZED_RDD)
			kmersRDD.persist(StorageLevel.MEMORY_ONLY_SER());
		else
			kmersRDD.persist(StorageLevel.MEMORY_ONLY());

		return kmersRDD;
	}

	public static short getMinMultiplicity(JavaPairRDD<Long,Short> kmersRDD) {

		// Build k-mer histogram and calculate maximum multiplicity
		JavaDoubleRDD multiplicityRDD = kmersRDD.values().mapToDouble(x -> x);

		if (Configuration.SERIALIZED_RDD)
			multiplicityRDD.persist(StorageLevel.MEMORY_ONLY_SER());
		else
			multiplicityRDD.persist(StorageLevel.MEMORY_ONLY());

		long[] histogram = multiplicityRDD.histogram(BUCKETS);
		int maxMulti = multiplicityRDD.max().intValue();
		multiplicityRDD.unpersist();
		multiplicityRDD = null;

		long nkmers = 0;
		System.out.println("K-mer frequency histogram (>=2):");
		for (int i = 2; i<histogram.length; i++) {
			System.out.print(histogram[i]+",");
			nkmers+=histogram[i];
		}
		System.out.println();
		logger.info("maximum multiplicity: {}", maxMulti);
		logger.info("number of k-mers (multi > 1): {}", nkmers);

		return findWatershed(histogram, maxMulti);
	}

	public static void generateKmers(Sequence read, short kSize, Kmer km, Kmer rep, int nkmers, List<Tuple2<Long, Short>> kmers) {
		int i;
		byte[] bases = read.getBases();
		km.setBases(bases);

		for (i = 0; i <= nkmers; i++) {
			if (i > 0)
				km.forwardBase(bases[i + kSize - 1]);

			rep.twin(km);
			if(km.lower(rep))
				rep.setBases(km); // rep = km

			kmers.add(new Tuple2<Long, Short>(rep.hash(), (short) 1));
		}
	}

	private static short findWatershed(long[] kmerFreqs, int length) {
		int valleyIndex, peakIndex, i;
		long minMulti, maxMulti;
		short lowerBoundMulti = 100;

		/* Reaching the plateau */
		valleyIndex = 2;
		for (i = valleyIndex + 1; i < length; i++) {
			if (kmerFreqs[i] > kmerFreqs[valleyIndex]) {
				break;
			}
			valleyIndex++;
		}
		valleyIndex++;
		
		/* Find the peak */
		peakIndex = valleyIndex;
		maxMulti = kmerFreqs[peakIndex];
		for (i = peakIndex + 1; i < length; i++) {
			if (kmerFreqs[i] > maxMulti) {
				maxMulti = kmerFreqs[i];
				peakIndex = i;
			}
		}

		/* Find the smallest frequency around the valley */
		minMulti = kmerFreqs[valleyIndex];
		for (i = valleyIndex + 1; i < peakIndex; i++) {
			if (kmerFreqs[i] < lowerBoundMulti) {
				continue;
			}
			if (kmerFreqs[i] < minMulti) {
				minMulti = kmerFreqs[i];
				valleyIndex = i;
			}
		}
		
		return (short) valleyIndex;
	}
}
