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
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.util.collection.BitSet;

import es.udc.gac.smusket.util.Constants;
import es.udc.gac.smusket.util.Options;

public final class ErrorCorrection {

	private static final Logger logger = LogManager.getLogger();
	private static final byte decodeToLower[] = { 'a', 'c', 'g', 't' };
	private static final byte decodeToUpper[] = { 'A', 'C', 'G', 'T' };

	private short kSize;
	private short maxIters;
	private short maxTrim;
	private short numErrors;
	private boolean lowercase;
	private Broadcast<Constants> constantsBC;
	private Map<Long, Short> kmersMap;

	public ErrorCorrection(Broadcast<Constants> constantsBC, Options options) {
		this.kSize = options.getKmerSize();
		this.maxIters = options.getMaxIters();
		this.maxTrim = options.getMaxTrim();
		this.numErrors = options.getNumErros();
		this.lowercase = options.isLowercase();
		this.constantsBC = constantsBC;
	}

	public static void run(JavaRDD<Sequence> reads, String outputFile, 
			Broadcast<ErrorCorrection> errorCorrectionBC,  Broadcast<Map<Long, Short>> kmersMapBC, Broadcast<Short> watershedBC) {
		// Correct reads
		JavaRDD<Sequence> correctedReads = reads.map(new Function<Sequence, Sequence>() {
			/**
			 * 
			 */
			private static final long serialVersionUID = -2907044707468915318L;

			@Override
			public Sequence call(Sequence read) {
				Kmer.Constants = errorCorrectionBC.value().constantsBC.value();

				// Get k-mer map instance
				errorCorrectionBC.value().kmersMap = kmersMapBC.value();

				// Correct sequence
				errorCorrectionBC.value().CorrectCore(read, watershedBC.value());

				return read;
			}
		});

		// Write corrected reads
		correctedReads.saveAsTextFile(outputFile);
	}

	/*
	 * Core function of error correction
	 */
	private void CorrectCore(Sequence sequence, short watershed) {
		List<Correction> corrections = new ArrayList<Correction>();
		List<Correction> correctionsAux = new ArrayList<Correction>();
		List<MutablePair<Byte,Short>> bases = new ArrayList<MutablePair<Byte,Short>>();
		List<MutablePair<Byte,Short>> lbases = new ArrayList<MutablePair<Byte,Short>>();
		PriorityQueue<CorrectRegion> regionsPingComp = new PriorityQueue<CorrectRegion>(new CorrectRegionComp.CorrectRegionPingComp());
		PriorityQueue<CorrectRegion> regionsPongComp = new PriorityQueue<CorrectRegion>(new CorrectRegionComp.CorrectRegionPongComp());
		PriorityQueue<CorrectRegion> regions;
		Correction corr = new Correction();
		Correction auxCorr;
		Kmer km = new Kmer();
		Kmer lkm = new Kmer();
		Kmer rep = new Kmer();
		Kmer lrep = new Kmer();
		Kmer kmAux1 = new Kmer();
		Kmer kmAux2 = new Kmer();
		BitSet solids = new BitSet(sequence.getLength());
		int[][] votes = new int[sequence.getLength()][4];
		int i, j, nerr, numCorrections, maxVote, minVote = 3;
		boolean pingComp;

		logger.debug("Correcting {}", () ->  sequence.basesToString());

		/* Stage 1: conservative correcting using two-sided correcting */
		for (i = 0; i < maxIters; i++) {
			if ((numCorrections = CorrectCoreTwoSides(sequence, corr, bases, lbases, km, rep, lkm, lrep, 
					kmAux1, kmAux2, solids, watershed)) == 0) {
				/* error free */
				logger.debug("Error free (1)");
				return;
			}

			if (numCorrections < 0) // Negative value means more than 1 error was found
				break;

			/* Make changes to the input sequence */
			logger.debug("Correction(1): base {}, index {}", corr.getBase(), corr.getIndex());
			sequence.getBases()[corr.getIndex()] = lowercase? decodeToLower[corr.getBase()] : decodeToUpper[corr.getBase()];
		}

		/* Stage 2: aggressive correcting from one side */
		for (nerr = 1; nerr <= numErrors; nerr++) {
			pingComp = true;
			for (i = 0; i < maxIters; i++) {
				regions = pingComp? regionsPingComp : regionsPongComp;
				pingComp = !pingComp;

				if (CorrectCoreOneSide(sequence, corrections, correctionsAux, bases, km, rep, lkm, lrep, nerr, numErrors - nerr + 1, 
						regions, solids, watershed) == true) {
					/* error free */
					logger.debug("Error free (2)");
					return;
				}

				if (corrections.size() == 0)
					break;

				/* Make changes to the input sequence */
				for(j = 0; j < corrections.size(); j++) {
					auxCorr = corrections.get(j);
					logger.debug("Correction(2): base {}, index {}", auxCorr.getBase(), auxCorr.getIndex());
					sequence.getBases()[auxCorr.getIndex()] = lowercase? decodeToLower[auxCorr.getBase()] : decodeToUpper[auxCorr.getBase()];
				}
			}

			/* Stage 3: voting and fix erroneous bases */
			maxVote = voteErroneousRead(sequence, km, rep, lkm, lrep, votes, watershed);

			if (maxVote == 0) {
				/* error free */
				logger.debug("Error free (3)");
				return;
			} else if (maxVote >= minVote) {
				fixErroneousBase(sequence.getLength(), corrections, votes, maxVote);

				/* Make changes to the input sequence */
				for(j = 0; j < corrections.size(); j++) {
					auxCorr = corrections.get(j);
					logger.debug("Correction(3): base {}, index {}", auxCorr.getBase(), auxCorr.getIndex());
					sequence.getBases()[auxCorr.getIndex()] = lowercase? decodeToLower[auxCorr.getBase()] : decodeToUpper[auxCorr.getBase()];
				}
			}
		}

		if (maxTrim > 0) {
			MutablePair<Short,Short> longestRegion = new MutablePair<Short,Short>((short)0, (short)-1);

			/* Attempt to trim the sequence using the largest k-mer size */
			if (isTrimmable(sequence, km, rep, longestRegion, watershed)) {
				logger.debug("Trimmable");

				int seqLen = longestRegion.getRight() - longestRegion.getLeft();

				if (longestRegion.getLeft() > 0) {
					sequence.setBases(sequence.getBases(), longestRegion.getLeft(), seqLen);

					/* Base quality scores */
					if (sequence.getQuals() != null)
						sequence.setQuals(sequence.getQuals(), longestRegion.getLeft(), seqLen);
				}
			}
		}
	}

	/*
	 * Error correcting that only allows 1 error in any k-mer of a read
	 */
	private int CorrectCoreTwoSides(Sequence sequence, Correction correction, 
			List<MutablePair<Byte,Short>> bases, List<MutablePair<Byte,Short>> lbases, 
			Kmer km, Kmer rep, Kmer lkm, Kmer lrep, Kmer kmAux1, Kmer kmAux2, BitSet solids, short watershed) {
		byte[] seq = sequence.getBases();
		int seqLen = sequence.getLength();
		int size = seqLen - kSize;
		int numBases, lnumBases, ipos, i, j, numCorrections;
		byte base, lbase;
		boolean revcomp = true;
		boolean lrevcomp = true;

		solids.clear();

		/* Iterate each k-mer to check its solidity */
		km.setBases(seq);
		logger.debug("km {}", () -> km.toString());

		for (ipos = 0; ipos <= size; ipos++) {
			if (ipos > 0)
				km.forwardBase(seq[ipos + kSize - 1]);

			rep.twin(km);
			if(km.lower(rep))
				rep.setBases(km); // rep = km

			if (getMulti(rep.hash()) >= watershed) {
				for (i = ipos; i < ipos + kSize; i++)
					solids.set(i);
			}
		}

		/* if the read is error-free */
		if (solids.cardinality() == seqLen)
			return 0;

		/* Fix the unique errors relying on k-mer neighboring information */
		km.setBases(seq);
		lkm.setBases(km);

		/* For bases from index 0 to index size (seqLen - kSize) */
		for (ipos = 0; ipos <= size; ipos++) {
			if (ipos > 0)
				km.forwardBase(seq[ipos + kSize - 1]);

			if (ipos >= kSize)
				lkm.forwardBase(seq[ipos]);

			if (solids.get(ipos))
				continue;

			/* Try to find all mutations for the rightmost k-mer */
			rep.twin(km);
			revcomp = true;
			if(km.lower(rep)) {
				rep.setBases(km); // rep = km
				revcomp = false;
			}
			numBases = selectAllMutations(rep, kmAux1, kmAux2, revcomp, 0, bases, watershed);

			/* Try to find all mutations for the leftmost k-mer */
			rep.twin(lkm);
			revcomp = true;
			if(lkm.lower(rep)) {
				rep.setBases(lkm); // rep = lkm
				revcomp = false;
			}
			lnumBases = selectAllMutations(rep, kmAux1, kmAux2, revcomp, ipos >= kSize? kSize - 1 : ipos, lbases, watershed);

			numCorrections = 0;

			/* If the two bases are the same, this base is modified */
			for (i = 0; i < numBases && numCorrections <= 1; i++) {
				base = bases.get(i).left;
				for (j = 0; j < lnumBases; j++) {
					lbase = lbases.get(j).left;
					if (base == lbase) {
						numCorrections++;
						correction.setBase(base);
						correction.setIndex((short)ipos);
					}
				}
			}

			/* Check if only one correction is found */
			if (numCorrections == 1)
				return 1;
		}

		/* For the remaining k-1 bases */
		rep.twin(km);
		revcomp = true;
		if(km.lower(rep)) {
			rep.setBases(km); // rep = km
			revcomp = false;
		}

		for (; ipos < seqLen; ipos++) {
			lkm.forwardBase(seq[ipos]);

			if (solids.get(ipos))
				continue;

			/* Try to fix using the current k-mer */
			numBases = selectAllMutations(rep, kmAux1, kmAux2, revcomp, ipos - size, bases, watershed);

			/* Try to fix using the left k-mer */
			lrep.twin(lkm);
			lrevcomp = true;
			if(lkm.lower(lrep)) {
				lrep.setBases(lkm); // lrep = lkm
				lrevcomp = false;
			}
			lnumBases = selectAllMutations(lrep, kmAux1, kmAux2, lrevcomp, kSize - 1, lbases, watershed);

			numCorrections = 0;

			/* If the two bases are the same, this base is modified */
			for (i = 0; i < numBases && numCorrections <= 1; i++) {
				base = bases.get(i).left;
				for (j = 0; j < lnumBases; j++) {
					lbase = lbases.get(j).left;
					if (base == lbase) {
						numCorrections++;
						correction.setBase(base);
						correction.setIndex((short)ipos);
					}
				}
			}

			/* Check if only one correction is found */
			if (numCorrections == 1)
				return 1;
		}

		/* Not error-free */
		return -1;
	}

	/*
	 * Core function of error correction without gaps
	 */
	private boolean CorrectCoreOneSide(Sequence sequence, List<Correction> corrections, List<Correction> correctionsAux, 
			List<MutablePair<Byte,Short>> bases, Kmer km, Kmer rep, Kmer kmAux1, Kmer kmAux2,int maxErrorPerKmer, 
			int stride, PriorityQueue<CorrectRegion> regions, BitSet solids, short watershed) {
		MutablePair<Byte,Short> best = new MutablePair<Byte,Short>();
		byte[] seq = sequence.getBases();
		int seqLen = sequence.getLength();
		int size = seqLen - kSize;
		int numBases, ipos, i, targetPos, lastPosition, numCorrectionsPerKmer;
		int leftKmer = -1, rightKmer = -1;
		boolean solidRegion = false, revcomp, done;
		CorrectRegion region;

		solids.clear();
		corrections.clear();
		regions.clear();

		km.setBases(seq);
		logger.debug("km {}", () -> km.toString());

		for (ipos = 0; ipos <= size; ipos++) {
			if (ipos > 0)
				km.forwardBase(seq[ipos + kSize - 1]);

			rep.twin(km);
			if(km.lower(rep))
				rep.setBases(km); // rep = km

			if (getMulti(rep.hash()) >= watershed) {
				// Found a healthy k-mer!
				if (!solidRegion) {
					solidRegion = true;
					leftKmer = rightKmer = ipos;
				} else {
					rightKmer++;
				}

				for (i = ipos; i < ipos + kSize; i++)
					solids.set(i);
			} else {
				// Save the trusted region
				if (leftKmer >= 0) {
					regions.add(new CorrectRegion(leftKmer, rightKmer));
					leftKmer = rightKmer = -1;
				}
				solidRegion = false;
			}
		}

		if (solidRegion && leftKmer >= 0)
			regions.add(new CorrectRegion(leftKmer, rightKmer));

		if (regions.size() == 0)
			/* This read is error-free */
			return true;

		if (logger.isDebugEnabled()) {
			logger.debug("solids {}", () -> solids.cardinality());

			for(CorrectRegion reg: regions)
				logger.debug(reg.getLeftKmer() + " " + reg.getRightKmer() + " " + (reg.getRightKmer() - reg.getLeftKmer() + 1));
		}

		/* Calculate the minimal number of votes per base */
		while (regions.size() > 0) {
			/* Get the region with the highest priority */
			region = regions.poll();
			leftKmer = region.getLeftKmer();
			rightKmer = region.getRightKmer();

			/* Form the starting k-mer */
			km.setBases(seq, rightKmer);

			if (logger.isDebugEnabled())
				logger.debug("km: {}, leftKmer: {}, rightKmer: {}", km.toString(), leftKmer, rightKmer);

			lastPosition = -1;
			numCorrectionsPerKmer = 0;
			correctionsAux.clear();

			for (ipos = rightKmer + 1; ipos <= size; ipos++) {
				targetPos = ipos + kSize - 1;

				/* Check the solids of the k-mer */
				if (solids.get(targetPos)) {
					/* If it reaches another trusted region */
					break;
				}

				km.forwardBase(seq[targetPos]);

				rep.twin(km);
				revcomp = true;
				if(km.lower(rep)) {
					rep.setBases(km); // rep = km
					revcomp = false;
				}

				if (getMulti(rep.hash()) < watershed) {
					/* Select all possible mutations */
					numBases = selectAllMutations(rep, kmAux1, kmAux2, revcomp, kSize - 1, bases, watershed);

					/* Start correcting */
					done = false;

					if (numBases == 1) {
						byte base = bases.get(0).left;
						correctionsAux.add(new Correction((short)targetPos, base));
						/* Set the last base */
						km.setBase(base, kSize - 1);
						done = true;
						logger.debug("km: {}", () ->  km.toString());
						logger.debug("base: {}, tpos: {}, multi: {}", base, targetPos, bases.get(0).right);
					} else {
						/* Select the best substitution */
						best.setLeft((byte)0);
						best.setRight((short)0);

						for (i = 0; i < numBases; i++) {
							byte base = bases.get(i).left;
							kmAux1.setBases(km);
							kmAux1.setBase(base, kSize - 1);

							if (successor(seq, ipos + 1, seqLen - ipos - 1, kmAux1, kmAux2, stride, watershed)) {
								/* Check the multiplicity */
								if (best.getRight() < bases.get(i).right) {
									best = bases.get(i);
								}
							}
						}

						/* If finding a best one */
						if (best.getRight() > 0) {
							correctionsAux.add(new Correction((short)targetPos, best.getLeft()));
							km.setBase(best.getLeft(), kSize - 1);
							done = true;
							logger.debug("km: {}", () ->  km.toString());
							logger.debug("tpos: {}, numBases: {}, best.left: {}, best.right: {}", targetPos, numBases, best.getLeft(), best.getRight());
						}
					}

					/* If finding one correction */
					if (done) {
						/* Recording the position */
						if (lastPosition < 0)
							lastPosition = targetPos;

						/* Check the number of errors in any k-mer length */
						if (targetPos - lastPosition < kSize) {
							/* Increase the number of corrections */
							numCorrectionsPerKmer++;

							if (numCorrectionsPerKmer > maxErrorPerKmer) {
								for (i = 0; i < numCorrectionsPerKmer; i++) {
									correctionsAux.remove(correctionsAux.size() - 1);
								}
								break;
							}
						} else {
							lastPosition = targetPos;
							numCorrectionsPerKmer = 0;
						}
						continue;
					}
					break; // Check the next region
				} // end if
			} // end for

			/* Save the corrections in this region */
			corrections.addAll(correctionsAux);

			/* Towards the beginning of the sequence from this position */
			lastPosition = (correctionsAux.size() > 0)? correctionsAux.get(0).getIndex() : -1;
			numCorrectionsPerKmer = 0;
			correctionsAux.clear();

			if (leftKmer > 0) {
				km.setBases(seq, leftKmer);

				for (ipos = leftKmer - 1; ipos >= 0; ipos--) {
					if (solids.get(ipos))
						/* If it reaches another trusted region */
						break;

					km.backwardBase(seq[ipos]);

					rep.twin(km);
					revcomp = true;
					if(km.lower(rep)) {
						rep.setBases(km); // rep = km
						revcomp = false;
					}

					if (getMulti(rep.hash()) < watershed) {
						/* Select all possible mutations */
						numBases = selectAllMutations(rep, kmAux1, kmAux2, revcomp, 0, bases, watershed);

						/* Start correcting */
						done = false;

						if (numBases == 1) {
							byte base = bases.get(0).left;
							correctionsAux.add(new Correction((short)ipos, base));
							/* Set the last base */
							km.setBase(base, 0);
							done = true;
							logger.debug("km: {}", () ->  km.toString());
							logger.debug("base: {}, ipos: {}, multi: {}", base, ipos, bases.get(0).right);
						} else {
							/* Select the best substitution */
							best.setLeft((byte)0);
							best.setRight((short)0);

							for (i = 0; i < numBases; i++) {
								byte base = bases.get(i).left;
								kmAux1.setBases(km);
								kmAux1.setBase(base, 0);

								if (predecessor(seq, 0, ipos - 1, kmAux1, kmAux2, stride, watershed)) {
									if (best.getRight() < bases.get(i).right) {
										best = bases.get(i);
									}
								}
							}

							/* If finding a best one */
							if (best.getRight() > 0) {
								correctionsAux.add(new Correction((short)ipos, best.getLeft()));
								km.setBase(best.getLeft(), 0);
								done = true;
								logger.debug("km: {}", () ->  km.toString());
								logger.debug("ipos: {}, numBases: {}, best.left: {}, best.right: {}", ipos, numBases, best.getLeft(), best.getRight());
							}
						}

						/* If finding one correction */
						if (done) {
							/* Recording the position */
							if (lastPosition < 0)
								lastPosition = ipos;

							/* Check the number of errors in any k-mer length */
							if (lastPosition - ipos < kSize) {
								/* Increase the number of corrections */
								numCorrectionsPerKmer++;

								if (numCorrectionsPerKmer > maxErrorPerKmer) {
									for (i = 0; i < numCorrectionsPerKmer; i++) {
										correctionsAux.remove(correctionsAux.size() - 1);
									}
									break;
								}
							} else {
								lastPosition = ipos;
								numCorrectionsPerKmer = 0;
							}
							continue;
						}
						break; // Check the next region
					} // end if
				} // end for
			} // end if leftKmer > 0

			/* Save the corrections in this region */
			corrections.addAll(correctionsAux);
		} // end while

		/* Not error-free */
		return false;
	}

	private void fixErroneousBase(int seqLen, List<Correction> corrections, int[][] votes, int maxVote) {
		int alternativeBase, pos, base;

		corrections.clear();

		/* Find the qualified base mutations */
		for (pos = 0; pos < seqLen; pos++) {
			alternativeBase = -1;
			for (base = 0; base < 4; base++) {
				if (votes[pos][base] == maxVote) {
					if (alternativeBase == -1) {
						alternativeBase = base;
					} else {
						alternativeBase = -1;
						break;
					}
				}
			}

			if (alternativeBase >= 0)
				corrections.add(new Correction((short)pos, (byte)alternativeBase));
		}
	}

	private int voteErroneousRead(Sequence sequence, Kmer km, Kmer rep, Kmer alternativeKmer, Kmer alternativeSearch, 
			int[][] votes, short watershed) {
		// Cast votes for mutations
		int ipos, off, baseIndex, originalBase, base, vote, maxVote = 0;
		byte[] seq = sequence.getBases();
		int seqLen = sequence.getLength();
		boolean errorFree = true, revcomp;

		km.setBases(seq);

		// Initialize the votes matrix
		for (ipos = 0; ipos < seqLen; ipos++) {
			for (base = 0; base < 4; base++) {
				votes[ipos][base] = 0;
			}
		}

		for (ipos = 0; ipos <= seqLen - kSize; ipos++) {
			if (ipos > 0)
				km.forwardBase(seq[ipos + kSize - 1]);

			rep.twin(km);
			revcomp = true;
			if(km.lower(rep)) {
				rep.setBases(km); // rep = km
				revcomp = false;
			}

			if (getMulti(rep.hash()) >= watershed)
				continue;

			errorFree = false;

			// For each offset
			for (off = 0; off < kSize; off++) {
				/* For each possible mutation */
				baseIndex = revcomp ? kSize - off - 1 : off;
				originalBase = rep.getBase(baseIndex);

				/* Get all possible alternatives */
				for (base = (originalBase + 1) & 3; base != originalBase; base = (base + 1) & 3) {
					alternativeKmer.setBases(rep);
					alternativeKmer.setBase((byte)base, baseIndex);
					alternativeSearch.twin(alternativeKmer);

					if (alternativeKmer.lower(alternativeSearch))
						alternativeSearch.setBases(alternativeKmer); // alternativeSearch = alternativeKmer;

					if (getMulti(alternativeSearch.hash()) >= watershed) {
						votes[ipos + off][revcomp ? base ^ 3 : base]++;
					}
				}
			}
		}

		/* Error-free read */
		if (errorFree)
			return 0;

		/* Select the maximum vote */
		for (ipos = 0; ipos < seqLen; ipos++) {
			for (base = 0; base < 4; base++) {
				vote = votes[ipos][base];
				if (vote > maxVote)
					maxVote = vote;
			}
		}

		logger.debug("maxVote: {}", maxVote);
		return maxVote;
	}

	private boolean isTrimmable(Sequence sequence, Kmer km, Kmer rep, MutablePair<Short,Short> region, short watershed) {
		byte[] seq = sequence.getBases();
		int seqLen = sequence.getLength();
		int ipos, leftKmer = -1, rightKmer = -1;
		boolean trustedRegion = true;

		km.setBases(seq);

		/* Check the trustiness of each k-mer */
		for (ipos = 0; ipos <= seqLen - kSize; ipos++) {
			if (ipos > 0)
				km.forwardBase(seq[ipos + kSize - 1]);

			rep.twin(km);
			if(km.lower(rep))
				rep.setBases(km); // rep = km

			if (getMulti(rep.hash()) >= watershed) {
				// Found a healthy k-mer!
				if (trustedRegion) {
					trustedRegion = false;
					leftKmer = rightKmer = ipos;
				} else {
					rightKmer++;
				}
			} else {
				// Save the trusted region
				if (leftKmer >= 0) {
					if (rightKmer - leftKmer > region.getRight() - region.getLeft()) {
						region.setLeft((short)leftKmer);
						region.setRight((short)rightKmer);
					}
					leftKmer = rightKmer = -1;
				}
				trustedRegion = true;
			}
		}

		if (trustedRegion == false && leftKmer >= 0) {
			if (rightKmer - leftKmer > region.getRight() - region.getLeft()) {
				region.setLeft((short)leftKmer);
				region.setRight((short)rightKmer);
			}
		}

		if (region.getRight() < region.getLeft())
			return false;

		/* Check the longest trusted region */
		region.setRight((short)(region.getRight() + kSize));

		if (seqLen - (region.getRight() - region.getLeft()) > maxTrim)
			return false;

		return true;
	}

	private int selectAllMutations(Kmer km, Kmer alternativeKmer, Kmer alternativeSearch, boolean revcomp, int index, 
			List<MutablePair<Byte,Short>> bases, short watershed) {
		int base;
		short multi;

		bases.clear();

		/* Get the original base at index */
		int baseIndex = revcomp ? kSize - 1 - index : index;
		int originalBase = km.getBase(baseIndex);
		alternativeKmer.setBases(km);

		/* Get all possible alternatives */
		for (base = (originalBase + 1) & 3; base != originalBase; base = (base + 1) & 3) {
			alternativeKmer.setBase((byte)base, baseIndex);
			alternativeSearch.twin(alternativeKmer);

			if (alternativeKmer.lower(alternativeSearch))
				alternativeSearch.setBases(alternativeKmer); // alternativeSearch = alternativeKmer;

			/* Get k-mer multiplicity */
			multi = getMulti(alternativeSearch.hash());

			if (multi >= watershed)
				bases.add(new MutablePair<Byte,Short>(revcomp? (byte) (base ^ 3) : (byte)base, multi));
		}

		return bases.size();
	}

	/*
	 * Check the correctness of its successing k-mer
	 */
	private boolean successor(byte[] seq, int offset, int seqLen, Kmer km, Kmer rep, int dist, short watershed) {
		if (seqLen < kSize || dist <=0)
			return true;

		int endPos = Math.min(seqLen - kSize, dist - 1);
		int ipos;

		for (ipos = 0; ipos <= endPos; ipos++) {
			km.forwardBase(seq[offset + ipos + kSize - 1]);

			rep.twin(km);
			if (km.lower(rep))
				rep.setBases(km); // rep = km

			if (getMulti(rep.hash()) < watershed)
				return false;
		}

		return true;
	}

	/*
	 * Check the correctness of its precessing k-mer
	 */
	private boolean predecessor(byte[] seq, int offset, int seqLen, Kmer km, Kmer rep, int dist, short watershed) {
		if (seqLen <= 0 || dist <=0)
			return true;

		int startPos = Math.max(0, seqLen - dist);
		int ipos;

		for (ipos = seqLen - 1; ipos >= startPos; ipos--) {
			km.backwardBase(seq[offset + ipos]);

			rep.twin(km);
			if (km.lower(rep))
				rep.setBases(km); // rep = km

			if (getMulti(rep.hash()) < watershed)
				return false;
		}

		return true;
	}

	private short getMulti(long key) {
		Short value = kmersMap.get(key);
		return (value == null)? 0 : value;
	}
}
