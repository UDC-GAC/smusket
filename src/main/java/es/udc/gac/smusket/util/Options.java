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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import es.udc.gac.smusket.RunSparkMusket;
import es.udc.gac.smusket.util.SequenceParserFactory.FileFormat;

public class Options {

	private static final Logger logger = LogManager.getLogger();

	private String inputFile1 = null;
	private String inputFile2 = null;
	private String outputFile1 = null;
	private String outputFile2 = null;
	private FileFormat inputFormat = FileFormat.FILE_FORMAT_UNKNOWN;
	private boolean paired = false;
	private short kmerSize = 21;
	private short watershed = -1;
	private short maxIters = 2;
	private short maxTrim = 0;
	private short numErrors = 4;
	private boolean lowercase = false;
	private int partitionsPerCore = 32;

	public Options() {}

	public String getInputFile1() {
		return inputFile1;
	}

	public String getInputFile2() {
		return inputFile2;
	}

	public String getOutputFile1() {
		return outputFile1;
	}

	public String getOutputFile2() {
		return outputFile2;
	}

	public FileFormat getInputFileFormat() {
		return inputFormat;
	}

	public boolean isPaired() {
		return paired;
	}

	public short getKmerSize() {
		return kmerSize;
	}

	public short getWatershed() {
		return watershed;
	}

	public void setWatershed(short watershed) {
		this.watershed = watershed;
	}

	public short getMaxIters() {
		return maxIters;
	}

	public short getMaxTrim() {
		return maxTrim;
	}

	public short getNumErros() {
		return numErrors;
	}

	public boolean isLowercase() {
		return lowercase;
	}

	public int getPartitions() {
		return partitionsPerCore;
	}

	private void printUsage(String name) {
		System.out.println();
		System.out.println(
				"Usage: " + name + " -i inputFile1 [-o outputFile1] [-p inputFile2] [-r outputFile2] [options]");
		System.out.println("Input:");
		System.out.println(
				"    -i <string> inputFile1 (input file in FASTQ/FASTA format)");
		System.out.println(
				"    -p <string> inputFile2 (input file in FASTQ/FASTA format for paired-end scenarios)");
		System.out.println(
				"    -q (input sequence files are in FASTQ format, default: autodetect)");
		System.out.println(
				"    -f (input sequence files are in FASTA format, default: autodetect)");
		System.out.println("Output:");
		System.out.println(
				"    -o <string> outputFile1 (output file in FASTQ/FASTA format, default = inputFile1.corrected)");
		System.out.println(
				"    -r <string> outputFile2 (output file in FASTQ/FASTA format for paired-end scenarios, default = inputFile2.corrected)");
		System.out.println("Computation:");
		System.out.println(
				"    -k <short> (k-mer size, default = "+kmerSize+")");
		System.out.println(
				"    -n <int> (partitions per executor core, default = "+partitionsPerCore+")");
		System.out.println(
				"    -maxtrim <short> (maximum number of bases that can be trimmed, default = "+maxTrim+")");
		System.out.println(
				"    -lowercase (write corrected bases in lowercase, default = "+lowercase+")");	
		System.out.println(
				"    -maxerr <short> (maximum number of mutations in any region of length #k, default = "+numErrors+")");
		System.out.println(
				"    -maxiter <short> (maximum number of correction iterations, default = "+maxIters+")");
		System.out.println(
				"    -minmulti <short> (minimum multiplicity for correct k-mers, default = autocalculated)");
		System.out.println("Others:");
		System.out.println(
				"    -h (print out the usage of the program)");
		System.out.println(
				"    -v (print out the version of the program)");
	}

	public boolean parse(String[] args) {
		int argInd = 0;
		boolean withInputFile = false;
		boolean withOutputFile = false;
		boolean withPairedOutputFile = false;

		while (argInd < args.length) {
			if (args[argInd].equals("-i")) {
				argInd++;
				if (argInd < args.length) {
					inputFile1 = args[argInd];
					withInputFile = true;
					argInd++;
				} else {
					logger.error("No value specified for parameter -i");
					printUsage(RunSparkMusket.class.getName());
					return false;
				}
			} else if (args[argInd].equals("-p")) {
				argInd++;
				if (argInd < args.length) {
					inputFile2 = args[argInd];
					paired = true;
					argInd++;
				} else {
					logger.error("No value specified for parameter -p");
					printUsage(RunSparkMusket.class.getName());
					return false;
				}
			} else if (args[argInd].equals("-o")) {
				argInd++;
				if (argInd < args.length) {
					outputFile1 = args[argInd];
					withOutputFile = true;
					argInd++;
				} else {
					logger.error("No value specified for parameter -o");
					printUsage(RunSparkMusket.class.getName());
					return false;
				}
			} else if (args[argInd].equals("-r")) {
				argInd++;
				if (argInd < args.length) {
					outputFile2 = args[argInd];
					withPairedOutputFile = true;
					argInd++;
				} else {
					logger.error("No value specified for parameter -r");
					printUsage(RunSparkMusket.class.getName());
					return false;
				}
			} else if (args[argInd].equals("-q")) {
				argInd++;
				inputFormat = FileFormat.FILE_FORMAT_FASTQ;
			} else if (args[argInd].equals("-f")) {
				inputFormat = FileFormat.FILE_FORMAT_FASTA;
				argInd++;
			} else if (args[argInd].equals("-k")) {
				// K-mer size
				argInd++;
				if (argInd < args.length) {
					kmerSize = Short.parseShort(args[argInd]);
					argInd++;

					if (kmerSize < Configuration.MIN_KMER_SIZE) {
						logger.warn("k-mer size (-k "+kmerSize+") cannot be less than "
								+Configuration.MIN_KMER_SIZE+". Using -k "+Configuration.MIN_KMER_SIZE+" instead");
						kmerSize = Configuration.MIN_KMER_SIZE;
					}
				} else {
					logger.error("No value specified for parameter -k");
					printUsage(RunSparkMusket.class.getName());
					return false;
				}
			} else if (args[argInd].equals("-n")) {
				// number of partitions
				argInd++;
				if (argInd < args.length) {
					partitionsPerCore = Integer.parseInt(args[argInd]);
					argInd++;

					if (partitionsPerCore < 1) {
						logger.warn("the number of partitions per executor core (-n "+partitionsPerCore+") cannot be less than "
								+ "1. Using 8 instead.");

						partitionsPerCore = 8;
					}
				} else {
					logger.error("No value specified for parameter -n");
					printUsage(RunSparkMusket.class.getName());
					return false;
				}
			} else if (args[argInd].equals("-maxtrim")) {
				// Maximum bases to be trimmed
				argInd++;
				if (argInd < args.length) {
					maxTrim = Short.parseShort(args[argInd]);
					argInd++;

					if (maxTrim < 0) {
						logger.warn("the maximum number of bases that can be trimmed "
								+ "(-maxtrim "+maxTrim+") cannot be less than 0. Using 0 instead");
						maxTrim = 0;
					}
				} else {
					logger.error("No value specified for parameter -maxtrim");
					printUsage(RunSparkMusket.class.getName());
					return false;
				}
			} else if (args[argInd].equals("-lowercase")) {
				argInd++;
				lowercase = true;
			} else if (args[argInd].equals("-maxerr")) {
				// Maximum number of mutations
				argInd++;
				if (argInd < args.length) {
					numErrors = Short.parseShort(args[argInd]);
					argInd++;

					if (numErrors < 2) {
						logger.warn("the maximum number of mutations in any region of length #k "
								+ "(-maxerr "+numErrors+") cannot be less than 2. Using 2 instead");
						numErrors = 2;
					}
				} else {
					logger.error("No value specified for parameter -maxerr");
					printUsage(RunSparkMusket.class.getName());
					return false;
				}
			} else if (args[argInd].equals("-maxiter")) {
				// Maximum iterations
				argInd++;
				if (argInd < args.length) {
					maxIters = Short.parseShort(args[argInd]);
					argInd++;

					if (maxIters < 1) {
						logger.warn("the maximum number of correcting iterations "
								+ "(-maxiter "+maxIters+") cannot be less than 1. Using 1 instead");
						maxIters = 1;
					}
				} else {
					logger.error("No value specified for parameter -maxiter");
					printUsage(RunSparkMusket.class.getName());
					return false;
				}
			} else if (args[argInd].equals("-minmulti")) {
				// Minimum multiplicty
				argInd++;
				if (argInd < args.length) {
					watershed = Short.parseShort(args[argInd]);
					argInd++;

					if (watershed < 0) {
						logger.warn("the minimum multiplicty (-minmulti "+watershed+") "
								+ "cannot be less than 0. Actual value will be autocalculated");
						watershed = -1;
					}
				} else {
					logger.error("No value specified for parameter -minmulti");
					printUsage(RunSparkMusket.class.getName());
					return false;
				}
			} else if (args[argInd].equals("-h")) {
				// Check the help
				printUsage(RunSparkMusket.class.getName());
				return false;
			} else if (args[argInd].equals("-v")) {
				// Check the version
				logger.info("version {}", Configuration.SMUSKET_VERSION);
				logger.info("more info available at {}", Configuration.SMUSKET_WEBPAGE);
				return false;
			} else {
				logger.error("Parameter " + args[argInd] + " is not valid");
				printUsage(RunSparkMusket.class.getName());
				return false;
			}
		}

		if (!withInputFile) {
			logger.error("No input file specified with -i");
			printUsage(RunSparkMusket.class.getName());
			return false;
		}

		if (!withOutputFile)
			outputFile1 = inputFile1+".corrected";

		if (!withPairedOutputFile && paired)
			outputFile2 = inputFile2+".corrected";

		return true;
	}
}
