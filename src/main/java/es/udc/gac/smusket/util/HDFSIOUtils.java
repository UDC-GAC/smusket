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

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import es.udc.gac.hadoop.sequence.parser.mapreduce.FastAInputFormat;
import es.udc.gac.hadoop.sequence.parser.mapreduce.FastQInputFormat;
import es.udc.gac.hadoop.sequence.parser.mapreduce.SingleEndSequenceInputFormat;
import es.udc.gac.smusket.util.SequenceParserFactory.FileFormat;

public final class HDFSIOUtils {

	private static final Logger logger = LogManager.getLogger();

	private HDFSIOUtils() {}

	public static long checkInputPaths(org.apache.hadoop.conf.Configuration hadoopConf, String inputFile1, 
			String inputFile2, int partitionsPerCore, int cores) throws IOException {
		Path inputPath1 = null, inputPath2 = null;
		Path basePath;
		long inputPath1Length, inputPath2Length = 0;
		long inputPath1LengthMB, inputPath2LengthMB = 0;
		boolean isInputPath1Compressed, isInputPath2Compressed;
		boolean isInputPath1Splitable, isInputPath2Splitable;
		long blockSize, blockSizeMB, partitionSize, partitionSizeMB;
		int blocks, partitions;

		FileSystem fs = FileSystem.get(hadoopConf);

		if (Configuration.HDFS_BASE_PATH == null)
			basePath = new Path(fs.getHomeDirectory().toString());
		else
			basePath = new Path(Configuration.HDFS_BASE_PATH);

		if (!basePath.equals(fs.getHomeDirectory())) {
			if (!fs.exists(basePath)) {
				throw new RuntimeException("HDFS_BASE_PATH="+Configuration.HDFS_BASE_PATH+" is not valid: "+basePath+" does not exist");
			}

			if(!fs.isDirectory(basePath)) {
				throw new RuntimeException("HDFS_BASE_PATH="+Configuration.HDFS_BASE_PATH+" is not valid: "+basePath+" is not a directory");
			}
		} else {
			if (!fs.exists(basePath))
				fs.mkdirs(basePath);
		}

		Configuration.HDFS_BASE_PATH = fs.resolvePath(basePath).toString();
		String concatPath = Configuration.HDFS_BASE_PATH;

		if (basePath.isRoot())
			concatPath = concatPath.substring(0, concatPath.length() - 1);

		Configuration.OUTPUT_PATH = concatPath+Configuration.SLASH+"SparkMusket"+Configuration.SLASH+"output";
		Configuration.TEMP_OUTPUT_PATH_1 = concatPath+Configuration.SLASH+"SparkMusket"+Configuration.SLASH+"tmp"+Configuration.SLASH+"output1";
		Configuration.TEMP_OUTPUT_PATH_2 = concatPath+Configuration.SLASH+"SparkMusket"+Configuration.SLASH+"tmp"+Configuration.SLASH+"output2";

		inputPath1 = fs.resolvePath(new Path(inputFile1));
		inputPath1Length = fs.getFileStatus(inputPath1).getLen();
		inputPath1LengthMB = inputPath1Length / (1024 * 1024);
		isInputPath1Compressed = HDFSIOUtils.isInputPathCompressed(hadoopConf, inputPath1);
		isInputPath1Splitable = HDFSIOUtils.isInputPathSplitable(hadoopConf, inputPath1);
		blockSize = fs.getFileStatus(inputPath1).getBlockSize();
		blockSizeMB = blockSize / (1024 * 1024);
		blocks = SingleEndSequenceInputFormat.getNumberOfSplits(inputPath1, inputPath1Length, isInputPath1Splitable, blockSize);
		partitions = partitionsPerCore * cores;
		partitionSize = inputPath1Length / partitions;

		if (blocks >= partitions) {
			partitions = blocks;
			partitionSize = blockSize;
		} else {
			if (partitionSize < Configuration.MIN_PARTITION_SIZE) {
				logger.warn("the partition size ({}) that is needed to create {} partitions is less than {} bytes, which can "
						+ "affect performance. Using {} instead.", partitionSize, partitions, Configuration.MIN_PARTITION_SIZE, Configuration.MIN_PARTITION_SIZE);

				partitions = (int) Math.max(1, inputPath1Length / Configuration.MIN_PARTITION_SIZE);
				partitionSize = (partitions == 1)? inputPath1Length : Configuration.MIN_PARTITION_SIZE;
			}
		}

		if (partitionSize != blockSize) {
			hadoopConf.setLong("mapreduce.input.fileinputformat.split.minsize", partitionSize);
			hadoopConf.setLong("mapreduce.input.fileinputformat.split.maxsize", partitionSize);
		}

		partitionSizeMB = partitionSize / (1024 * 1024);

		if (inputFile2 != null) {
			inputPath2 = fs.resolvePath(new Path(inputFile2));
			inputPath2Length = fs.getFileStatus(inputPath2).getLen();
			inputPath2LengthMB = inputPath2Length / (1024 * 1024);
			isInputPath2Compressed = HDFSIOUtils.isInputPathCompressed(hadoopConf, inputPath2);
			isInputPath2Splitable = HDFSIOUtils.isInputPathSplitable(hadoopConf, inputPath2);

			// Sanity checks
			if ((isInputPath1Compressed && !isInputPath2Compressed) || (!isInputPath1Compressed && isInputPath2Compressed))
				throw new UnsupportedOperationException("Both input files must be compressed or not compressed");

			if (isInputPath1Compressed && isInputPath2Compressed) {
				if ((isInputPath1Splitable && !isInputPath2Splitable) || (!isInputPath1Splitable && isInputPath2Splitable))
					throw new UnsupportedOperationException("Both compressed input files must be splitable or not splitable");
			}

			if (!isInputPath1Compressed && !isInputPath2Compressed) {
				if (inputPath1Length != inputPath2Length)
					throw new IOException("Input files must be the same size in paired-end mode");
			}
		}

		logger.info("HDFS base path = {}", Configuration.HDFS_BASE_PATH);
		logger.info("HDFS block replication = {}", Configuration.HDFS_BLOCK_REPLICATION);
		logger.info("HDFS block size = {} MiBytes", blockSizeMB);

		if (inputPath1LengthMB > 0)
			logger.info("input path = {} ({} MiBytes)", inputPath1, inputPath1LengthMB);
		else
			logger.info("input path = {} ({} bytes)", inputPath1, inputPath1Length);

		if (inputPath2 != null) {
			if (inputPath2LengthMB > 0)
				logger.info("input path = {} ({} MiBytes)", inputPath2, inputPath2LengthMB);
			else
				logger.info("input path = {} ({} bytes)", inputPath2, inputPath2Length);
		}

		logger.info("HDFS blocks = {}", blocks);
		logger.info("number of cores = {}", cores);
		logger.info("partitions per core = {}", partitionsPerCore);
		logger.info("number of partitions = {}", partitions);
		if (partitionSizeMB > 0)
			logger.info("partition size = {} MiBytes", partitionSizeMB);
		else
			logger.info("partition size = {} bytes", partitionSize);

		return partitionSize;
	}

	public static void checkOutputPaths(org.apache.hadoop.conf.Configuration hadoopConf, String outputFile1, String outputFile2) throws IOException {		
		FileSystem fs = FileSystem.get(hadoopConf);
		Path outputDir = new Path(Configuration.OUTPUT_PATH);
		Path outputPath1 = new Path(Configuration.OUTPUT_PATH+Configuration.SLASH+outputFile1);
		Path tmpOutputPath1 = new Path(Configuration.TEMP_OUTPUT_PATH_1);

		if (!fs.exists(outputDir))
			fs.mkdirs(outputDir);

		if (fs.exists(outputPath1))
			fs.delete(outputPath1, true);
		if (fs.exists(tmpOutputPath1))
			fs.delete(tmpOutputPath1, true);

		if (outputFile2 != null) {
			Path outputPath2 = new Path(Configuration.OUTPUT_PATH+Configuration.SLASH+outputFile2);
			Path tmpOutputPath2 = new Path(Configuration.TEMP_OUTPUT_PATH_2);

			if (fs.exists(outputPath2))
				fs.delete(outputPath2, true);
			if (fs.exists(tmpOutputPath2))
				fs.delete(tmpOutputPath2, true);
		}
	}

	public static Class<? extends SingleEndSequenceInputFormat> getInputFormatClass(org.apache.hadoop.conf.Configuration hadoopConf, FileFormat format, Path inputPath) throws IOException {
		Class<? extends SingleEndSequenceInputFormat> inputFormatClass = null;
		FileSystem fs = FileSystem.get(hadoopConf);

		if (format == FileFormat.FILE_FORMAT_UNKNOWN) {
			// Try to autodetect the input file format
			logger.info("detecting input file format from {}", inputPath);

			if (HDFSIOUtils.isInputPathCompressed(hadoopConf, inputPath))
				throw new IOException("Input file "+inputPath.getName()+" is compressed. You must specify its format using -q (FASTQ) or -f (FASTA) command-line options.");

			format = SequenceParserFactory.autoDetectFileFormat(fs, inputPath);
		}

		// Set input format class
		if (format == FileFormat.FILE_FORMAT_FASTQ) {
			logger.info("FASTQ format identified");
			inputFormatClass = FastQInputFormat.class;
		} else if (format == FileFormat.FILE_FORMAT_FASTA) {
			inputFormatClass = FastAInputFormat.class;
		}

		return inputFormatClass;
	}

	private static boolean isInputPathCompressed(org.apache.hadoop.conf.Configuration conf, Path inputPath) {
		CompressionCodec codec = new CompressionCodecFactory(conf).getCodec(inputPath);

		if (codec == null)
			return false;

		return true;
	}

	private static boolean isInputPathSplitable(org.apache.hadoop.conf.Configuration conf, Path inputPath) {
		CompressionCodec codec = new CompressionCodecFactory(conf).getCodec(inputPath);

		if (codec == null)
			return true;

		return codec instanceof SplittableCompressionCodec;
	}
}
