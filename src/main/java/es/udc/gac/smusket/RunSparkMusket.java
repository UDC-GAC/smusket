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

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;

import es.udc.gac.hadoop.sequence.parser.mapreduce.PairText;
import es.udc.gac.hadoop.sequence.parser.mapreduce.PairedEndSequenceInputFormat;
import es.udc.gac.hadoop.sequence.parser.mapreduce.SingleEndSequenceInputFormat;
import es.udc.gac.smusket.util.Configuration;
import es.udc.gac.smusket.util.Constants;
import es.udc.gac.smusket.util.HDFSIOUtils;
import es.udc.gac.smusket.util.Options;
import es.udc.gac.smusket.util.SequenceParser;
import es.udc.gac.smusket.util.SequenceParserFactory;
import es.udc.gac.smusket.util.Timer;
import scala.Tuple2;

public class RunSparkMusket {

	private static final Logger logger = LogManager.getLogger();
	// Timer
	private static final Timer timer = new Timer();
	private static final int T_TOTAL = 0;
	private static final int T_INIT = 1;
	private static final int T_ERROR_CORRECTION = 2;
	private static final int T_KMER_COUNTING = 3;
	private static final int T_KMER_MAP_BROADCAST = 4;
	private static final int T_READ_CORRECTION = 5;
	private static final int T_MERGE = 6;

	public static void main(String[] args) throws Exception {
		Path inputPath1 = null, inputPath2 = null;
		Path outputPath1 = null, outputPath2 = null;
		Path tmpOutputPath1 = null, tmpOutputPath2 = null;
		Path success1 = null, success2 = null;
		short watershed;
		Class<? extends SingleEndSequenceInputFormat> inputFormatClass;
		FileSystem fs;
		JavaRDD<Sequence> readsRDD = null;
		JavaPairRDD<Sequence,Sequence> pairedReadsRDD = null;
		JavaPairRDD<Long,Short> kmersRDD = null;
		Broadcast<Map<Long, Short>> kmersMapBC;
		Broadcast<SequenceParser> stringParserBC;
		Broadcast<Constants> constantsBC;
		Broadcast<ErrorCorrection> errorCorrectionBC;
		Broadcast<Short> watershedBC;

		timer.start(T_TOTAL);

		timer.start(T_INIT);

		logger.info("starting from SMUSKET_HOME = {}", Configuration.SMUSKET_HOME);

		// Parse command-line options
		Options options = new Options();
		if (!options.parse(args))
			System.exit(-1);

		// Create Spark configuration
		SparkConf sparkConf = new SparkConf()
				.setAppName("SparkMusket")
				.setSparkHome(Configuration.SPARK_HOME)
				.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
				.set("spark.kryo.registrationRequired", "false")
				.set("spark.kryo.unsafe", "true")
				.set("spark.kryo.referenceTracking", "false")
				.set("spark.kryoserializer.buffer", "2m")
				.set("spark.executor.heartbeatInterval", "25s")
				.set("spark.broadcast.checksum", "false")
				.set("spark.shuffle.service.enabled", "false")
				.set("spark.shuffle.registration.timeout", "10000")
				.set("spark.shuffle.compress", "true")
				.set("spark.shuffle.spill.compress", "true")
				.set("spark.broadcast.compress", "true")
				.set("spark.rdd.compress", String.valueOf(Configuration.RDD_COMPRESS))
				.set("spark.io.compression.codec", String.valueOf(Configuration.COMPRESSION_CODEC))
				.set("spark.speculation", "false")
				.set("spark.dynamicAllocation.enabled", "false")
				.set("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
				.set("spark.driver.maxResultSize", "0");

		String executorJavaOptions = "-Dlog4j.configurationFile="+Configuration.LOG_GILE+ " -Djava.net.preferIPv4Stack=true";

		if (sparkConf.contains("spark.executor.extraJavaOptions"))
			executorJavaOptions+=" "+sparkConf.get("spark.executor.extraJavaOptions");

		sparkConf.set("spark.executor.extraJavaOptions", executorJavaOptions);
		logger.debug("spark.executor.extraJavaOptions = {}", () -> sparkConf.get("spark.executor.extraJavaOptions"));

		// Register custom classes with Kryo
		sparkConf.registerKryoClasses(new Class<?>[]{
			Class.forName("es.udc.gac.smusket.util.SequenceParser"),
			Class.forName("es.udc.gac.smusket.util.FastQParser"),
			Class.forName("es.udc.gac.smusket.util.FastAParser"),
			Class.forName("es.udc.gac.smusket.util.SequenceParserFactory"),
			Class.forName("es.udc.gac.smusket.util.SequenceParserFactory$FileFormat"),
			Class.forName("es.udc.gac.smusket.util.Constants"),
			Class.forName("es.udc.gac.smusket.util.MurmurHash3"),
			Class.forName("es.udc.gac.smusket.ErrorCorrection"),
			Class.forName("es.udc.gac.smusket.Sequence"),
			Class.forName("es.udc.gac.smusket.Kmer"),
			Class.forName("es.udc.gac.smusket.Correction"),
			Class.forName("es.udc.gac.smusket.CorrectRegion"),
			Class.forName("es.udc.gac.smusket.CorrectRegionComp"),
			Class.forName("es.udc.gac.smusket.CorrectRegionComp$CorrectRegionPingComp"),
			Class.forName("es.udc.gac.smusket.CorrectRegionComp$CorrectRegionPongComp"),
			Class.forName("java.util.Map"),
			Class.forName("java.util.HashMap"),
			Class.forName("java.util.ArrayList"),
			Class.forName("java.util.PriorityQueue"),
			Class.forName("java.lang.Long"),
			Class.forName("java.lang.Short"),
			Class.forName("java.lang.Byte"),
			Class.forName("java.lang.StringBuilder"),
			Class.forName("scala.Tuple2"),
			Class.forName("org.apache.commons.lang3.tuple.MutablePair"),
			Class.forName("org.apache.logging.log4j.Logger"),
			Class.forName("org.apache.logging.log4j.LogManager"),
			Class.forName("org.apache.hadoop.io.Text"),
			Class.forName("org.apache.hadoop.io.LongWritable"),
			Class.forName("org.apache.spark.util.collection.BitSet")
		});

		// Create Spark session and context
		SparkSession sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();		
		JavaSparkContext jSparkContext = JavaSparkContext.fromSparkContext(sparkSession.sparkContext());

		// Get info about Spark executors
		int executorCores = sparkSession.sparkContext().defaultParallelism();

		// Get Hadoop configuration and file system
		org.apache.hadoop.conf.Configuration hadoopConf = jSparkContext.hadoopConfiguration();
		fs = FileSystem.get(hadoopConf);

		// Sanity checks on input and output paths
		HDFSIOUtils.checkInputPaths(hadoopConf, options.getInputFile1(), options.getInputFile2(), options.getPartitions(), executorCores);
		HDFSIOUtils.checkOutputPaths(hadoopConf, options.getOutputFile1(), options.getOutputFile2());

		// Get input and output paths for the first dataset
		inputPath1 = fs.resolvePath(new Path(options.getInputFile1()));
		outputPath1 = new Path(Configuration.OUTPUT_PATH+"/"+options.getOutputFile1());
		tmpOutputPath1 = new Path(Configuration.TEMP_OUTPUT_PATH_1);
		success1 = new Path(tmpOutputPath1+"/_SUCCESS");

		// Configure input buffer size for HSP library
		es.udc.gac.hadoop.sequence.parser.util.Configuration.setInputBufferSize(hadoopConf, Configuration.INPUT_BUFFER_SIZE);

		// Get input format class
		inputFormatClass = HDFSIOUtils.getInputFormatClass(hadoopConf, options.getInputFileFormat(), inputPath1);

		// Print some command line options and configuration parameters
		logger.info("k-mer size = {}", options.getKmerSize());
		logger.info("maximum iterations = {}", options.getMaxIters());
		logger.info("maximum mutations = {}", options.getNumErros());
		logger.info("maximum bases trimmed = {}", options.getMaxTrim());
		logger.info("minimum multiplicity = {}", options.getWatershed());
		logger.info("lowercase = {}", options.isLowercase());
		logger.info("input buffer size = {}", Configuration.INPUT_BUFFER_SIZE+" bytes");
		logger.info("serialized rdd = {}", Configuration.SERIALIZED_RDD);
		logger.info("rdd compress = {}", Configuration.RDD_COMPRESS);
		logger.info("compression codec = {}", Configuration.COMPRESSION_CODEC);

		// Broadcast variables
		SequenceParser stringParser = SequenceParserFactory.createStringParser(inputFormatClass);
		Constants constants = new Constants(options);
		stringParserBC = jSparkContext.broadcast(stringParser);
		constantsBC = jSparkContext.broadcast(constants);
		ErrorCorrection errorCorrection = new ErrorCorrection(constantsBC, options);
		errorCorrectionBC = jSparkContext.broadcast(errorCorrection);

		timer.stop(RunSparkMusket.T_INIT);

		timer.start(T_ERROR_CORRECTION);

		timer.start(RunSparkMusket.T_KMER_COUNTING);

		// Create input RDDs
		if (!options.isPaired()) {
			logger.info("running in single-end mode");

			JavaPairRDD<LongWritable,Text> inputReadsRDD = jSparkContext.newAPIHadoopFile(inputPath1.toString(), 
					inputFormatClass, LongWritable.class, Text.class, hadoopConf);

			// Parse reads
			readsRDD = inputReadsRDD.values().map(new Function<Text, Sequence>() {
				/**
				 * 
				 */
				private static final long serialVersionUID = 1647254546130571830L;

				@Override
				public Sequence call(Text read) {
					return stringParserBC.value().parseSequence(read.getBytes(), 0, read.getLength());
				}
			});

			if (Configuration.SERIALIZED_RDD)
				readsRDD.persist(StorageLevel.MEMORY_ONLY_SER());
			else
				readsRDD.persist(StorageLevel.MEMORY_ONLY());
		} else {
			logger.info("running in paired-end mode");

			// Get input and output paths for the second dataset
			inputPath2 = fs.resolvePath(new Path(options.getInputFile2()));
			outputPath2 = new Path(Configuration.OUTPUT_PATH+"/"+options.getOutputFile2());
			tmpOutputPath2 = new Path(Configuration.TEMP_OUTPUT_PATH_2);
			success2 = new Path(tmpOutputPath2+"/_SUCCESS");

			// Set left and right input paths for HSP
			PairedEndSequenceInputFormat.setLeftInputPath(hadoopConf, inputPath1, inputFormatClass);
			PairedEndSequenceInputFormat.setRightInputPath(hadoopConf, inputPath2, inputFormatClass);

			JavaPairRDD<LongWritable,PairText> inputReadsRDD = jSparkContext.newAPIHadoopFile(inputPath1.toString(), 
					PairedEndSequenceInputFormat.class, LongWritable.class, PairText.class, hadoopConf);

			// Parse reads
			pairedReadsRDD = inputReadsRDD.mapToPair(new PairFunction<Tuple2<LongWritable, PairText>, Sequence, Sequence>() {
				/**
				 * 
				 */
				private static final long serialVersionUID = -6470737542648634108L;

				@Override
				public Tuple2<Sequence, Sequence> call(Tuple2<LongWritable, PairText> read) {
					Sequence left = stringParserBC.value().parseSequence(read._2.getLeft().getBytes(), 0, read._2.getLeft().getLength());
					Sequence right = stringParserBC.value().parseSequence(read._2.getRight().getBytes(), 0, read._2.getRight().getLength());
					return new Tuple2<Sequence, Sequence>(left, right);
				}
			});

			if (Configuration.SERIALIZED_RDD)
				pairedReadsRDD.persist(StorageLevel.MEMORY_ONLY_SER());
			else
				pairedReadsRDD.persist(StorageLevel.MEMORY_ONLY());
		}

		// K-mer counting
		logger.info("determine non-unique k-mers and their multiplicity");

		if (!options.isPaired())
			kmersRDD = KmerCounting.runSingle(readsRDD, constantsBC);
		else
			kmersRDD = KmerCounting.runPaired(pairedReadsRDD, constantsBC);

		if (options.getWatershed() < 0) {
			logger.info("estimate the minimum multiplicty from the k-mer histogram");
			watershed = KmerCounting.getMinMultiplicity(kmersRDD);
			logger.info("minimum multiplicity: {}", watershed);
		} else {
			watershed = options.getWatershed();
			logger.info("using the user-specified minimum multiplicity: {}", watershed);
		}

		timer.stop(RunSparkMusket.T_KMER_COUNTING);

		// Create K-mer map
		timer.start(RunSparkMusket.T_KMER_MAP_BROADCAST);
		logger.info("collect k-mers (multi >= {}) and broadcast them", watershed);

		watershedBC = jSparkContext.broadcast(watershed);
		JavaPairRDD<Long,Short> kmersRDDGEwatershed = kmersRDD.filter(kmer -> kmer._2 >= watershedBC.value());
		kmersMapBC = jSparkContext.broadcast(new HashMap<Long, Short>(kmersRDDGEwatershed.collectAsMap()));

		logger.info("number of k-mers (multi >= {}): {}", watershed, kmersMapBC.value().size());

		if (logger.isTraceEnabled()) {
			String kmersFile = tmpOutputPath1+".kmers";
			Path kmersPath = new Path(kmersFile);

			if (fs.exists(kmersPath))
				fs.delete(kmersPath, true);

			logger.trace("writing k-mers (multi >= {}) file at {}", watershed, kmersFile);
			kmersRDDGEwatershed.coalesce(1).saveAsTextFile(kmersFile);
		}

		timer.stop(RunSparkMusket.T_KMER_MAP_BROADCAST);

		// Perform error correction
		timer.start(RunSparkMusket.T_READ_CORRECTION);
		logger.info("perform error correction");

		if (!options.isPaired()) {
			ErrorCorrection.run(readsRDD, tmpOutputPath1.toString(), errorCorrectionBC, kmersMapBC, watershedBC);
		} else {
			ErrorCorrection.run(pairedReadsRDD.keys(), tmpOutputPath1.toString(), errorCorrectionBC, kmersMapBC, watershedBC);
			ErrorCorrection.run(pairedReadsRDD.values(), tmpOutputPath2.toString(), errorCorrectionBC, kmersMapBC, watershedBC);
		}

		timer.stop(RunSparkMusket.T_READ_CORRECTION);

		jSparkContext.close();
		sparkSession.stop();

		timer.stop(T_ERROR_CORRECTION);

		if (fs.exists(tmpOutputPath1)) {
			// Cleanup
			if (fs.exists(success1))
				fs.delete(success1, true);

			if (options.isPaired()) {
				if (fs.exists(success2))
					fs.delete(success2, true);
			}

			timer.start(T_MERGE);

			long outputFileCount = fs.getContentSummary(tmpOutputPath1).getFileCount();

			if (outputFileCount == 1) {
				Path output1 = new Path(tmpOutputPath1+"/part-00000");
				fs.rename(output1, outputPath1);

				if (options.isPaired()) {
					Path output2 = new Path(tmpOutputPath2+"/part-00000");
					fs.rename(output2, outputPath2);
				}
			} else if (Configuration.MERGE_OUTPUT) {
				// Merge output files
				logger.info("merging output files...");
				RunMerge.merge(fs, tmpOutputPath1, fs, outputPath1, Configuration.INPUT_BUFFER_SIZE, Configuration.HDFS_BLOCK_REPLICATION, Configuration.DELETE_TEMP, hadoopConf);

				if (options.isPaired())
					RunMerge.merge(fs, tmpOutputPath2, fs, outputPath2, Configuration.INPUT_BUFFER_SIZE, Configuration.HDFS_BLOCK_REPLICATION, Configuration.DELETE_TEMP, hadoopConf);
			} else {
				outputPath1 = tmpOutputPath1;
				outputPath2 = tmpOutputPath2;
			}

			timer.stop(T_MERGE);

			logger.info("output path = {}", outputPath1);
			if (options.isPaired())
				logger.info("output path = {}", outputPath2);
		} else {
			logger.error("output path does not exist: {}", tmpOutputPath1);
		}

		timer.stop(T_TOTAL);

		logger.printf(Level.INFO, "total execution time = %.3f seconds", timer.readTotal(T_TOTAL));
		logger.printf(Level.INFO, "initialization = %.3f seconds", timer.readTotal(T_INIT));
		logger.printf(Level.INFO, "error correction = %.3f seconds", timer.readTotal(T_ERROR_CORRECTION));
		logger.printf(Level.INFO, "  k-mer counting = %.3f seconds", timer.readTotal(T_KMER_COUNTING));
		logger.printf(Level.INFO, "  k-mer broadcast = %.3f seconds", timer.readTotal(T_KMER_MAP_BROADCAST));
		logger.printf(Level.INFO, "  read correction = %.3f seconds", timer.readTotal(T_READ_CORRECTION));
		if (Configuration.MERGE_OUTPUT)
			logger.printf(Level.INFO, "merge output = %.3f seconds", timer.readTotal(T_MERGE));

		System.exit(0);
	}
}
