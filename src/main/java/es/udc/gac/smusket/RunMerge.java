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

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import es.udc.gac.smusket.util.Timer;

public class RunMerge extends Configured implements Tool {

	private static final Timer timer = new Timer();
	private static final int T_TOTAL = 0;
	private static String inputDir;
	private static String outputFile;
	private static short replication = 1;
	private static boolean deleteSrc = false;

	public static void merge(FileSystem srcFS, Path srcDir, FileSystem dstFS, Path dstFile, int bufferSize, short replication,
			long blockSize, boolean deleteSource, org.apache.hadoop.conf.Configuration conf) throws IOException {

		if (!srcFS.getFileStatus(srcDir).isDirectory())
			throw new IOException("Input source path is not a directory");

		FSDataOutputStream out = dstFS.create(dstFile, true, bufferSize, replication, blockSize);
		FSDataInputStream in = null;

		try {
			FileStatus contents[] = srcFS.listStatus(srcDir);
			Arrays.sort(contents);
			for (int i = 0; i < contents.length; i++) {
				if (contents[i].isFile() && contents[i].getLen() > 0) {
					in = srcFS.open(contents[i].getPath());
					try {
						IOUtils.copyBytes(in, out, conf, false);
					} finally {
						in.close();
					} 
				}
			}
		} finally {
			out.close();
		}

		if (deleteSource)
			srcFS.delete(srcDir, true);
	}

	public static void merge(FileSystem srcFS, Path srcDir, FileSystem dstFS, Path dstFile, int bufferSize, short replication, 
			boolean deleteSource, org.apache.hadoop.conf.Configuration conf) throws IOException {

		if (!srcFS.getFileStatus(srcDir).isDirectory())
			throw new IOException("Input source path is not a directory");

		long blockSize = conf.getLong("dfs.blocksize", 128 * 1024 * 1024);

		FSDataOutputStream out = dstFS.create(dstFile, true, bufferSize, replication, blockSize);
		FSDataInputStream in = null;

		try {
			FileStatus contents[] = srcFS.listStatus(srcDir);
			Arrays.sort(contents);
			for (int i = 0; i < contents.length; i++) {
				if (contents[i].isFile() && contents[i].getLen() > 0) {
					in = srcFS.open(contents[i].getPath());
					try {
						IOUtils.copyBytes(in, out, conf, false);
					} finally {
						in.close();
					} 
				}
			}
		} finally {
			out.close();
		}

		if (deleteSource)
			srcFS.delete(srcDir, true);
	}

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new org.apache.hadoop.conf.Configuration(), new RunMerge(), args));
	}

	@Override
	public int run(String[] args) throws Exception {

		// Get Hadoop configuration
		org.apache.hadoop.conf.Configuration conf = this.getConf();

		if(!parse(args))
			return -1;

		FileSystem fs  = FileSystem.get(conf);

		Path inputPath = fs.resolvePath(new Path(inputDir));
		Path outputPath = new Path(outputFile);

		System.out.println("Input source path = "+inputPath);

		long blockSize = conf.getLong("dfs.blocksize", 128 * 1024 * 1024);
		int bufferSize = conf.getInt("io.file.buffer.size", 8192);

		timer.start(T_TOTAL);
		RunMerge.merge(fs, inputPath, fs, outputPath, bufferSize, replication, blockSize, deleteSrc, conf);
		timer.stop(T_TOTAL);

		outputPath = fs.resolvePath(outputPath);
		System.out.format(java.util.Locale.ENGLISH,"Merge done in %.3f seconds\n", timer.readTotal(T_TOTAL));
		System.out.println("Merged output file = "+outputPath);

		return 0;
	}

	private static void printUsage(String name) {
		System.out.println();
		System.out.println(
				"Usage: " + RunMerge.class.getName() + " -i <src> -o <dst> [-d] [-r <replication>]");
		System.out.println("Options:");
		System.out.println(
				"    -i <string> input source path in HDFS");
		System.out.println(
				"    -o <string> output destination file in HDFS");
		System.out.println(
				"    -d delete input source path (default: false)");
		System.out.println(
				"    -r <integer> HDFS replication factor for output destination file (default: 1)");
		System.out.println(
				"    -h (print out the usage of the program)");
		System.out.println();
		System.out.println("Takes a source directory and a destination file as"
				+ " input and concatenates files in <src> path into the <dst> file. Both inputs must be HDFS paths.");

	}

	private static boolean parse(String[] args) {
		int argInd = 0;
		boolean withInputDir = false;
		boolean withOutputFile = false;

		while (argInd < args.length) {
			if (args[argInd].equals("-i")) {
				argInd++;
				if (argInd < args.length) {
					inputDir = args[argInd];
					withInputDir = true;
					argInd++;
				} else {
					System.err.println("No value specified for parameter -i");
					return false;
				}
			} else if (args[argInd].equals("-o")) {
				argInd++;
				if (argInd < args.length) {
					outputFile = args[argInd];
					withOutputFile = true;
					argInd++;
				} else {
					System.err.println("No value specified for parameter -o");
					return false;
				}
			} else if (args[argInd].equals("-r")) {
				argInd++;
				if (argInd < args.length) {
					replication = Short.parseShort(args[argInd]);
					argInd++;
				} else {
					System.err.println("No value specified for parameter -r");
					return false;
				}
			} else if (args[argInd].equals("-d")) {
				deleteSrc = true;
				argInd++;
			} else if (args[argInd].equals("-h")) {
				// Check the help
				printUsage(RunMerge.class.getName());
				return false;
			} else {
				System.err.println("Parameter " + args[argInd] + " is not valid");
				return false;
			}
		}

		if (!withInputDir) {
			System.err.println("No input path specified with -i");
			printUsage(RunMerge.class.getName());
			return false;
		}

		if (!withOutputFile) {
			System.err.println("No output file specified with -o");
			printUsage(RunMerge.class.getName());
			return false;
		}

		if (replication <= 0) {
			System.err.println("Invalid replicationf factor: "+replication);
			return false;
		}

		return true;
	}
}
