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

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import java.util.Map;

public final class Configuration {

	public static final String SMUSKET_VERSION = "1.1";
	public static final String SMUSKET_WEBPAGE = "https://github.com/rreye/smusket";

	// SparkMusket configuration (etc/smusket.conf)
	public static final String SMUSKET_HOME;
	private static final String SMUSKET_CONFIG_FILE = "smusket.conf";
	public static final String SPARK_HOME;
	public static final String SLASH;
	public static final String LOG_GILE;
	public static final long MIN_PARTITION_SIZE = 1*1024*1024; // 1 MiB
	private static final Properties SMUSKET_PROPERTIES;
	private static final int MIN_INPUT_BUFFER_SIZE = 1024;
	public static final int MIN_KMER_SIZE = 4;
	public static boolean MERGE_OUTPUT;
	public static boolean DELETE_TEMP;
	public static short HDFS_BLOCK_REPLICATION;
	public static String HDFS_BASE_PATH;
	public static int INPUT_BUFFER_SIZE;
	public static boolean SERIALIZED_RDD;
	public static boolean RDD_COMPRESS;
	public static String COMPRESSION_CODEC;
	public static String OUTPUT_PATH;
	public static String TEMP_OUTPUT_PATH_1;
	public static String TEMP_OUTPUT_PATH_2;

	static {
		Map<String,String> map = System.getenv();
		SMUSKET_HOME = map.get("SMUSKET_HOME");

		if(SMUSKET_HOME == null)
			throw new RuntimeException("'SMUSKET_HOME' must be set");

		SPARK_HOME = map.get("SPARK_HOME");

		if(SPARK_HOME == null)
			throw new RuntimeException("'SPARK_HOME' must be set");

		SMUSKET_PROPERTIES = new Properties();
		SLASH = System.getProperty("file.separator");
		LOG_GILE = SMUSKET_HOME+SLASH+"etc"+SLASH+"log4j2.xml";

		String configFileName = SMUSKET_HOME+SLASH+"etc"+SLASH+SMUSKET_CONFIG_FILE;

		try {
			FileInputStream configFile = new FileInputStream(configFileName);
			SMUSKET_PROPERTIES.load(configFile);
			configFile.close();
		} catch (IOException e) {
			throw new RuntimeException("Configuration file not found: "+configFileName);
		}

		MERGE_OUTPUT = Boolean.parseBoolean(Configuration.getProperty(SMUSKET_PROPERTIES, "MERGE_OUTPUT", "false"));
		DELETE_TEMP = Boolean.parseBoolean(Configuration.getProperty(SMUSKET_PROPERTIES, "DELETE_TEMP", "true"));
		HDFS_BLOCK_REPLICATION = Short.parseShort(Configuration.getProperty(SMUSKET_PROPERTIES, "HDFS_BLOCK_REPLICATION", "1"));
		INPUT_BUFFER_SIZE = Integer.parseUnsignedInt(Configuration.getProperty(SMUSKET_PROPERTIES, "INPUT_BUFFER_SIZE", "65536"));
		SERIALIZED_RDD = Boolean.parseBoolean(Configuration.getProperty(SMUSKET_PROPERTIES, "SERIALIZED_RDD", "true"));
		RDD_COMPRESS = Boolean.parseBoolean(Configuration.getProperty(SMUSKET_PROPERTIES, "RDD_COMPRESS", "false"));
		COMPRESSION_CODEC = Configuration.getProperty(SMUSKET_PROPERTIES, "COMPRESSION_CODEC", "lz4");
		HDFS_BASE_PATH = Configuration.getProperty(SMUSKET_PROPERTIES, "HDFS_BASE_PATH", null);

		if (INPUT_BUFFER_SIZE < MIN_INPUT_BUFFER_SIZE)
			throw new RuntimeException("INPUT_BUFFER_SIZE="+INPUT_BUFFER_SIZE+" is not valid. Minimum value: "+MIN_INPUT_BUFFER_SIZE);

		if (HDFS_BLOCK_REPLICATION < 1)
			throw new RuntimeException("HDFS_BLOCK_REPLICATION="+HDFS_BLOCK_REPLICATION+" is not valid. Minimum value: 1");

		if (!COMPRESSION_CODEC.equalsIgnoreCase("lz4") && !COMPRESSION_CODEC.equalsIgnoreCase("snappy"))
			throw new RuntimeException("COMPRESSION_CODEC="+COMPRESSION_CODEC+" is not valid. Supported values: lz4 and snappy");
	}

	private Configuration() {}

	private static String getProperty(Properties properties, String key, String defaultValue) {
		String value = properties.getProperty(key, defaultValue);

		if (value.isEmpty())
			return defaultValue;

		return value;
	}
}
