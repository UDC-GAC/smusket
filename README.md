# SMusket: Spark k-spectrum-based error corrector for distributed-memory systems

**SparkMusket (SMusket)** is a parallel read error corrector built upon the open-source [Apache Spark](http://spark.apache.org) Big Data framework that supports single-end and paired-end reads from FASTQ/FASTA datasets. This tool implements an error correction algorithm based on [Musket](http://musket.sourceforge.net/homepage.htm), which relies on the k-spectrum-based approach and provides three correction techniques in a multistage workflow.

Moreover, SMusket uses the [Hadoop Sequence Parser (HSP)](https://github.com/rreye/hsp) library to efficiently read the input datasets stored in the Hadoop Distributed File System (HDFS), being able to process datasets compressed with Gzip and BZip2 codecs.

## Getting Started

### Prerequisites

* Make sure you have Java Runtime Environment (JRE) version 1.8 or above
  * JAVA_HOME environmental variable must be set accordingly
 
* Make sure you have a working Spark distribution version 2.0 or above
  * SPARK_HOME environmental variable must be set accordingly
  * See [Spark's Cluster Mode Overview](https://spark.apache.org/docs/latest/cluster-overview.html)

* Download SMusket by executing the following command to clone the github repository:

```
git clone https://github.com/rreye/smusket.git
```

* Set SMUSKET_HOME and PATH environmental variables. On Linux, you can set them in your profile or your shell configuration files (e.g., .bashrc). Follow the instructions below:

```
export SMUSKET_HOME=/path/to/smusket
export PATH=$SMUSKET_HOME/bin:$PATH
```

### Execution

SMusket can be executed by using the provided *smusketrun* command, which launches the Spark jobs to the cluster. The usage of this command is the following:

```
smusketrun -sm|--smusket "SMUSKET_ARGS" [SPARK_ARGS]
```

This command accepts any argument to be passed to SMusket by using -sm|--smusket "SMUSKET_ARGS" (e.g. -sm "-i /path/to/dataset.fastq -k 24"), while the rest of the specified parameters are forwarded to the Spark runtime. The command-line arguments available for SMusket are:

* -i <string>. Compulsory both in single-end and paired-end scenarios. String with the HDFS path to the input sequence file in FASTA/FASTQ format.
* -p <string>. Compulsory only in paired-end scenarios. String with the HDFS path to the second input sequence file in FASTA/FASTQ format.
* -o <string>. Optional. String with the name for the output sequence file corresponding to the input file specified with -i. The default value is the input file name followed by *.corrected*.
* -r <string>. Optional. String with the name for the output sequence file corresponding to the input file specified with -p. The default value is the input file name followed by *.corrected*.
* -q. Specify that input sequence files are in FASTQ format. By default, SMusket tries to autodetect the input format, but if input files are compressed the user must specify the appropriate argument.
* -f. Specify that input sequence files are in FASTA format. By default, SMusket tries to autodetect the input format, but if input files are compressed the user must specify the appropriate argument.
* -k <int>. Optional. k-mer length. The default value is 21.
* -n <int>. Optional. Number of partitions per executor core to divide the input. The default value is 32.
* -maxtrim <int>. Optional. Maximum number of bases that can be trimmed. The default value is 0.
* -maxerr <int>. Optional. Maximum number of mutations in any region of length *k*. The default value is 4.
* -maxiter <int> Optional. Maximum number of correction iterations. The default value is 2.
* -minmulti <int> Optional. Minimum multiplicity for correct k-mers. The default value is autocalculated. 
* -lowercase. Optional. Write corrected bases in lowercase. By default, corrected bases are written in uppercase.
* -h. Print out the usage of the program and exit.
* -v. Print out the version of the program and exit

Note that the input FASTQ/FASTA datasets must be stored in the Hadoop Distributed File System (HDFS), which can be in compressed format. See https://github.com/rreye/hsp for more information about this topic.

### Examples

The following command corrects a single-end dataset and specifies the name of the output file:

```
smusketrun -sm "-i dataset.fastq -o dataset-corrected.fastq"
```

The following command corrects a paired-end dataset using a k-mer length of 24 and 4 correction iterations, writing corrected bases in lowercase:

```
smusketrun -sm "-i dataset1.fastq -p dataset2.fastq -k 24 -maxiter 4 -lowercase"
```

The following command corrects a single-end dataset compressed in BZip2 using 16 partitions per executor core, while passing extra arguments to Spark:

```
smusketrun -sm "-i dataset.fastq.bz2 -n 16" -sp "--master spark://207.184.161.138:7077 --deploy-mode client"
```

### Configuration

Some SMusket settings can be configured by means of the *smusket.conf* file located at the *etc* directory. The available parameters are:

* MERGE_OUTPUT (boolean). Merge output files stored in HDFS into a single one. The default value is false.
* DELETE_TEMP (boolean). Delete the intermediate files created by SMusket (if any). The default value is true.
* HDFS_BASE_PATH (string). Base path on HDFS where SMusket stores the output output files as well as temporary intermediate files (if any). The user running SMusket must have write permissions on this path. The default value is blank, which means to use the HDFS user’s home directory.
* HDFS_BLOCK_REPLICATION (short). HDFS block replication factor for output sequence files (significant only when MERGE_OUTPUT is set to true). The default value is 1.
* INPUT_BUFFER_SIZE (integer). Buffer size in bytes used for input read operations. It should probably be a multiple of the hardware page size (e.g., 4096). The default value is 65536 bytes.
* SERIALIZED_RDD (boolean). Whether to store serialized RDD partitions. RDD serialization can reduce memory usage at the cost of slightly slower access times to RDD elements due to having to deserialize each object on the fly. SMusket minimizes this overhead by using the Kryo serializer by default, which is significantly faster and more compact than Java serialization. The default value is true. Note that Kryo is used not only for RDD serialization but also for shuffling data between nodes, which improves SMusket performance.
* RDD_COMPRESS (boolean). Whether to compress serialized RDD partitions, only significant when SERIALIZED_RDD is set to true. RDD compression can reduce memory usage at the cost of some extra CPU time. The default value is false.
* COMPRESSION_CODEC (string). The codec used to compress broadcast variables, shuffle outputs and RDD partitions (the latter only when RDD_COMPRESS is set to true). Supported codecs: lz4 and snappy. The default value is lz4.

## Compilation

In case you need to recompile SMusket the prerequisites are:

* Make sure you have Java Develpment Kit (JDK) version 1.8 or above

* Make sure you have a working Apache Maven distribution version 3 or above
  * See [Installing Apache Maven](https://maven.apache.org/install.html)

In order to build SMusket just execute the following Maven command from within the SMusket root directory:

```
mvn package
```

This will recreate the *jar* file needed to run SMusket. Note that the first time you execute the previous command, Maven will download all the plugins and related dependencies it needs to fulfill the command. From a clean installation of Maven, this can take quite a while. If you execute the command again, Maven will now have what it needs, so it will be able to execute the command much more quickly.

## Authors

* **Roberto R. Expósito** (http://gac.udc.es/~rober)
* **Jorge González-Domínguez** (http://gac.udc.es/~jgonzalezd)
* **Juan Touriño** (http://gac.udc.es/~juan)

## License

This library is distributed as free software and is publicly available under the GPLv3 license (see the [LICENSE](LICENSE) file for more details)

