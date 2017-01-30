Tar FileSystem for Hadoop
==========================

<hr/>
<small>
Version: 2.0_beta
</small>
<hr/>

TAR is a widely used format for storing backup images, distributing large datasets etc. Many of those files could be used as an input to analytic jobs.

Apache Hadoop, as of now, is not TAR aware. That is, it can not directly read a file inside a TAR. Neither it can run map-reduce on those files. To run analytic jobs on a TAR, one needs to first copy it to local disk, un-TAR it, then copy back to Hadoop file system. Or convert it to sequence file/other Hadoop aware format using custom (java) program. This procedure is time consuming and the user ends up having two copies of data.

By using the TarFileSystem for Hadoop, Hadoop can directly read files inside a TAR and run analytic jobs on those file. This way, no conversion/extraction is required. 

Building
---------
Run "mvn package" inside the project directory. The TarFileSystem distribution is created as a jar file at `./target/hadoop-tarfs-2.0_beta.jar`


Distribution and Configuration
-------------------------------
TAR File System binary for Hadoop is distributed as a JAR library (`hadoop-tarfs-*.jar`). This JAR contains all the required classes to support TarFileSystem. Copy this JAR to the `HADOOP_HOME/lib` directory (`HDFS_HOME/lib` for Hadoop 2.0) or add the jar to `HADOOP_CLASSPATH` environment variable. 

Next expose `tar://` uri schema to Hadoop by adding the following property in `HADOOP_CONF_DIR/core-site.xml` file.

	<property>
	  <name>fs.tar.impl</name>
	  <value>org.apache.hadoop.fs.tar.TarFileSystem</value>
	</property>

### Optional Configuration:

By default, TarFileSystem creates an `.index` file in the same directory where the tar file resides. Index writing may fail if you do not have sufficient permission in that directory. In that case you may specify a temporary directory where you have write permission and tell TarFileSystem to use that directory instead. You may specify the following property in core-site.xml for this:

	<property>
	  <name>tarfs.tmp.dir</name>
	  <value>/a/directory/with/write/permission</value>
	</property>

Note that, TarFileSystem will still prefer the same directory where the tar file exists for writing the .index file. Only if writing to the same directory fails it will use the tarfs.tmp.dir. In that case, if tarfs.tmp.dir is not specified or writing to that directory also fail, it will skip writing the .index file with a warning message.

Using TAR File System
----------------------
Hadoop can access a TAR archive using TAR URI SCHEMA (URI starting with tar://). The following examples shows this:

Following is a TAR inside Hadoop file System

	[jd@node1 ~]$ bin/hadoop fs -ls /tardemo/archive.tar ↲
	Found 1 items
	-rw-r--r--   1 jd supergroup    1751040 2013-07-15 20:30 /tardemo/archive.tar

To access files inside this tar, simply prepone this with tar:// to make it a TAR File System URI

	[jd@node1 ~]$ bin/hadoop fs -ls tar:///tardemo/archive.tar ↲
	13/07/15 20:33:04 INFO tar.TarFileSystem: *** Using Tar file system ***
	Found 3 items
	-rw-rw-r--   1 jd jd     502760 2013-07-15 20:27 /tardemo/archive.tar+/data+file2.txt
	-rw-rw-r--   1 jd jd     594933 2013-07-15 20:26 /tardemo/archive.tar+/data+file1.txt
	-rw-rw-r--   1 jd jd     641720 2013-07-15 20:27 /tardemo/archive.tar+/data+file3.txt

To access a file inside a TAR archive, append the name of the file after the TAR URI using a ‘+’ sign. All sub-directory paths within a TAR archive are also defined using ‘+’ sign. For example, if the file is in path `dir1/dir2/file1.txt` within tar archive, use the following path to read it.

	[jd@node1 ~]$ bin/hadoop fs -cat tar://hdfs-localhost:54310/tardemo/archive.tar/+dir1+dir2+file1.txt ↲
	13/07/15 20:38:35 INFO tar.TarFileSystem: *** Using Tar file system ***
	This is the file content.
	[...]

In TAR File System, the TAR archive is modeled like a directory and all the files inside a TAR are modeled like files within a directory. One can run mapreduce jobs on files within a TAR archive just like they do it on normal files.

	[jd@node1 ~]$ bin/hadoop jar hadoop*examples*.jar wordcount tar:///tardemo/archive.tar wc_out ↲ 
	13/07/15 20:43:05 INFO tar.TarFileSystem: *** Using Tar file system ***
	13/07/15 20:43:05 INFO input.FileInputFormat: Total input paths to process : 3
	13/07/15 20:43:05 INFO mapred.JobClient: Running job: job_201307151954_0001
	13/07/15 20:43:06 INFO mapred.JobClient:  map 0% reduce 0%
	 [...]

# TO DO
  1. Implement efficient seek in SeekableTarInputStream
  2. Support compressed TAR archives

