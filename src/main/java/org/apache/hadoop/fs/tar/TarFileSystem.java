/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.fs.tar;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BufferedFSInputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

/**
 * Creates a FileSystem out of a TAR file. A tar file is treated as a directory
 * containing files. Sub-directories are not yet supported. Tar files can be
 * specified using following sample URI schema <br/>
 *
 * tar://hdfs-namenode:port/tarfile.tar (the whole tar.. treated as a directory)<br/>
 * tar://hdfs-namenode:port/tarfile.tar+somefile.txt <br/>
 * <ul>
 * <li>TODO subdirectories within a tar is not yet supported</li>
 * <li>TODO tarfiles must not contain '+' anywhere in the path</li>
 * <li>TODO TAR.GZ files are not yet supported. GZ is not seekable.</li>
 * </ul>
 *
 * @author joydip
 *
 */
public class TarFileSystem extends FileSystem {

  private URI uri;
  private TarIndex index;
  private FileSystem underlyingFS = null;
  private Path workingDir;

  private static final String TAR_URLPREFIX = "tar:/";
  private static final char TAR_INFILESEP = '+';
  private static final String TAR_INFILESEP_STR = "\\+";

  public static final Log LOG = LogFactory.getLog(TarFileSystem.class);

  @Override
  public void initialize(URI name, Configuration conf) throws IOException {
    LOG.info("*** Using Tar file system ***");
    super.initialize(name, conf);

    this.underlyingFS = TarFSUtils.getHadoopFS(
        getBaseTarPath(new Path(name)).toUri(),
        conf);
    this.index = new TarIndex(
        underlyingFS,
        getBaseTarPath(new Path(name)), true, conf);

    initURI(name, conf);
    setConf(conf);
  }

  private void initURI(URI name, Configuration conf) {
    if (name.getAuthority() != null) {
      String uriStr = name.getScheme() + "://" + name.getAuthority();
      this.uri = URI.create(uriStr);
    }
    else {
      this.uri = URI.create(name.getScheme() + ":///");
    }
  }

  @Override
  public URI getUri() {
    return uri;
  }

  private String getFileInArchive(Path tarPath) {
    String fullUri = tarPath.toUri().toString();
    int i = fullUri.indexOf(TAR_INFILESEP);
    if (i == -1)
      return null;
    return fullUri.substring(i + 1)
      .replaceAll(TAR_INFILESEP_STR, Path.SEPARATOR);
  }

  /**
   * Creates an URI for base tar-ball from an URI in tar FS schema
   *
   * @param tarPath
   * @return
   * @throws URISyntaxException
   */
  private Path getBaseTarPath(Path tarPath) {
    URI uri = tarPath.toUri();

    // form the prefix part
    String basePrefix = uri.getAuthority();
    if (basePrefix != null)
      basePrefix = basePrefix.replaceFirst("-", "://");
    else
      basePrefix = "";

    // form the path component
    String basePath = uri.getPath();
    // strip the part containing inFile name
    int lastPlusIndex = basePath.indexOf(TAR_INFILESEP);
    if (lastPlusIndex != -1)
      basePath = basePath.substring(0, lastPlusIndex);

    basePath = basePrefix + basePath;

    return new Path(basePath);
  }

  private Path makeAbsolute(Path path) {
    String pathStr = path.toUri().toASCIIString();
    if (pathStr.startsWith(TAR_URLPREFIX))
      return path;
    else
      return new Path(getUri() + path.toUri().toString());
  }

  @Override
  public FSDataInputStream open(Path f, int bufferSize) throws IOException {

    Path baseTarPath = getBaseTarPath(f);
    String inFile = getFileInArchive(f);

    if (inFile == null)
      throw new IOException("TAR FileSystem: Can not open the whole TAR");

    // adjust for the header
    long offset = index.getOffset(inFile);
    long size = index.getSize(inFile);

    FSDataInputStream in = underlyingFS.open(baseTarPath);

    in.seek(offset - 512);
    TarArchiveEntry entry = readHeaderEntry(in);
    if (!entry.getName().equals(inFile)) {
      LOG.fatal(
          "Index file is corrupt." +
          "Requested filename is present in index " +
          "but absent in TAR.");
      throw new IOException("Requested filename does not match ");
    }

    return new FSDataInputStream(
        new BufferedFSInputStream(
            new SeekableTarInputStream(in, size, offset),
            bufferSize));
  }

  private TarArchiveEntry readHeaderEntry(InputStream is)
      throws IOException {
    byte[] buffer = new byte[512];
    readHeaderBuffer(is, buffer);
    return new TarArchiveEntry(buffer);
  }

  private TarArchiveEntry readHeaderEntry(InputStream is, byte[] buffer)
      throws IOException {
    readHeaderBuffer(is, buffer);
    return new TarArchiveEntry(buffer);
  }

  private void readHeaderBuffer(InputStream is, byte[] buffer)
      throws IOException {
    int bytesRead = is.read(buffer);
    if (bytesRead == -1)
      throw new IOException("EOF Occurred while reading buffer.");
    if (bytesRead < 512)
      throw new IOException("Could not read the full header.");
  }

  @Override
  public FileStatus[] listStatus(Path f) throws IOException {
    ArrayList<FileStatus> ret = new ArrayList<FileStatus>();
    Path abs = makeAbsolute(f);
    Path baseTar = getBaseTarPath(abs);
    String inFile = getFileInArchive(abs);
    FileStatus underlying = underlyingFS.getFileStatus(baseTar);

    // if subfile exists in the path, just return the status of that
    if (inFile != null) {
      ret.add(getFileStatus(abs));
    }

    else {
    	FSDataInputStream in = underlyingFS.open(baseTar);
      try {
      	byte[] buffer = new byte[512];

      	for (long offset : index.getOffsetList()) {
      		in.seek(offset - 512); // adjust for the header
      		TarArchiveEntry entry = readHeaderEntry(in, buffer);
      		// Construct a FileStatus object
      		FileStatus fstatus = new FileStatus(
      				entry.getSize(),
      				entry.isDirectory(),
      				(int) underlying.getReplication(),
      				underlying.getBlockSize(),
      				entry.getModTime().getTime(),
      				underlying.getAccessTime(),
      				new FsPermission((short) entry.getMode()),
      				entry.getUserName(),
      				entry.getGroupName(),
      				new Path(
      						abs.toUri().toASCIIString()
      						+ Path.SEPARATOR
      						+ TAR_INFILESEP
      						+ entry.getName()
                  	.replaceAll(Path.SEPARATOR, TAR_INFILESEP_STR)));
      		ret.add(fstatus);
      	}
      }
      finally {
      	org.apache.commons.io.IOUtils.closeQuietly(in);
      }
    }

    // copy back
    FileStatus[] retArray = new FileStatus[ret.size()];
    ret.toArray(retArray);
    return retArray;
  }

  @Override
  public void setWorkingDirectory(Path new_dir) {
    this.workingDir = new_dir;

  }

  @Override
  public Path getWorkingDirectory() {
    return this.workingDir;
  }

  @Override
  public FileStatus getFileStatus(Path f) throws IOException {
    FileStatus fstatus = null;
    Path abs = makeAbsolute(f);
    Path baseTar = getBaseTarPath(abs);
    String inFile = getFileInArchive(abs);

    FileStatus underlying = underlyingFS.getFileStatus(baseTar);

    if (inFile == null) {
      // return the status of the tar itself but make it a dir
      fstatus = new FileStatus(
          underlying.getLen(),
          true,
          underlying.getReplication(),
          underlying.getBlockSize(),
          underlying.getModificationTime(),
          underlying.getAccessTime(),
          underlying.getPermission(),
          underlying.getOwner(),
          underlying.getGroup(),
          abs);
    }

    else {
      long offset = index.getOffset(inFile);

      FSDataInputStream in = underlyingFS.open(baseTar);
      try {
      	in.seek(offset - 512);
      	TarArchiveEntry entry = readHeaderEntry(in);
      
      
      	if (!entry.getName().equals(inFile)) {
      		LOG.fatal(
      				"Index file is corrupt." +
      						"Requested filename is present in index " +
                	"but absent in TAR.");
      		throw new IOException("NBU-TAR: FATAL: entry file name " +
      				"does not match requested file name");
      	}

      	// Construct a FileStatus object
      	fstatus = new FileStatus(
	          entry.getSize(),
	          entry.isDirectory(),
	          (int) underlying.getReplication(),
	          underlying.getBlockSize(),
	          entry.getModTime().getTime(),
	          underlying.getAccessTime(),
	          new FsPermission((short) entry.getMode()),
	          entry.getUserName(),
	          entry.getGroupName(),
	          abs);
      }
      finally {
      	org.apache.commons.io.IOUtils.closeQuietly(in);
      }
    }

    return fstatus;
  }

  /*
   * DISABLED METHODS FOR READ-ONLY FILE SYSTEM
   */
  private String notSupportedMsg(String op) throws IOException {
    return op + " is not supported on a Tar File System";
  }

  @Override
  public FSDataOutputStream create(Path f, FsPermission permission,
      boolean overwrite, int bufferSize, short replication,
      long blockSize, Progressable progress) throws IOException {
    throw new IOException(notSupportedMsg("Create"));
  }

  @Override
  public FSDataOutputStream append(Path f, int bufferSize,
      Progressable progress) throws IOException {
    throw new IOException(notSupportedMsg("Append"));
  }

  @Override
  public boolean rename(Path src, Path dst) throws IOException {
    throw new IOException(notSupportedMsg("Rename"));
  }

  @Override
  @Deprecated
  public boolean delete(Path f) throws IOException {
    throw new IOException(notSupportedMsg("Delete"));
  }

  @Override
  public boolean delete(Path f, boolean recursive) throws IOException {
    throw new IOException(notSupportedMsg("Delete"));
  }

  @Override
  public boolean mkdirs(Path f, FsPermission permission) throws IOException {
    throw new IOException(notSupportedMsg("Mkdir"));
  }

}
