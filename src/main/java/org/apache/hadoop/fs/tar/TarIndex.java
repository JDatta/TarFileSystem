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
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Scanner;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.AccessControlException;

/**
 * Creates a Index out of a Tar file. Also stores the index in a index file.
 *
 * @author joydip
 *
 */
public class TarIndex {

  private HashMap<String, IndexEntry> index = new HashMap<String, IndexEntry>();

  public static final Log LOG = LogFactory.getLog(TarIndex.class);
  public static final String INDEX_EXT = ".index";

  private static class IndexEntry {

    long size;
    long offset;

    public IndexEntry(long size, long offset) {
      this.size = size;
      this.offset = offset;
    }
  }

  public TarIndex(FileSystem fs, Path tarPath) throws IOException {
    this(fs, tarPath, true, new Configuration());
  }

  /**
   * Creates a Index out of a tar file. Index is a MAP between
   * Filename->start_offset
   *
   * @param fs
   *          Underlying Hadoop FileSystem
   * @param tarPath
   *          Path to the Tar file
   * @param isWrite
   *          should I write the index to a file
   * @throws IOException
   */
  public TarIndex(FileSystem fs, Path tarPath, boolean isWrite,
      Configuration conf) throws IOException {

    Path indexPath = getIndexPath(tarPath);
    Path altIndexP = getAltIndexPath(tarPath, conf);

    boolean readOK = false;
    readOK = readIndexFile(fs, indexPath);

    if (readOK == false)
      readOK = readIndexFile(fs, altIndexP);

    if (readOK == false) {
      FSDataInputStream is = fs.open(tarPath);
      byte[] buffer = new byte[512];

      while (true) {
        int bytesRead = is.read(buffer);
        if (bytesRead == -1)
          break;
        if (bytesRead < 512)
          throw new IOException("Could not read the full header.");

        long currOffset = is.getPos();
        TarArchiveEntry entry = new TarArchiveEntry(buffer);

        // Index only normal files. Do not support directories yet.
        if (entry.isFile()) {
          String name = entry.getName().trim();
          if (!name.equals("")) {
            IndexEntry ie = new IndexEntry(entry.getSize(), currOffset);
            index.put(name, ie);
          }
        }

        long nextOffset = currOffset + entry.getSize();
        if (nextOffset % 512 != 0)
          nextOffset = ((nextOffset / 512) + 1) * 512;
        is.seek(nextOffset);
      }
      is.close();

      if (isWrite) {
        boolean writeOK = writeIndex(fs, indexPath);

        if (writeOK == false && altIndexP != null)
          writeOK = writeIndex(fs, altIndexP);

        if (writeOK == false) {
          Path p = altIndexP == null ? indexPath : altIndexP;

          LOG.error("Could not create INDEX file " + p.toUri());
          if (altIndexP == null)
            LOG.error("You can specify alternate location for index" +
                " creation using tarfs.tmp.dir property.");

          LOG.error("Skipping writing index file.");
        }
      }
    }
  }

  private Path getIndexPath(Path tarPath) {
    return new Path(tarPath.toUri() + INDEX_EXT);
  }

  private Path getAltIndexPath(Path tarPath, Configuration conf) {
    String tmp = conf.get("tarfs.tmp.dir", null);
    if (tmp == null)
      return null;
    else
      return new Path(tmp + Path.SEPARATOR + tarPath + INDEX_EXT);
  }

  /**
   * Writes the index map to a file
   *
   * @param fs
   * @param indexPath
   * @throws IOException
   */
  private boolean writeIndex(FileSystem fs, Path indexPath) throws IOException {

    if (fs.exists(indexPath)) {
      LOG.error("Index file already exists. Skipping writing index.");
      return false;
    }

    OutputStream os = null;
    PrintWriter out = null;

    try {
      fs.mkdirs(indexPath.getParent());
      os = fs.create(indexPath);
      out = new PrintWriter(os);

      for (String name : index.keySet()) {
        IndexEntry ie = index.get(name);
        out.println(name + " " + ie.size + " " + ie.offset);
      }
      return true;
    } catch (AccessControlException e) {
      return false;
    } finally {
      if (out != null)
        out.close();
      if (os != null)
        os.close();
    }
  }

  private boolean readIndexFile(FileSystem fs, Path indexPath)
      throws IOException {

    if (indexPath == null || !fs.exists(indexPath))
      return false;

    FSDataInputStream is = null;
    Scanner s = null;

    try {
      is = fs.open(indexPath);
      s = new Scanner(is);

      while (s.hasNextLine()) {
        String[] tokens = s.nextLine().split(" ");
        if (tokens.length != 3) {
          LOG.error("Invalid Index File: " + indexPath);
          return false;
        }

        IndexEntry ie = new IndexEntry(
            Long.parseLong(tokens[1]),
            Long.parseLong(tokens[2]));
        index.put(tokens[0], ie);
      }
      return true;
    } catch (AccessControlException e) {
      LOG.error("Can not open Index file for reading " + indexPath + " "
          + e.getMessage());
      return false;
    } finally {
      if (s != null)
        s.close();
      if (is != null)
        is.close();
    }
  }

  private IndexEntry getIndexEntry(String name) throws IOException {
    IndexEntry ie = index.get(name);
    if (ie == null)
      throw new IOException("Requested file \""
          + name + "\" does not exist inside tar.");
    return ie;
  }

  public long getOffset(String name) throws IOException {
    return getIndexEntry(name).offset;
  }

  public long getSize(String name) throws IOException {
    return getIndexEntry(name).size;
  }

  /**
   * return a sorted list of all offsets
   */
  public long[] getOffsetList() {
    long[] offsetArr = new long[index.size()];
    int i = 0;
    for (String name : index.keySet()) {
      offsetArr[i++] = index.get(name).offset;
    }

    // TBD: Could this sort be a bottleneck?
    Arrays.sort(offsetArr);
    return offsetArr;
  }

  public String[] getFileList() {
    String[] fileNames = new String[index.size()];
    return index.keySet().toArray(fileNames);
  }
}
