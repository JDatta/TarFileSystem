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

import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Common utilities for a TAR file system
 *
 * @author joydip
 *
 */
public class TarFSUtils {

  public static FileSystem getHadoopFS(URI sample)
      throws URISyntaxException, IOException {

    return getHadoopFS(sample, new Configuration());
  }

  public static FileSystem getHadoopFS(URI sample, Configuration conf)
      throws IOException {

    return FileSystem.get(sample, conf);
  }

  @Deprecated
  public static FSDataInputStream openHadoopIS(FileSystem fs, URI file)
      throws IOException {

    Path f = new Path(file);
    return fs.open(f);
  }

  @Deprecated
  public static TarArchiveInputStream createTarInputStream(
      FileSystem fs, String tarFile, long offset)
      throws URISyntaxException, IOException {

    return createTarInputStream(fs, new Path(tarFile), offset);
  }

  public static TarArchiveInputStream createTarInputStream(
      FileSystem fs, Path tarPath, long offset)
      throws URISyntaxException, IOException {

    FSDataInputStream fsdis = fs.open(tarPath);
    fsdis.seek(offset);
    return new TarArchiveInputStream(new BufferedInputStream(fsdis));
  }

  @Deprecated
  public static TarArchiveInputStream createTarInputStream(InputStream in)
      throws IOException {
    return new TarArchiveInputStream(in);
  }

  private static byte[] getFirstFewBytes(InputStream in, int n)
      throws IOException {
    byte[] b = new byte[n];

    if (in.markSupported()) {
      in.mark(n);
      int bytesRead = in.read(b);
      in.reset();

      if (bytesRead < n)
        return null;
    }
    else
      throw new IOException("Mark must be supported");

    return b;
  }

  /**
   * Returns true if the file pointed by filename is a valid gunzip compressed
   * file.
   *
   * @param filename
   * @param in
   *          an inputstream to the file.
   * @return
   * @throws IOException
   */
  public static boolean isGZ(String filename, InputStream in)
      throws IOException {

    File f = new File(filename);
    if (!f.isFile())
      return false;

    if (filename.endsWith(".gz") || filename.endsWith(".tgz")) {
      byte[] b = getFirstFewBytes(in, 3);
      if (b == null)
        return false;

      if ((b[0] & 0xff) == 0x1f && (b[1] & 0xff) == 0x8b)
        return true;
      else
        return false;
    }

    else
      return false;

  }

}
