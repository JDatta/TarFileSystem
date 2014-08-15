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

import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.Seekable;

/**
 * {@link TarArchiveInputStream} can not be used inside Hadoop as it does not
 * implement {@link Seekable}.
 *
 * This implementation is independent of {@link TarArchiveInputStream}
 *
 * @author joydip
 *
 */
public class SeekableTarInputStream extends FSInputStream {

  FSDataInputStream in;
  final long length;
  final long start;
  long pos = 0;

  public static final Log LOG = LogFactory.getLog(SeekableTarInputStream.class);

  public SeekableTarInputStream(FSDataInputStream in, long size)
      throws IOException {
    this.in = in;
    this.length = size;
    start = in.getPos();
  }

  public SeekableTarInputStream(FSDataInputStream in, long size, long offset)
      throws IOException {
    this.in = in;
    this.length = size;
    this.in.seek(offset);
    this.start = in.getPos();
  }

  @Override
  public void seek(long desired) throws IOException {
    if (desired > this.length)
      throw new IOException("Can not seek past EOF!");

    this.pos = desired;
    in.seek(start + desired);
  }

  /**
   * Returns the position within a TAR internal file
   */
  @Override
  public long getPos() throws IOException {
    return pos;
  }

  @Override
  public boolean seekToNewSource(long targetPos) throws IOException {
    return false;
  }

  @Override
  public int read() throws IOException {
    if ((this.pos + 1) > this.length) {
      return -1;
    }
    this.pos += 1;
    return in.read();
  }

  @Override
  public void close() throws IOException {
    super.close();
    in.close();
  }
}
