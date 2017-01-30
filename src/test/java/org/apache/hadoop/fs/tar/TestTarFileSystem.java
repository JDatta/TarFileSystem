package org.apache.hadoop.fs.tar;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.net.URISyntaxException;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.tar.test.TarFileSystemTestFramework;
import org.apache.hadoop.fs.tar.test.TestUtils;
import org.junit.Test;

import junit.framework.Assert;

public class TestTarFileSystem extends TarFileSystemTestFramework {

  private static final String SAMPLE_TEXT =
    "Lorem ipsum dolor sit amet, consectetur adipiscing elit, "
      + "sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. "
      + "Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris "
      + "nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in "
      + "reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla "
      + "pariatur. Excepteur sint occaecat cupidatat non proident, sunt in "
      + "culpa qui officia deserunt mollit anim id est laborum.";

  @Override
  protected void createTarFile() throws IOException {
    TestUtils.createLocalTarFile(
      this.getTestTarFile(), "", SAMPLE_TEXT, 10);
  }

  @Test
  public void testListStatus() throws IOException, URISyntaxException {
    assertEquals(this.getTarfs().listStatus(this.getTestTarPath()).length, 10);
  }

  @Test
  public void testGetFileStatus() throws IOException, URISyntaxException {
    final FileStatus[] stats = this.getTarfs().listStatus(this.getTestTarPath());
    assertEquals(stats.length, 10);
    for (int i = 0; i < stats.length; i++) {
      Assert.assertEquals(
        stats[i], this.getTarfs().getFileStatus(stats[i].getPath()));
    }
  }

  @Test
  public void testRead() throws IOException, URISyntaxException {
    final FileStatus[] stats = this.getTarfs().listStatus(this.getTestTarPath());
    assertEquals(stats.length, 10);
    for (int i = 0; i < stats.length; i++) {
      InputStream in = null;
      try {
        System.out.println(stats[i].getPath());
        in = this.getTarfs().open(stats[i].getPath());
        final StringWriter writer = new StringWriter();
        IOUtils.copy(in, writer);
        Assert.assertEquals(SAMPLE_TEXT + i, writer.toString());
      } finally {
        IOUtils.closeQuietly(in);
      }
    }
  }
}
