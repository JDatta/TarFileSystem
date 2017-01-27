package org.apache.hadoop.fs.tar;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.net.URISyntaxException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.tar.test.TarFileSystemTestFramework;
import org.apache.hadoop.fs.tar.test.TestUtils;
import org.junit.Test;


public class TestTarFileSystem extends TarFileSystemTestFramework {

  @Override
  protected void createTarFile() throws IOException {
    TestUtils.createLocalTarFile(getTestTarFile(), 10);
  }

  @Test
  public void testListStatus() throws IOException, URISyntaxException {

    FileStatus[] stuses = getTarfs().listStatus(getTestTarPath());
    assertEquals(stuses.length, 10);
  }

}
