package org.apache.hadoop.fs.tar.test;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.tar.TarFileSystem;
import org.junit.After;
import org.junit.Before;

public abstract class TarFileSystemTestFramework {

  private File testTarFile = null;
  private Path testTarPath = null;
  private TarFileSystem tarfs = null;


  protected abstract void createTarFile() throws IOException;

  public final TarFileSystem getTarfs() {
    return tarfs;
  }

  public File getTestTarFile() {
    return testTarFile;
  }

  public Path getTestTarPath() {
    return testTarPath;
  }

  @Before
  public void setup() throws IOException {

    testTarFile = File.createTempFile("/tmp/", ".tar");
    testTarPath = new Path("tar://"+testTarFile.getAbsolutePath());

    createTarFile();

    tarfs = new TarFileSystem();
    tarfs.initialize(testTarPath.toUri(), new Configuration());

  }

  @After
  public void cleanup() throws IOException {
    if (tarfs != null) {
      tarfs.close();
    }
    if (testTarFile != null) {
      testTarFile.delete();
    }
  }
}
