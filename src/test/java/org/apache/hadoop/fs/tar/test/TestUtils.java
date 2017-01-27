package org.apache.hadoop.fs.tar.test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;

public class TestUtils {

  public static void createLocalTarFile(File tarFile, int count)
      throws IOException {

    createLocalTarFile(tarFile, "file_", count);
  }

  public static void createLocalTarFile(
      File tarFile, String filePrefix, int count)
          throws IOException {

    TarArchiveOutputStream tarOutput = null;
    try {
      OutputStream os = new FileOutputStream(tarFile);
      tarOutput = new TarArchiveOutputStream(os);

      for (int i = 0; i < count; i++) {
        String msg = "Lorem";
        byte[] bytes = msg.getBytes();
        TarArchiveEntry entry = new TarArchiveEntry(filePrefix + i);
        entry.setSize(bytes.length);
        tarOutput.putArchiveEntry(entry);
        tarOutput.write(bytes);
        tarOutput.closeArchiveEntry();
      }
    } finally {
      if (tarOutput != null) {
        tarOutput.close();
      }
    }
  }
}
