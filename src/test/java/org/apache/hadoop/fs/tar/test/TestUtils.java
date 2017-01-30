package org.apache.hadoop.fs.tar.test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.hadoop.fs.Path;

public class TestUtils {

  public static void createLocalTarFile(File tarFile, int count)
      throws IOException {

    createLocalTarFile(tarFile, "files", "Lorem", count);
  }

  public static void createLocalTarFile(
    File tarFile, String prefix, String message, int count)
          throws IOException {

    TarArchiveOutputStream tarOutput = null;
    try {
      OutputStream os = new FileOutputStream(tarFile);
      tarOutput = new TarArchiveOutputStream(os);

      for (int i = 0; i < count; i++) {
        String thisMessage = message + i;
        byte[] bytes = thisMessage.getBytes();
        // put the i-th file in i-th level directory
        // i.e. 2nd file is placed in prefix/dir1/dir2/file
        // 3rd file is placed in prefix/dir1/dir2/dir3/file and so on
        StringBuilder thisPrefix = new StringBuilder(prefix);
        for (int j = 0; j < i; j++) {
          thisPrefix.append(Path.SEPARATOR);
          thisPrefix.append("dir");
          thisPrefix.append(j);
        }
        thisPrefix.append(Path.SEPARATOR);
        thisPrefix.append("file_");
        thisPrefix.append(i);

        TarArchiveEntry entry = new TarArchiveEntry(thisPrefix.toString());
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
