package org.apache.hadoop.fs.tar;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.EnumSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.AbstractFileSystem;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FsServerDefaults;
import org.apache.hadoop.fs.FsStatus;
import org.apache.hadoop.fs.Options.ChecksumOpt;
import org.apache.hadoop.fs.ParentNotDirectoryException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.util.Progressable;

public class TarFs extends AbstractFileSystem {

  public static final Log LOG = LogFactory.getLog(TarFileSystem.class);

  public TarFs(
    URI uri, String supportedScheme, boolean authorityNeeded, int defaultPort)
    throws URISyntaxException {
    super(uri, supportedScheme, authorityNeeded, defaultPort);
    LOG.info("Constructed a TarFs");
    // TODO Auto-generated constructor stub
  }

  @Override
  public int getUriDefaultPort() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public FsServerDefaults getServerDefaults() throws IOException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public FSDataOutputStream createInternal(
    Path f, EnumSet<CreateFlag> flag, FsPermission absolutePermission,
    int bufferSize, short replication, long blockSize, Progressable progress,
    ChecksumOpt checksumOpt, boolean createParent)
    throws AccessControlException, FileAlreadyExistsException,
    FileNotFoundException, ParentNotDirectoryException,
    UnsupportedFileSystemException, UnresolvedLinkException, IOException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void mkdir(Path dir, FsPermission permission, boolean createParent)
    throws AccessControlException, FileAlreadyExistsException,
    FileNotFoundException, UnresolvedLinkException, IOException {
    // TODO Auto-generated method stub

  }

  @Override
  public boolean delete(Path f, boolean recursive)
    throws AccessControlException, FileNotFoundException,
    UnresolvedLinkException, IOException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public FSDataInputStream open(Path f, int bufferSize)
    throws AccessControlException, FileNotFoundException,
    UnresolvedLinkException, IOException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public boolean setReplication(Path f, short replication)
    throws AccessControlException, FileNotFoundException,
    UnresolvedLinkException, IOException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public void renameInternal(Path src, Path dst)
    throws AccessControlException, FileAlreadyExistsException,
    FileNotFoundException, ParentNotDirectoryException, UnresolvedLinkException,
    IOException {
    // TODO Auto-generated method stub

  }

  @Override
  public void setPermission(Path f, FsPermission permission)
    throws AccessControlException, FileNotFoundException,
    UnresolvedLinkException, IOException {
    // TODO Auto-generated method stub

  }

  @Override
  public void setOwner(Path f, String username, String groupname)
    throws AccessControlException, FileNotFoundException,
    UnresolvedLinkException, IOException {
    // TODO Auto-generated method stub

  }

  @Override
  public void setTimes(Path f, long mtime, long atime)
    throws AccessControlException, FileNotFoundException,
    UnresolvedLinkException, IOException {
    // TODO Auto-generated method stub

  }

  @Override
  public FileChecksum getFileChecksum(Path f)
    throws AccessControlException, FileNotFoundException,
    UnresolvedLinkException, IOException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public FileStatus getFileStatus(Path f)
    throws AccessControlException, FileNotFoundException,
    UnresolvedLinkException, IOException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public BlockLocation[] getFileBlockLocations(Path f, long start, long len)
    throws AccessControlException, FileNotFoundException,
    UnresolvedLinkException, IOException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public FsStatus getFsStatus()
    throws AccessControlException, FileNotFoundException, IOException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public FileStatus[] listStatus(Path f)
    throws AccessControlException, FileNotFoundException,
    UnresolvedLinkException, IOException {
    LOG.info("in abstract fs");
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void setVerifyChecksum(boolean verifyChecksum)
    throws AccessControlException, IOException {
    // TODO Auto-generated method stub

  }

}
