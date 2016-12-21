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
package org.apache.raft.io.nativeio;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.util.Shell;
import org.apache.raft.protocol.AlreadyExistsException;
import org.apache.raft.util.NativeCodeLoader;
import org.apache.raft.util.RaftUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.Unsafe;

import java.io.File;
import java.io.FileDescriptor;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

/**
 * JNI wrappers for various native IO-related calls not available in Java.
 * These functions should generally be used alongside a fallback to another
 * more portable mechanism.
 */
public class NativeIO {
  private static final Logger LOG = LoggerFactory.getLogger(NativeIO.class);

  public static class POSIX {
    // Flags for open() call from bits/fcntl.h - Set by JNI
    public static int O_RDONLY = -1;
    public static int O_WRONLY = -1;
    public static int O_RDWR = -1;
    public static int O_CREAT = -1;
    public static int O_EXCL = -1;
    public static int O_NOCTTY = -1;
    public static int O_TRUNC = -1;
    public static int O_APPEND = -1;
    public static int O_NONBLOCK = -1;
    public static int O_SYNC = -1;

    // Flags for posix_fadvise() from bits/fcntl.h - Set by JNI
    /* No further special treatment.  */
    public static int POSIX_FADV_NORMAL = -1;
    /* Expect random page references.  */
    public static int POSIX_FADV_RANDOM = -1;
    /* Expect sequential page references.  */
    public static int POSIX_FADV_SEQUENTIAL = -1;
    /* Will need these pages.  */
    public static int POSIX_FADV_WILLNEED = -1;
    /* Don't need these pages.  */
    public static int POSIX_FADV_DONTNEED = -1;
    /* Data will be accessed once.  */
    public static int POSIX_FADV_NOREUSE = -1;


    // Updated by JNI when supported by glibc.  Leave defaults in case kernel
    // supports sync_file_range, but glibc does not.
    /* Wait upon writeout of all pages
       in the range before performing the
       write.  */
    public static int SYNC_FILE_RANGE_WAIT_BEFORE = 1;
    /* Initiate writeout of all those
       dirty pages in the range which are
       not presently under writeback.  */
    public static int SYNC_FILE_RANGE_WRITE = 2;
    /* Wait upon writeout of all pages in
       the range after performing the
       write.  */
    public static int SYNC_FILE_RANGE_WAIT_AFTER = 4;

    // Set to true via JNI if possible
    public static boolean fadvisePossible = false;

    private static boolean nativeLoaded = false;
    private static boolean syncFileRangePossible = true;

    private static long cacheTimeout = -1;

    private static CacheManipulator cacheManipulator = new CacheManipulator();

    public static CacheManipulator getCacheManipulator() {
      return cacheManipulator;
    }

    public static void setCacheManipulator(CacheManipulator cacheManipulator) {
      POSIX.cacheManipulator = cacheManipulator;
    }

    /**
     * Used to manipulate the operating system cache.
     */
    @VisibleForTesting
    public static class CacheManipulator {
      public void mlock(String identifier, ByteBuffer buffer,
          long len) throws IOException {
        POSIX.mlock(buffer, len);
      }

      public long getMemlockLimit() {
        return NativeIO.getMemlockLimit();
      }

      public long getOperatingSystemPageSize() {
        return NativeIO.getOperatingSystemPageSize();
      }

      public void posixFadviseIfPossible(String identifier,
        FileDescriptor fd, long offset, long len, int flags)
            throws NativeIOException {
        NativeIO.POSIX.posixFadviseIfPossible(identifier, fd, offset,
            len, flags);
      }

      public boolean verifyCanMlock() {
        return NativeIO.isAvailable();
      }
    }

    /**
     * A CacheManipulator used for testing which does not actually call mlock.
     * This allows many tests to be run even when the operating system does not
     * allow mlock, or only allows limited mlocking.
     */
    @VisibleForTesting
    public static class NoMlockCacheManipulator extends CacheManipulator {
      public void mlock(String identifier, ByteBuffer buffer,
          long len) throws IOException {
        LOG.info("mlocking " + identifier);
      }

      public long getMemlockLimit() {
        return 1125899906842624L;
      }

      public long getOperatingSystemPageSize() {
        return 4096;
      }

      public boolean verifyCanMlock() {
        return true;
      }
    }

    static {
      initNativeLib();
    }

    /**
     * Return true if the JNI-based native IO extensions are available.
     */
    public static boolean isAvailable() {
      return NativeCodeLoader.isNativeCodeLoaded() && nativeLoaded;
    }

    private static void assertCodeLoaded() throws IOException {
      if (!isAvailable()) {
        throw new IOException("NativeIO was not loaded");
      }
    }

    /** Wrapper around open(2) */
    public static native FileDescriptor open(String path, int flags, int mode) throws IOException;
    /** Wrapper around fstat(2) */
    private static native Stat fstat(FileDescriptor fd) throws IOException;

    /** Native chmod implementation. On UNIX, it is a wrapper around chmod(2) */
    private static native void chmodImpl(String path, int mode) throws IOException;

    public static void chmod(String path, int mode) throws IOException {
      if (!Shell.WINDOWS) {
        chmodImpl(path, mode);
      } else {
        try {
          chmodImpl(path, mode);
        } catch (NativeIOException nioe) {
          if (nioe.getErrorCode() == 3) {
            throw new NativeIOException("No such file or directory",
                Errno.ENOENT);
          } else {
            LOG.warn(String.format("NativeIO.chmod error (%d): %s",
                nioe.getErrorCode(), nioe.getMessage()));
            throw new NativeIOException("Unknown error", Errno.UNKNOWN);
          }
        }
      }
    }

    /** Wrapper around posix_fadvise(2) */
    static native void posix_fadvise(
      FileDescriptor fd, long offset, long len, int flags) throws NativeIOException;

    /** Wrapper around sync_file_range(2) */
    static native void sync_file_range(
      FileDescriptor fd, long offset, long nbytes, int flags) throws NativeIOException;

    /**
     * Call posix_fadvise on the given file descriptor. See the manpage
     * for this syscall for more information. On systems where this
     * call is not available, does nothing.
     *
     * @throws NativeIOException if there is an error with the syscall
     */
    static void posixFadviseIfPossible(String identifier,
        FileDescriptor fd, long offset, long len, int flags)
        throws NativeIOException {
      if (nativeLoaded && fadvisePossible) {
        try {
          posix_fadvise(fd, offset, len, flags);
        } catch (UnsatisfiedLinkError ule) {
          fadvisePossible = false;
        }
      }
    }

    /**
     * Call sync_file_range on the given file descriptor. See the manpage
     * for this syscall for more information. On systems where this
     * call is not available, does nothing.
     *
     * @throws NativeIOException if there is an error with the syscall
     */
    public static void syncFileRangeIfPossible(
        FileDescriptor fd, long offset, long nbytes, int flags)
        throws NativeIOException {
      if (nativeLoaded && syncFileRangePossible) {
        try {
          sync_file_range(fd, offset, nbytes, flags);
        } catch (UnsupportedOperationException | UnsatisfiedLinkError uoe) {
          syncFileRangePossible = false;
        }
      }
    }

    static native void mlock_native(
        ByteBuffer buffer, long len) throws NativeIOException;

    /**
     * Locks the provided direct ByteBuffer into memory, preventing it from
     * swapping out. After a buffer is locked, future accesses will not incur
     * a page fault.
     *
     * See the mlock(2) man page for more information.
     */
    static void mlock(ByteBuffer buffer, long len)
        throws IOException {
      assertCodeLoaded();
      if (!buffer.isDirect()) {
        throw new IOException("Cannot mlock a non-direct ByteBuffer");
      }
      mlock_native(buffer, len);
    }

    /**
     * Unmaps the block from memory. See munmap(2).
     *
     * There isn't any portable way to unmap a memory region in Java.
     * So we use the sun.nio method here.
     * Note that unmapping a memory region could cause crashes if code
     * continues to reference the unmapped code.  However, if we don't
     * manually unmap the memory, we are dependent on the finalizer to
     * do it, and we have no idea when the finalizer will run.
     *
     * @param buffer    The buffer to unmap.
     */
    public static void munmap(MappedByteBuffer buffer) {
      if (buffer instanceof sun.nio.ch.DirectBuffer) {
        sun.misc.Cleaner cleaner =
            ((sun.nio.ch.DirectBuffer)buffer).cleaner();
        cleaner.clean();
      }
    }

    /** Linux only methods used for getOwner() implementation */
    private static native long getUIDforFDOwnerforOwner(FileDescriptor fd) throws IOException;
    private static native String getUserName(long uid) throws IOException;

    /**
     * Result type of the fstat call
     */
    public static class Stat {
      private int ownerId, groupId;
      private String owner, group;
      private int mode;

      // Mode constants - Set by JNI
      public static int S_IFMT = -1;    /* type of file */
      public static int S_IFIFO  = -1;  /* named pipe (fifo) */
      public static int S_IFCHR  = -1;  /* character special */
      public static int S_IFDIR  = -1;  /* directory */
      public static int S_IFBLK  = -1;  /* block special */
      public static int S_IFREG  = -1;  /* regular */
      public static int S_IFLNK  = -1;  /* symbolic link */
      public static int S_IFSOCK = -1;  /* socket */
      public static int S_ISUID = -1;  /* set user id on execution */
      public static int S_ISGID = -1;  /* set group id on execution */
      public static int S_ISVTX = -1;  /* save swapped text even after use */
      public static int S_IRUSR = -1;  /* read permission, owner */
      public static int S_IWUSR = -1;  /* write permission, owner */
      public static int S_IXUSR = -1;  /* execute/search permission, owner */

      Stat(int ownerId, int groupId, int mode) {
        this.ownerId = ownerId;
        this.groupId = groupId;
        this.mode = mode;
      }

      Stat(String owner, String group, int mode) {
        if (!Shell.WINDOWS) {
          this.owner = owner;
        } else {
          this.owner = stripDomain(owner);
        }
        if (!Shell.WINDOWS) {
          this.group = group;
        } else {
          this.group = stripDomain(group);
        }
        this.mode = mode;
      }

      @Override
      public String toString() {
        return "Stat(owner='" + owner + "', group='" + group + "'" +
          ", mode=" + mode + ")";
      }

      public String getOwner() {
        return owner;
      }
      public String getGroup() {
        return group;
      }
      public int getMode() {
        return mode;
      }
    }

    private static class CachedName {
      final long timestamp;
      final String name;

      public CachedName(String name, long timestamp) {
        this.name = name;
        this.timestamp = timestamp;
      }
    }

    public final static int MMAP_PROT_READ = 0x1;
    public final static int MMAP_PROT_WRITE = 0x2;
    public final static int MMAP_PROT_EXEC = 0x4;

    public static native long mmap(FileDescriptor fd, int prot,
        boolean shared, long length) throws IOException;

    public static native void munmap(long addr, long length)
        throws IOException;
  }

  private static boolean workaroundNonThreadSafePasswdCalls = false;


  public static class Windows {
    // Flags for CreateFile() call on Windows
    public static final long GENERIC_READ = 0x80000000L;
    public static final long GENERIC_WRITE = 0x40000000L;

    public static final long FILE_SHARE_READ = 0x00000001L;
    public static final long FILE_SHARE_WRITE = 0x00000002L;
    public static final long FILE_SHARE_DELETE = 0x00000004L;

    public static final long CREATE_NEW = 1;
    public static final long CREATE_ALWAYS = 2;
    public static final long OPEN_EXISTING = 3;
    public static final long OPEN_ALWAYS = 4;
    public static final long TRUNCATE_EXISTING = 5;

    public static final long FILE_BEGIN = 0;
    public static final long FILE_CURRENT = 1;
    public static final long FILE_END = 2;

    public static final long FILE_ATTRIBUTE_NORMAL = 0x00000080L;

    /**
     * Create a directory with permissions set to the specified mode.  By setting
     * permissions at creation time, we avoid issues related to the user lacking
     * WRITE_DAC rights on subsequent chmod calls.  One example where this can
     * occur is writing to an SMB share where the user does not have Full Control
     * rights, and therefore WRITE_DAC is denied.
     *
     * @param path directory to create
     * @param mode permissions of new directory
     * @throws IOException if there is an I/O error
     */
    public static void createDirectoryWithMode(File path, int mode)
        throws IOException {
      createDirectoryWithMode0(path.getAbsolutePath(), mode);
    }

    /** Wrapper around CreateDirectory() on Windows */
    private static native void createDirectoryWithMode0(String path, int mode)
        throws NativeIOException;

    /** Wrapper around CreateFile() on Windows */
    public static native FileDescriptor createFile(String path,
        long desiredAccess, long shareMode, long creationDisposition)
        throws IOException;

    /**
     * Create a file for write with permissions set to the specified mode.  By
     * setting permissions at creation time, we avoid issues related to the user
     * lacking WRITE_DAC rights on subsequent chmod calls.  One example where
     * this can occur is writing to an SMB share where the user does not have
     * Full Control rights, and therefore WRITE_DAC is denied.
     *
     * This method mimics the semantics implemented by the JDK in
     * {@link FileOutputStream}.  The file is opened for truncate or
     * append, the sharing mode allows other readers and writers, and paths
     * longer than MAX_PATH are supported.  (See io_util_md.c in the JDK.)
     *
     * @param path file to create
     * @param append if true, then open file for append
     * @param mode permissions of new directory
     * @return FileOutputStream of opened file
     * @throws IOException if there is an I/O error
     */
    public static FileOutputStream createFileOutputStreamWithMode(File path,
        boolean append, int mode) throws IOException {
      long shareMode = FILE_SHARE_READ | FILE_SHARE_WRITE;
      long creationDisposition = append ? OPEN_ALWAYS : CREATE_ALWAYS;
      return new FileOutputStream(createFileWithMode0(path.getAbsolutePath(),
          GENERIC_WRITE, shareMode, creationDisposition, mode));
    }

    /** Wrapper around CreateFile() with security descriptor on Windows */
    private static native FileDescriptor createFileWithMode0(String path,
        long desiredAccess, long shareMode, long creationDisposition, int mode)
        throws NativeIOException;

    /** Wrapper around SetFilePointer() on Windows */
    public static native long setFilePointer(FileDescriptor fd,
        long distanceToMove, long moveMethod) throws IOException;

    /** Windows only methods used for getOwner() implementation */
    private static native String getOwner(FileDescriptor fd) throws IOException;

    /** Supported list of Windows access right flags */
    public enum AccessRight {
      ACCESS_READ (0x0001),      // FILE_READ_DATA
      ACCESS_WRITE (0x0002),     // FILE_WRITE_DATA
      ACCESS_EXECUTE (0x0020);   // FILE_EXECUTE

      private final int accessRight;
      AccessRight(int access) {
        accessRight = access;
      }

      public int accessRight() {
        return accessRight;
      }
    }

    /** Windows only method used to check if the current process has requested
     *  access rights on the given path. */
    private static native boolean access0(String path, int requestedAccess);

    /**
     * Checks whether the current process has desired access rights on
     * the given path.
     *
     * Longer term this native function can be substituted with JDK7
     * function Files#isReadable, isWritable, isExecutable.
     *
     * @param path input path
     * @param desiredAccess ACCESS_READ, ACCESS_WRITE or ACCESS_EXECUTE
     * @return true if access is allowed
     * @throws IOException I/O exception on error
     */
    public static boolean access(String path, AccessRight desiredAccess)
        throws IOException {
      return access0(path, desiredAccess.accessRight());
    }

    /**
     * Extends both the minimum and maximum working set size of the current
     * process.  This method gets the current minimum and maximum working set
     * size, adds the requested amount to each and then sets the minimum and
     * maximum working set size to the new values.  Controlling the working set
     * size of the process also controls the amount of memory it can lock.
     *
     * @param delta amount to increment minimum and maximum working set size
     * @throws IOException for any error
     * @see POSIX#mlock(ByteBuffer, long)
     */
    public static native void extendWorkingSetSize(long delta) throws IOException;

    static {
      initNativeLib();
    }
  }

  private static boolean nativeLoaded = false;

  static {
    initNativeLib();
  }

  private static void initNativeLib() {
    if (NativeCodeLoader.isNativeCodeLoaded()) {
      try {
        initNative();
        nativeLoaded = true;
      } catch (Throwable t) {
        LOG.debug("Unable to initialize NativeIO libraries", t);
      }
    }
  }

  /**
   * Return true if the JNI-based native IO extensions are available.
   */
  public static boolean isAvailable() {
    return NativeCodeLoader.isNativeCodeLoaded() && nativeLoaded;
  }

  /** Initialize the JNI method ID and class ID cache */
  private static native void initNative();

  /**
   * Get the maximum number of bytes that can be locked into memory at any
   * given point.
   *
   * @return 0 if no bytes can be locked into memory;
   *         Long.MAX_VALUE if there is no limit;
   *         The number of bytes that can be locked into memory otherwise.
   */
  static long getMemlockLimit() {
    return isAvailable() ? getMemlockLimit0() : 0;
  }

  private static native long getMemlockLimit0();

  /**
   * @return the operating system's page size.
   */
  static long getOperatingSystemPageSize() {
    try {
      Field f = Unsafe.class.getDeclaredField("theUnsafe");
      f.setAccessible(true);
      Unsafe unsafe = (Unsafe)f.get(null);
      return unsafe.pageSize();
    } catch (Throwable e) {
      LOG.warn("Unable to get operating system page size.  Guessing 4096.", e);
      return 4096;
    }
  }

  private static class CachedUid {
    final long timestamp;
    final String username;
    public CachedUid(String username, long timestamp) {
      this.timestamp = timestamp;
      this.username = username;
    }
  }

  private static boolean initialized = false;

  /**
   * The Windows logon name has two part, NetBIOS domain name and
   * user account name, of the format DOMAIN\UserName. This method
   * will remove the domain part of the full logon name.
   *
   * @param name full principal name containing the domain
   * @return name with domain removed
   */
  private static String stripDomain(String name) {
    int i = name.indexOf('\\');
    if (i != -1)
      name = name.substring(i + 1);
    return name;
  }

  /**
   * Create a FileInputStream that shares delete permission on the
   * file opened, i.e. other process can delete the file the
   * FileInputStream is reading. Only Windows implementation uses
   * the native interface.
   */
  public static FileInputStream getShareDeleteFileInputStream(File f)
      throws IOException {
    if (!Shell.WINDOWS) {
      // On Linux the default FileInputStream shares delete permission
      // on the file opened.
      //
      return new FileInputStream(f);
    } else {
      // Use Windows native interface to create a FileInputStream that
      // shares delete permission on the file opened.
      //
      FileDescriptor fd = Windows.createFile(
          f.getAbsolutePath(),
          Windows.GENERIC_READ,
          Windows.FILE_SHARE_READ |
              Windows.FILE_SHARE_WRITE |
              Windows.FILE_SHARE_DELETE,
          Windows.OPEN_EXISTING);
      return new FileInputStream(fd);
    }
  }

  /**
   * Create a FileInputStream that shares delete permission on the
   * file opened at a given offset, i.e. other process can delete
   * the file the FileInputStream is reading. Only Windows implementation
   * uses the native interface.
   */
  public static FileInputStream getShareDeleteFileInputStream(File f, long seekOffset)
      throws IOException {
    if (!Shell.WINDOWS) {
      RandomAccessFile rf = new RandomAccessFile(f, "r");
      if (seekOffset > 0) {
        rf.seek(seekOffset);
      }
      return new FileInputStream(rf.getFD());
    } else {
      // Use Windows native interface to create a FileInputStream that
      // shares delete permission on the file opened, and set it to the
      // given offset.
      //
      FileDescriptor fd = NativeIO.Windows.createFile(
          f.getAbsolutePath(),
          NativeIO.Windows.GENERIC_READ,
          NativeIO.Windows.FILE_SHARE_READ |
              NativeIO.Windows.FILE_SHARE_WRITE |
              NativeIO.Windows.FILE_SHARE_DELETE,
          NativeIO.Windows.OPEN_EXISTING);
      if (seekOffset > 0)
        NativeIO.Windows.setFilePointer(fd, seekOffset, NativeIO.Windows.FILE_BEGIN);
      return new FileInputStream(fd);
    }
  }

  /**
   * Create the specified File for write access, ensuring that it does not exist.
   * @param f the file that we want to create
   * @param permissions we want to have on the file (if security is enabled)
   *
   * @throws AlreadyExistsException if the file already exists
   * @throws IOException if any other error occurred
   */
  public static FileOutputStream getCreateForWriteFileOutputStream(File f, int permissions)
      throws IOException {
    if (!Shell.WINDOWS) {
      // Use the native wrapper around open(2)
      try {
        FileDescriptor fd = NativeIO.POSIX.open(f.getAbsolutePath(),
            NativeIO.POSIX.O_WRONLY | NativeIO.POSIX.O_CREAT
                | NativeIO.POSIX.O_EXCL, permissions);
        return new FileOutputStream(fd);
      } catch (NativeIOException nioe) {
        if (nioe.getErrno() == Errno.EEXIST) {
          throw new AlreadyExistsException(nioe);
        }
        throw nioe;
      }
    } else {
      // Use the Windows native APIs to create equivalent FileOutputStream
      try {
        FileDescriptor fd = NativeIO.Windows.createFile(f.getCanonicalPath(),
            NativeIO.Windows.GENERIC_WRITE,
            NativeIO.Windows.FILE_SHARE_DELETE
                | NativeIO.Windows.FILE_SHARE_READ
                | NativeIO.Windows.FILE_SHARE_WRITE,
            NativeIO.Windows.CREATE_NEW);
        NativeIO.POSIX.chmod(f.getCanonicalPath(), permissions);
        return new FileOutputStream(fd);
      } catch (NativeIOException nioe) {
        if (nioe.getErrorCode() == 80) {
          // ERROR_FILE_EXISTS
          // 80 (0x50)
          // The file exists
          throw new AlreadyExistsException(nioe);
        }
        throw nioe;
      }
    }
  }
  
  /**
   * A version of renameTo that throws a descriptive exception when it fails.
   *
   * @param src                  The source path
   * @param dst                  The destination path
   * 
   * @throws NativeIOException   On failure.
   */
  public static void renameTo(File src, File dst)
      throws IOException {
    if (!nativeLoaded) {
      if (!src.renameTo(dst)) {
        throw new IOException("renameTo(src=" + src + ", dst=" +
          dst + ") failed.");
      }
    } else {
      renameTo0(src.getAbsolutePath(), dst.getAbsolutePath());
    }
  }

  /**
   * A version of renameTo that throws a descriptive exception when it fails.
   *
   * @param src                  The source path
   * @param dst                  The destination path
   * 
   * @throws NativeIOException   On failure.
   */
  private static native void renameTo0(String src, String dst)
      throws NativeIOException;

  private static native void link0(String src, String dst)
      throws NativeIOException;

  /**
   * Unbuffered file copy from src to dst without tainting OS buffer cache
   *
   * In POSIX platform:
   * It uses FileChannel#transferTo() which internally attempts
   * unbuffered IO on OS with native sendfile64() support and falls back to
   * buffered IO otherwise.
   *
   * It minimizes the number of FileChannel#transferTo call by passing the the
   * src file size directly instead of a smaller size as the 3rd parameter.
   * This saves the number of sendfile64() system call when native sendfile64()
   * is supported. In the two fall back cases where sendfile is not supported,
   * FileChannle#transferTo already has its own batching of size 8 MB and 8 KB,
   * respectively.
   *
   * In Windows Platform:
   * It uses its own native wrapper of CopyFileEx with COPY_FILE_NO_BUFFERING
   * flag, which is supported on Windows Server 2008 and above.
   *
   * Ideally, we should use FileChannel#transferTo() across both POSIX and Windows
   * platform. Unfortunately, the wrapper(Java_sun_nio_ch_FileChannelImpl_transferTo0)
   * used by FileChannel#transferTo for unbuffered IO is not implemented on Windows.
   * Based on OpenJDK 6/7/8 source code, Java_sun_nio_ch_FileChannelImpl_transferTo0
   * on Windows simply returns IOS_UNSUPPORTED.
   *
   * Note: This simple native wrapper does minimal parameter checking before copy and
   * consistency check (e.g., size) after copy.
   * It is recommended to use wrapper function like
   * the Storage#nativeCopyFileUnbuffered() function with pre/post copy checks.
   *
   * @param src                  The source path
   * @param dst                  The destination path
   */
  public static void copyFileUnbuffered(File src, File dst) throws IOException {
    if (nativeLoaded && Shell.WINDOWS) {
      copyFileUnbuffered0(src.getAbsolutePath(), dst.getAbsolutePath());
    } else {
      FileInputStream fis = null;
      FileOutputStream fos = null;
      FileChannel input = null;
      FileChannel output = null;
      try {
        fis = new FileInputStream(src);
        fos = new FileOutputStream(dst);
        input = fis.getChannel();
        output = fos.getChannel();
        long remaining = input.size();
        long position = 0;
        long transferred;
        while (remaining > 0) {
          transferred = input.transferTo(position, remaining, output);
          remaining -= transferred;
          position += transferred;
        }
      } finally {
        RaftUtils.cleanup(LOG, output, fos, input, fis);
      }
    }
  }

  private static native void copyFileUnbuffered0(String src, String dst)
      throws NativeIOException;
}
