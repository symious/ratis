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
package org.apache.raft.util;

import com.google.common.base.Preconditions;
import org.apache.hadoop.fs.FileUtil;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.raft.util.test.ExitUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.lang.reflect.Constructor;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;

public abstract class RaftUtils {
  public static final Logger LOG = LoggerFactory.getLogger(RaftUtils.class);
  private static final Class<?>[] EMPTY_CLASS_ARRAY = {};

  /**
   * Cache of constructors for each class. Pins the classes so they
   * can't be garbage collected until ReflectionUtils can be collected.
   */
  private static final Map<Class<?>, Constructor<?>> CONSTRUCTOR_CACHE =
      new ConcurrentHashMap<>();

  public static InterruptedIOException toInterruptedIOException(
      String message, InterruptedException e) {
    final InterruptedIOException iioe = new InterruptedIOException(message);
    iioe.initCause(e);
    return iioe;
  }

  public static IOException asIOException(Throwable t) {
    return t instanceof IOException? (IOException)t : new IOException(t);
  }

  public static IOException toIOException(ExecutionException e) {
    final Throwable cause = e.getCause();
    return cause != null? asIOException(cause): new IOException(e);
  }

  public static void truncateFile(File f, long target) throws IOException {
    try (FileOutputStream out = new FileOutputStream(f, true)) {
      out.getChannel().truncate(target);
    }
  }

  public static void deleteFile(File f) throws IOException {
    try {
      Files.delete(f.toPath());
    } catch (IOException e) {
      LOG.warn("Could not delete " + f);
      throw e;
    }
  }

  public static void deleteDir(File d) {
    FileUtil.fullyDelete(d);
  }

  public static void terminate(Throwable t, String message, Logger LOG) {
    LOG.error(message, t);
    ExitUtils.terminate(1, message);
  }

  /**
   * Interprets the passed string as a URI. In case of error it
   * assumes the specified string is a file.
   *
   * @param s the string to interpret
   * @return the resulting URI
   */
  public static URI stringAsURI(String s) throws IOException {
    URI u = null;
    // try to make a URI
    try {
      u = new URI(s);
    } catch (URISyntaxException e){
      LOG.error("Syntax error in URI " + s
          + ". Please check hdfs configuration.", e);
    }

    // if URI is null or scheme is undefined, then assume it's file://
    if(u == null || u.getScheme() == null){
      LOG.warn("Path " + s + " should be specified as a URI "
          + "in configuration files. Please update configuration.");
      u = fileAsURI(new File(s));
    }
    return u;
  }

  /**
   * Converts the passed File to a URI. This method trims the trailing slash if
   * one is appended because the underlying file is in fact a directory that
   * exists.
   *
   * @param f the file to convert
   * @return the resulting URI
   */
  public static URI fileAsURI(File f) throws IOException {
    URI u = f.getCanonicalFile().toURI();

    // trim the trailing slash, if it's present
    if (u.getPath().endsWith("/")) {
      String uriAsString = u.toString();
      try {
        u = new URI(uriAsString.substring(0, uriAsString.length() - 1));
      } catch (URISyntaxException e) {
        throw new IOException(e);
      }
    }
    return u;
  }

  /** Is the given object an instance of one of the given classes? */
  public static boolean isInstance(Object obj, Class<?>... classes) {
    for(Class<?> c : classes) {
      if (c.isInstance(obj)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Create an object for the given class and initialize it from conf
   *
   * @param theClass class of which an object is created
   * @return a new object
   */
  @SuppressWarnings("unchecked")
  public static <T> T newInstance(Class<T> theClass, Object... initArgs) {
    T result;
    try {
      Constructor<T> meth = (Constructor<T>) CONSTRUCTOR_CACHE.get(theClass);
      if (meth == null) {
        meth = theClass.getDeclaredConstructor(EMPTY_CLASS_ARRAY);
        meth.setAccessible(true);
        CONSTRUCTOR_CACHE.put(theClass, meth);
      }
      result = meth.newInstance(initArgs);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return result;
  }

  public static int getRandomBetween(int min, int max) {
    Preconditions.checkArgument(max > min);
    return ThreadLocalRandom.current().nextInt(max -min) + min;
  }

  public static void setLogLevel(Logger logger, Level level) {
    LogManager.getLogger(logger.getName()).setLevel(level);
  }


  public static void readFully(InputStream in, int buffSize) throws IOException {
    final byte buf[] = new byte[buffSize];
    for(int bytesRead = in.read(buf); bytesRead >= 0; ) {
      bytesRead = in.read(buf);
    }
  }

  /**
   * Reads len bytes in a loop.
   *
   * @param in InputStream to read from
   * @param buf The buffer to fill
   * @param off offset from the buffer
   * @param len the length of bytes to read
   * @throws IOException if it could not read requested number of bytes
   * for any reason (including EOF)
   */
  public static void readFully(InputStream in, byte[] buf, int off, int len)
      throws IOException {
    for(int toRead = len; toRead > 0; ) {
      final int ret = in.read(buf, off, toRead);
      if (ret < 0) {
        throw new IOException( "Premature EOF from inputStream");
      }
      toRead -= ret;
      off += ret;
    }
  }

  /**
   * Write a ByteBuffer to a FileChannel at a given offset,
   * handling short writes.
   *
   * @param fc               The FileChannel to write to
   * @param buf              The input buffer
   * @param offset           The offset in the file to start writing at
   * @throws IOException     On I/O error
   */
  public static void writeFully(FileChannel fc, ByteBuffer buf, long offset)
      throws IOException {
    do {
      offset += fc.write(buf, offset);
    } while (buf.remaining() > 0);
  }

  /**
   * Similar to readFully(). Skips bytes in a loop.
   * @param in The InputStream to skip bytes from
   * @param len number of bytes to skip.
   * @throws IOException if it could not skip requested number of bytes
   * for any reason (including EOF)
   */
  public static void skipFully(InputStream in, long len) throws IOException {
    long amt = len;
    while (amt > 0) {
      long ret = in.skip(amt);
      if (ret == 0) {
        // skip may return 0 even if we're not at EOF.  Luckily, we can
        // use the read() method to figure out if we're at the end.
        int b = in.read();
        if (b == -1) {
          throw new EOFException( "Premature EOF from inputStream after " +
              "skipping " + (len - amt) + " byte(s).");
        }
        ret = 1;
      }
      amt -= ret;
    }
  }

  /**
   * Close the Closeable objects and <b>ignore</b> any {@link Throwable} or
   * null pointers. Must only be used for cleanup in exception handlers.
   *
   * @param log the log to record problems to at debug level. Can be null.
   * @param closeables the objects to close
   */
  public static void cleanup(Logger log, Closeable... closeables) {
    for (Closeable c : closeables) {
      if (c != null) {
        try {
          c.close();
        } catch(Throwable e) {
          if (log != null && log.isDebugEnabled()) {
            log.debug("Exception in closing " + c, e);
          }
        }
      }
    }
  }
}
