/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/* This code copyright (c) grossman;
 *from http://jo-widgets.googlecode.com/svn/tags/0.29.0/modules/core/org.jowidgets.util/src/main/java/org/jowidgets/util/io/LazyBufferedInputStream.java*/

package org.apache.spark.network.util;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * This class provides an buffered input stream that creates its buffer lazily and destroys the buffer when
 * the stream get closed.
 *
 * This class is not threadsafe.
 */
public class LazyBufferedInputStream extends InputStream {

  private final InputStream original;
  private final int bufferSize;

  private BufferedInputStream bufferedInputStream;
  private boolean closed;

  public LazyBufferedInputStream(final InputStream original, final int bufferSize) {
    this.original = original;
    this.bufferSize = bufferSize;
  }

  @Override
  public int read() throws IOException {
    return getInputStream().read();
  }

  @Override
  public int read(final byte[] b) throws IOException {
    return getInputStream().read(b);
  }

  @Override
  public int read(final byte[] b, final int off, final int len) throws IOException {
    return getInputStream().read(b, off, len);
  }

  @Override
  public long skip(final long n) throws IOException {
    return getInputStream().skip(n);
  }

  @Override
  public int available() throws IOException {
    return getInputStream().available();
  }

  @Override
  public void close() throws IOException {
    closed = true;
    if (bufferedInputStream != null) {
      bufferedInputStream.close();
      bufferedInputStream = null;
    }
  }

  @Override
  public synchronized void mark(final int readlimit) {
    try {
      getInputStream().mark(readlimit);
    }
    catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public synchronized void reset() throws IOException {
    getInputStream().reset();
  }

  @Override
  public boolean markSupported() {
    return original.markSupported();
  }

  private InputStream getInputStream() throws IOException {
    if (closed) {
      throw new IOException("Stream was closed");
    }
    if (bufferedInputStream == null) {
      bufferedInputStream = new BufferedInputStream(original, bufferSize);
    }
    return bufferedInputStream;
  }
}