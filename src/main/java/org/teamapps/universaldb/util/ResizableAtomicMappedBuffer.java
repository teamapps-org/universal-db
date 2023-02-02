/*-
 * ========================LICENSE_START=================================
 * UniversalDB
 * ---
 * Copyright (C) 2014 - 2023 TeamApps.org
 * ---
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * =========================LICENSE_END==================================
 */
package org.teamapps.universaldb.util;

import org.agrona.concurrent.AtomicBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashSet;
import java.util.Set;

public class ResizableAtomicMappedBuffer {
    final static Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private static Set<String> bufferPathSet = new HashSet<>();

    private final File file;
    private int bufferSize;
    private AtomicBuffer buffer;

    public ResizableAtomicMappedBuffer(File file, int bufferSize) {
        this.file = file;
        this.bufferSize = file.exists() ? (int) Math.max(bufferSize, file.length()) : bufferSize;
        createBuffer();
    }

    private void createBuffer() {
        checkPath();
        file.getParentFile().mkdir();
        updateBufferSize();
    }

    private void checkPath() {
        String path = file.getPath();
        if (bufferPathSet.contains(path)) {
            System.err.println("ERROR: trying to create a second buffer for the same path!:" + path);
            throw new RuntimeException("ERROR: trying to create a second buffer for the same path!:" + path);
        }
        bufferPathSet.add(path);
    }

    public void ensureSize(int size) {
        if (size > bufferSize) {
            bufferSize = bufferSize * 2; //todo max 2 GB!
            updateBufferSize();
        }
    }

    public AtomicBuffer getBuffer() {
        return buffer;
    }

    private void updateBufferSize()  {
        try {
            RandomAccessFile  ras = new RandomAccessFile(file, "rw");
            if (!file.exists() || file.length() < bufferSize) {
                ras.seek(bufferSize - 4);
                ras.write(new byte[4]);
            }
            MappedByteBuffer mappedByteBuffer = ras.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, bufferSize);
            try {
                ras.close();
            } catch (Throwable t) {
                logger.warn("Error releasing RAS file on buffer creation:" + t.getMessage() + ", file:" + file);
            }
            AtomicBuffer oldBuffer = buffer;
            buffer = new UnsafeBuffer(mappedByteBuffer);
            releaseBufferUnsafe(oldBuffer);
        } catch (IOException e) {
            throw new RuntimeException("ERROR: updating buffer size of buffer:" + file.getPath(), e);
        }
    }

    public static void releaseBufferUnsafe(AtomicBuffer atomicBuffer) {
        try {
            MappedByteBuffer buffer = (MappedByteBuffer) atomicBuffer.byteBuffer();
            Class<?> unsafeClass = Class.forName("sun.misc.Unsafe");
            Field unsafeField = unsafeClass.getDeclaredField("theUnsafe");
            unsafeField.setAccessible(true);
            Object unsafe = unsafeField.get(null);
            Method invokeCleaner = unsafeClass.getMethod("invokeCleaner", ByteBuffer.class);
            invokeCleaner.invoke(unsafe, buffer);
        } catch (Throwable t) {
            t.printStackTrace();
        }
    }


}
