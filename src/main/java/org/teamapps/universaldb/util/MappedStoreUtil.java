/*-
 * ========================LICENSE_START=================================
 * UniversalDB
 * ---
 * Copyright (C) 2014 - 2024 TeamApps.org
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
/*
 * Copyright (c) 2016 teamapps.org (see code comments for author's name)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.teamapps.universaldb.util;

import org.agrona.concurrent.AtomicBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashSet;
import java.util.Set;

public class MappedStoreUtil {
	public static ByteOrder BYTE_ORDER = ByteOrder.BIG_ENDIAN;

	private static Logger logger = LoggerFactory.getLogger(MappedStoreUtil.class);
	private static Set<String> bufferPathSet = new HashSet<>();


	public static AtomicBuffer createAtomicBuffer(File file, int bufferSize) {
		MappedByteBuffer mappedByteBuffer = createBuffer(file, bufferSize);
		return new UnsafeBuffer(mappedByteBuffer);
	}

    public static MappedByteBuffer createBuffer(File file, int bufferSize) {
        try {
			String path = file.getPath();
			if (bufferPathSet.contains(path)) {
				System.err.println("ERROR: trying to create a second buffer for the same path!:" + path);
				throw new RuntimeException("ERROR: trying to create a second buffer for the same path!:" + path);
			}
			bufferPathSet.add(path);
			file.getParentFile().mkdir();
            RandomAccessFile ras = new RandomAccessFile(file, "rw");
            if (!file.exists()) {
                ras.seek(bufferSize - 10);
                ras.write(new byte[10]);
            }
            MappedByteBuffer buffer = ras.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, bufferSize);
            try {
                ras.close();
            } catch (Throwable t) {
                logger.warn("Error releasing RAS file on buffer creation:" + t.getMessage() + ", file:" + file);
            }
            return buffer;
        } catch (IOException e) {
            logger.error("Error creating buffer:" + e.getMessage() +  ", file:" + file) ;
            e.printStackTrace();
        }
        return null;
    }

    public static void deleteBufferAndData(File file, MappedByteBuffer buffer) {
    	try {
			buffer.force();
		} catch (Throwable e) {
			e.printStackTrace();
		}
		try {
			releaseBufferUnsafe(buffer);
			file.delete();
			bufferPathSet.remove(file.getPath());
		} catch (Throwable e) {
			e.printStackTrace();
		}
	}

	public static void deleteBufferAndData(File file, AtomicBuffer atomicBuffer) {
		MappedByteBuffer buffer = (MappedByteBuffer) atomicBuffer.byteBuffer();
		deleteBufferAndData(file, buffer);
	}

    public static void releaseBufferUnsafe(MappedByteBuffer buffer) {
    	try {
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
