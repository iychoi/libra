/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package libra.common.hadoop.io.reader.map;

import java.io.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Provide access to an existing map.
 */
@SuppressWarnings({"rawtypes", "deprecation"}) 
public class IndexCloseableMapFileReader implements java.io.Closeable {

    private static final Log LOG = LogFactory.getLog(IndexCloseableMapFileReader.class);

    /**
     * Number of index entries to skip between each entry. Zero by default.
     * Setting this to values larger than zero can facilitate opening large map
     * files using less memory.
     */
    private int INDEX_SKIP = 0;

    private WritableComparator comparator;

    private WritableComparable nextKey;
    private long seekPosition = -1;
    private int seekIndex = -1;
    private long firstPosition;

    // the data, on disk
    private SequenceFile.Reader data;
    private SequenceFile.Reader index;

    // whether the index Reader was closed
    private boolean indexClosed = false;
    private boolean indexCacheClosed = false;

    // the index, in memory
    private int count = -1;
    private WritableComparable[] keys;
    private long[] positions;

    /**
     * Returns the class of keys in this file.
     */
    public Class<?> getKeyClass() {
        return data.getKeyClass();
    }

    /**
     * Returns the class of values in this file.
     */
    public Class<?> getValueClass() {
        return data.getValueClass();
    }

    /**
     * Construct a map reader for the named map.
     */
    public IndexCloseableMapFileReader(FileSystem fs, String dirName, Configuration conf) throws IOException {
        this(fs, dirName, null, conf);
        INDEX_SKIP = conf.getInt("io.map.index.skip", 0);
    }

    /**
     * Construct a map reader for the named map using the named comparator.
     */
    public IndexCloseableMapFileReader(FileSystem fs, String dirName, WritableComparator comparator, Configuration conf) throws IOException {
        this(fs, dirName, comparator, conf, true);
    }

    /**
     * Hook to allow subclasses to defer opening streams until further
     * initialization is complete.
     *
     * @see #createDataFileReader(FileSystem, Path, Configuration)
     */
    protected IndexCloseableMapFileReader(FileSystem fs, String dirName, WritableComparator comparator, Configuration conf, boolean open) throws IOException {
        if (open) {
            open(fs, dirName, comparator, conf);
        }
    }

    protected synchronized void open(FileSystem fs, String dirName, WritableComparator comparator, Configuration conf) throws IOException {
        Path dir = new Path(dirName);
        Path dataFile = new Path(dir, MapFile.DATA_FILE_NAME);
        Path indexFile = new Path(dir, MapFile.INDEX_FILE_NAME);

        // open the data
        this.data = createDataFileReader(fs, dataFile, conf);
        this.firstPosition = data.getPosition();

        if (comparator == null) {
            this.comparator = WritableComparator.get(data.getKeyClass().asSubclass(WritableComparable.class));
        } else {
            this.comparator = comparator;
        }

        // open the index
        this.index = new SequenceFile.Reader(fs, indexFile, conf);
    }

    /**
     * Override this method to specialize the type of
     * {@link SequenceFile.Reader} returned.
     */
    
    protected SequenceFile.Reader createDataFileReader(FileSystem fs, Path dataFile, Configuration conf) throws IOException {
        return new SequenceFile.Reader(fs, dataFile, conf);
    }

    private void readIndex() throws IOException {
        if (this.indexCacheClosed) {
            throw new IOException("readIndex failed because index cache in memory is already closed");
        }
        
        // read the index entirely into memory
        if (this.keys != null) {
            return;
        }
        this.count = 0;
        this.keys = new WritableComparable[1024];
        this.positions = new long[1024];
        try {
            int skip = INDEX_SKIP;
            LongWritable position = new LongWritable();
            WritableComparable lastKey = null;
            while (true) {
                WritableComparable k = comparator.newKey();

                if (!index.next(k, position)) {
                    break;
                }

                // check order to make sure comparator is compatible
                if (lastKey != null && comparator.compare(lastKey, k) > 0) {
                    throw new IOException("key out of order: " + k + " after " + lastKey);
                }
                lastKey = k;

                if (skip > 0) {
                    skip--;
                    continue;                             // skip this entry
                } else {
                    skip = INDEX_SKIP;                    // reset skip
                }

                if (count == keys.length) {                // time to grow arrays
                    int newLength = (keys.length * 3) / 2;
                    WritableComparable[] newKeys = new WritableComparable[newLength];
                    long[] newPositions = new long[newLength];
                    System.arraycopy(keys, 0, newKeys, 0, count);
                    System.arraycopy(positions, 0, newPositions, 0, count);
                    keys = newKeys;
                    positions = newPositions;
                }

                keys[count] = k;
                positions[count] = position.get();
                count++;
            }
        } catch (EOFException e) {
            LOG.warn("Unexpected EOF reading " + index
                    + " at entry #" + count + ".  Ignoring.");
        } finally {
            indexClosed = true;
            indexCacheClosed = false;
            index.close();
        }
    }

    /**
     * Re-positions the reader before its first key.
     */
    public synchronized void reset() throws IOException {
        data.seek(firstPosition);
    }

    /**
     * Get the key at approximately the middle of the file.
     *
     * @throws IOException
     */
    public synchronized WritableComparable midKey() throws IOException {
        readIndex();
        int pos = ((count - 1) / 2);              // middle of the index
        if (pos < 0) {
            throw new IOException("MapFile empty");
        }
        return keys[pos];
    }

    /**
     * Reads the final key from the file.
     *
     * @param key key to read into
     */
    public synchronized void finalKey(WritableComparable key) throws IOException {
        long originalPosition = data.getPosition(); // save position
        try {
            readIndex();                              // make sure index is valid
            if (count > 0) {
                data.seek(positions[count - 1]);          // skip to last indexed entry
            } else {
                reset();                                // start at the beginning
            }
            while (data.next(key)) {
            }                 // scan to eof
        } finally {
            data.seek(originalPosition);              // restore position
        }
    }

    /**
     * Positions the reader at the named key, or if none such exists, at the
     * first entry after the named key. Returns true iff the named key exists in
     * this map.
     */
    public synchronized boolean seek(WritableComparable key) throws IOException {
        return seekInternal(key) == 0;
    }

    /**
     * Positions the reader at the named key, or if none such exists, at the
     * first entry after the named key.
     *
     * @return 0 - exact match found < 0 - positioned at next record 1 - no more
     * records in file
     */
    private synchronized int seekInternal(WritableComparable key) throws IOException {
        return seekInternal(key, false);
    }

    /**
     * Positions the reader at the named key, or if none such exists, at the key
     * that falls just before or just after dependent on how the
     * <code>before</code> parameter is set.
     *
     * @param before - IF true, and <code>key</code> does not exist, position
     * file at entry that falls just before <code>key</code>. Otherwise,
     * position file at record that sorts just after.
     * @return 0 - exact match found < 0 - positioned at next record 1 - no more
     * records in file
     */
    private synchronized int seekInternal(WritableComparable key, final boolean before) throws IOException {
        readIndex();                                // make sure index is read

        if (seekIndex != -1 // seeked before
                && seekIndex + 1 < count
                && comparator.compare(key, keys[seekIndex + 1]) < 0 // before next indexed
                && comparator.compare(key, nextKey)
                >= 0) {                                 // but after last seeked
            // do nothing
        } else {
            seekIndex = binarySearch(key);
            if (seekIndex < 0) // decode insertion point
            {
                seekIndex = -seekIndex - 2;
            }

            if (seekIndex == -1) // belongs before first entry
            {
                seekPosition = firstPosition;           // use beginning of file
            } else {
                seekPosition = positions[seekIndex];    // else use index
            }
        }
        data.seek(seekPosition);

        if (nextKey == null) {
            nextKey = comparator.newKey();
        }

      // If we're looking for the key before, we need to keep track
        // of the position we got the current key as well as the position
        // of the key before it.
        long prevPosition = -1;
        long curPosition = seekPosition;

        while (data.next(nextKey)) {
            int c = comparator.compare(key, nextKey);
            if (c <= 0) {                             // at or beyond desired
                if (before && c != 0) {
                    if (prevPosition == -1) {
              // We're on the first record of this index block
                        // and we've already passed the search key. Therefore
                        // we must be at the beginning of the file, so seek
                        // to the beginning of this block and return c
                        data.seek(curPosition);
                    } else {
                        // We have a previous record to back up to
                        data.seek(prevPosition);
                        data.next(nextKey);
                        // now that we've rewound, the search key must be greater than this key
                        return 1;
                    }
                }
                return c;
            }
            if (before) {
                prevPosition = curPosition;
                curPosition = data.getPosition();
            }
        }

        return 1;
    }

    private int binarySearch(WritableComparable key) {
        int low = 0;
        int high = count - 1;

        while (low <= high) {
            int mid = (low + high) >>> 1;
            WritableComparable midVal = keys[mid];
            int cmp = comparator.compare(midVal, key);

            if (cmp < 0) {
                low = mid + 1;
            } else if (cmp > 0) {
                high = mid - 1;
            } else {
                return mid;                             // key found
            }
        }
        return -(low + 1);                          // key not found.
    }

    /**
     * Read the next key/value pair in the map into <code>key</code> and
     * <code>val</code>. Returns true if such a pair exists and false when at
     * the end of the map
     */
    public synchronized boolean next(WritableComparable key, Writable val) throws IOException {
        return data.next(key, val);
    }

    /**
     * Return the value for the named key, or null if none exists.
     */
    public synchronized Writable get(WritableComparable key, Writable val) throws IOException {
        if (seek(key)) {
            data.getCurrentValue(val);
            return val;
        } else {
            return null;
        }
    }

    /**
     * Finds the record that is the closest match to the specified key. Returns
     * <code>key</code> or if it does not exist, at the first entry after the
     * named key.
     *     * 
- * @param key - key that we're trying to find - * @param val - data
     * value if key is found - * @return - the key that was the closest match or
     * null if eof.
     */
    public synchronized WritableComparable getClosest(WritableComparable key, Writable val) throws IOException {
        return getClosest(key, val, false);
    }

    /**
     * Finds the record that is the closest match to the specified key.
     *
     * @param key - key that we're trying to find
     * @param val - data value if key is found
     * @param before - IF true, and <code>key</code> does not exist, return the
     * first entry that falls just before the <code>key</code>. Otherwise,
     * return the record that sorts just after.
     * @return - the key that was the closest match or null if eof.
     */
    public synchronized WritableComparable getClosest(WritableComparable key, Writable val, final boolean before) throws IOException {
        int c = seekInternal(key, before);

      // If we didn't get an exact match, and we ended up in the wrong
        // direction relative to the query key, return null since we
        // must be at the beginning or end of the file.
        if ((!before && c > 0)
                || (before && c < 0)) {
            return null;
        }

        data.getCurrentValue(val);
        return nextKey;
    }
    
    public synchronized void closeIndex() throws IOException {
        this.keys = null;
        this.positions = null;
        this.indexCacheClosed = true;
    }

    /**
     * Close the map.
     */
    public synchronized void close() throws IOException {
        if (!indexClosed) {
            index.close();
        }
        data.close();
    }
}
