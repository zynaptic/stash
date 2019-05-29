/*
 * Zynaptic Stash - An asynchronous persistence framework for Java.
 * 
 * Copyright (c) 2009-2019, Zynaptic Limited.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 * 
 * Please visit www.zynaptic.com or contact reaction@zynaptic.com if you need
 * additional information or have any questions.
 */

package com.zynaptic.stash.core.file;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;

import com.zynaptic.reaction.Deferrable;
import com.zynaptic.reaction.Deferred;
import com.zynaptic.reaction.DeferredSplitter;
import com.zynaptic.reaction.Reactor;
import com.zynaptic.reaction.Threadable;
import com.zynaptic.reaction.Timeable;

/**
 * Provides the file access functionality for a single block of time series
 * data. This uses an open/append/close approach to updating the underlying time
 * series file, since it is assumed that updates are relatively infrequent and
 * this avoids holding a large number of open file handles.
 * 
 * @author Chris Holgate
 */
public final class StashFileTimeSeriesBlock<T extends Serializable> {

  // Handle on the reactor component for event management.
  private final Reactor reactor;

  // Local handle on the stash service backend.
  private final StashFileServiceBackend stashServiceBackend;

  // Local handle on the stash time series record.
  private final StashFileTimeSeriesRecord<T> timeSeriesRecord;

  // Timestamp at start of time series block range (inclusive).
  private final long startTimestamp;

  // Define current time series block state.
  private boolean timeSeriesBlockActive = false;

  // List of pending write operations.
  private List<DataPoint> writeQueueList = null;

  // Cached read data, formatted as a navigable map.
  private NavigableMap<Long, T> readDataMap = null;

  // Deferred splitter used for notifying completion of read operations.
  private DeferredSplitter<NavigableMap<Long, T>> deferredRead = null;

  // Deferred splitter used for notifying completion of write operations.
  private DeferredSplitter<Boolean> deferredWrite = null;

  // Deferred event object used to notify completion of invalidation process.
  private Deferred<Boolean> deferredInvalidate = null;;

  // Timeout handler used for cleaning out cached read data structures.
  private ReadCacheTimeoutHandler readCacheTimeoutHandler = null;

  /**
   * Provides default constructor which is used to create a new stash time series
   * block from a given set of time series metadata.
   * 
   * @param reactor This is a handle on the reactor component which is used for
   *   event management.
   * @param stashServiceBackend This is a handle on the main stash service backend
   *   object.
   * @param timeSeriesRecord This is the time series record object which contains
   *   this time series data block.
   * @param startTimestamp This is the timestamp for the start of the time series
   *   block range, expressed as the integer number of milliseconds since the Unix
   *   epoch (inclusive).
   */
  StashFileTimeSeriesBlock(Reactor reactor, StashFileServiceBackend stashServiceBackend,
      StashFileTimeSeriesRecord<T> timeSeriesRecord, long startTimestamp) {
    this.reactor = reactor;
    this.stashServiceBackend = stashServiceBackend;
    this.timeSeriesRecord = timeSeriesRecord;
    this.startTimestamp = startTimestamp;
  }

  /**
   * Accesses the timestamp which marks the start of the time series block range.
   * 
   * @return Returns the timestamp which is used to mark the start of the time
   *   series block range, expressed as the integer number of milliseconds since
   *   the Unix epoch (inclusive).
   */
  long getStartTimestamp() {
    return startTimestamp;
  }

  /**
   * Attempts to read back all the data points in the time series block as a
   * navigable map.
   * 
   * @return Returns a deferred event object which will have its callbacks
   *   executed on completion. The callback parameter will be a navigable map
   *   which contains all the data points in the time series block.
   */
  synchronized Deferred<NavigableMap<Long, T>> getDataPoints() {
    Deferred<NavigableMap<Long, T>> deferredResult;

    // Trap requests for invalidated time series blocks.
    if (deferredInvalidate != null) {
      throw new IllegalStateException("Attempted to access a time series block after it has been invalidated.");
    }

    // Return cached data if present.
    if (readDataMap != null) {
      resetDataCacheTimeout();
      return reactor.callDeferred(readDataMap);
    }

    // Queue up a new read request and process pending operations if required.
    if (deferredRead == null) {
      deferredRead = reactor.newDeferredSplitter();
      deferredResult = deferredRead.getOutputDeferred();
      processPendingOperations();
    }

    // Merge with outstanding read requests.
    else {
      deferredResult = deferredRead.getOutputDeferred();
    }
    return deferredResult;
  }

  /**
   * Attempts to write a new data point to the time series block.
   * 
   * @param timestamp This is the timestamp associated with the data point. It is
   *   assumed that the caller has validated it to be in the correct range.
   * @param data This is the data item to be written into the time series.
   * @return Returns a deferred event object which will have its callbacks
   *   executed on completion.
   */
  synchronized Deferred<Boolean> writeDataPoint(long timestamp, T data) {
    Deferred<Boolean> deferredStatus;
    DataPoint writeQueueEntry = new DataPoint(timestamp, data);

    // Trap requests for invalidated time series blocks.
    if (deferredInvalidate != null) {
      throw new IllegalStateException("Attempted to access a time series block after it has been invalidated.");
    }

    // If there is not currently an active write queue, create one and then
    // attempt to schedule a write task.
    if (deferredWrite == null) {
      deferredWrite = reactor.newDeferredSplitter();
      deferredStatus = deferredWrite.getOutputDeferred();
      writeQueueList = new LinkedList<DataPoint>();
      writeQueueList.add(writeQueueEntry);
      processPendingOperations();
    }

    // Just append the queued write operation if there are other active tasks
    // running.
    else {
      deferredStatus = deferredWrite.getOutputDeferred();
      writeQueueList.add(writeQueueEntry);
    }
    return deferredStatus;
  }

  /**
   * Invalidates the time series block, deleting the associated time series data
   * file.
   * 
   * @return Returns a deferred event object which will have its callbacks
   *   executed on completion.
   */
  synchronized Deferred<Boolean> invalidate() {

    // Trap requests for invalidated time series blocks.
    if (deferredInvalidate != null) {
      throw new IllegalStateException("Attempted to access a time series block after it has been invalidated.");
    }

    // Mark invalidation as a pending operation and attempt to process it.
    deferredInvalidate = reactor.newDeferred();
    processPendingOperations();
    return deferredInvalidate;
  }

  /*
   * Processes pending operations on task completion. Should be run with
   * synchronization lock held.
   */
  private void processPendingOperations() {
    if (timeSeriesBlockActive == false) {

      // Process outstanding write operations.
      if (deferredWrite != null) {
        timeSeriesBlockActive = true;
        deferredWrite.addInputDeferred(
            reactor.runThread(new DataPointsWriteTask(), writeQueueList).addDeferrable(new DataPointsWriteHandler()));
        deferredWrite = null;
        writeQueueList = null;

        // Invalidate the cached read data. This will force all the file
        // contents to be read back from the persisted file once this write
        // operation has completed.
        readDataMap = null;
        cancelDataCacheTimeout();
      }

      // Process outstanding read operation.
      else if (deferredRead != null) {
        timeSeriesBlockActive = true;
        deferredRead.addInputDeferred(
            reactor.runThread(new DataPointsReadTask(), null).addDeferrable(new DataPointsReadHandler()));
        deferredRead = null;
      }

      // Processes outstanding invalidation. This issues a callback to indicate
      // that there are no other pending operations on the time series block,
      // but leaves file deletion to the common cleanup code.
      else if (deferredInvalidate != null) {
        timeSeriesBlockActive = true;
        readDataMap = null;
        cancelDataCacheTimeout();
        deferredInvalidate.callback(true);
      }
    }
  }

  /*
   * Resets the read cache timeout. Should be run with synchronization lock held.
   */
  private void resetDataCacheTimeout() {
    if (readCacheTimeoutHandler == null) {
      readCacheTimeoutHandler = new ReadCacheTimeoutHandler();
    }
    reactor.runTimerOneShot(readCacheTimeoutHandler, stashServiceBackend.getCacheTimeout(), null);
  }

  /*
   * Cancels the read cache timeout. Should be run with synchronization lock held.
   */
  private void cancelDataCacheTimeout() {
    if (readCacheTimeoutHandler != null) {
      reactor.cancelTimer(readCacheTimeoutHandler);
      readCacheTimeoutHandler = null;
    }
  }

  /*
   * Performs data point write operations. This opens the time series block file,
   * appends the outstanding data entries and closes it on completion.
   */
  private final class DataPointsWriteTask implements Threadable<List<DataPoint>, Boolean> {
    public Boolean run(List<DataPoint> writeQueueList) throws FileNotFoundException, IOException {

      // Determine data file to be used.
      File objectDataDir = StashFileUtils.stashIdToPath(stashServiceBackend.getStashDataDir(),
          timeSeriesRecord.getStashId());
      File objectDataFile = new File(objectDataDir, "@" + startTimestamp);
      ObjectOutputStream objectDataOutputStream = null;

      // Attempt to append the new time series data entries. If the file already
      // exists, we need to use an appending object data stream which strips out
      // the serialisation header information.
      try {
        if (objectDataFile.exists()) {
          objectDataOutputStream = new AppendingObjectOutputStream(
              new BufferedOutputStream(new FileOutputStream(objectDataFile, true)));
        } else {
          objectDataOutputStream = new ObjectOutputStream(
              new BufferedOutputStream(new FileOutputStream(objectDataFile, true)));
        }
        StashFileObjectStreamer<T> objectStreamer = timeSeriesRecord.getObjectStreamer();
        for (DataPoint dataPoint : writeQueueList) {
          objectDataOutputStream.writeLong(dataPoint.getTimestamp());
          objectStreamer.write(objectDataOutputStream, dataPoint.getData());
        }
      } finally {
        if (objectDataOutputStream != null) {
          objectDataOutputStream.close();
        }
      }
      return true;
    }
  }

  /*
   * Implements completion handling for writes to the time series block file. Note
   * that any error condition encountered during writes is forwarded to all
   * deferred write handlers.
   */
  private final class DataPointsWriteHandler implements Deferrable<Boolean, Boolean> {
    public Boolean onCallback(Deferred<Boolean> deferred, Boolean status) {
      synchronized (StashFileTimeSeriesBlock.this) {
        timeSeriesBlockActive = false;
        processPendingOperations();
      }
      return status;
    }

    public Boolean onErrback(Deferred<Boolean> deferred, Exception error) throws Exception {
      synchronized (StashFileTimeSeriesBlock.this) {
        timeSeriesBlockActive = false;
        processPendingOperations();
      }
      throw error;
    }
  }

  /*
   * Performs data point read operations. This opens the time series block file
   * and reads all the contents, placing them into a navigable map data structure
   * indexed by the timestamp values.
   */
  private final class DataPointsReadTask implements Threadable<Boolean, NavigableMap<Long, T>> {
    public NavigableMap<Long, T> run(Boolean status) throws FileNotFoundException, IOException, ClassNotFoundException {
      NavigableMap<Long, T> dataPointMap = new TreeMap<Long, T>();

      // Determine data file to be used.
      File objectDataDir = StashFileUtils.stashIdToPath(stashServiceBackend.getStashDataDir(),
          timeSeriesRecord.getStashId());
      File objectDataFile = new File(objectDataDir, "@" + startTimestamp);
      ObjectInputStream objectDataInputStream = null;

      // Attempt to read back the input data objects.
      try {
        objectDataInputStream = new ResolvedObjectInputStream(
            new BufferedInputStream(new FileInputStream(objectDataFile)), timeSeriesRecord.getDataClassLoader());
        StashFileObjectStreamer<T> objectStreamer = timeSeriesRecord.getObjectStreamer();
        while (true) {
          Long nextTimestamp = objectDataInputStream.readLong();
          T rawObjectData = objectStreamer.read(objectDataInputStream);

          // Performs runtime type checking on the time series data.
          timeSeriesRecord.runtimeTypeCheck(rawObjectData);
          dataPointMap.put(nextTimestamp, rawObjectData);
        }
      }

      // Terminate at end of file condition. We assume that this is a clean EOF
      // and that no data corruption has occurred at the end of the file. This
      // has the side effect that corrupted data at the end of a file may be
      // silently discarded.
      catch (EOFException eofDetected) {
        // Exits while(true) loop.
      } finally {
        if (objectDataInputStream != null) {
          objectDataInputStream.close();
        }
      }
      return dataPointMap;
    }
  }

  /*
   * Implements completion handling for reads of the time series block file.
   */
  private final class DataPointsReadHandler implements Deferrable<NavigableMap<Long, T>, NavigableMap<Long, T>> {
    public NavigableMap<Long, T> onCallback(Deferred<NavigableMap<Long, T>> deferred,
        NavigableMap<Long, T> dataPointMap) {
      synchronized (StashFileTimeSeriesBlock.this) {
        timeSeriesBlockActive = false;
        readDataMap = dataPointMap;
        resetDataCacheTimeout();
        processPendingOperations();
      }
      return dataPointMap;
    }

    public NavigableMap<Long, T> onErrback(Deferred<NavigableMap<Long, T>> deferred, Exception error) throws Exception {
      synchronized (StashFileTimeSeriesBlock.this) {
        timeSeriesBlockActive = false;
        processPendingOperations();
      }
      throw error;
    }
  }

  /*
   * Implements timeout handling for the cached read data. Invalidates the cached
   * data and marks the cache timeout as being inactive.
   */
  private final class ReadCacheTimeoutHandler implements Timeable<Integer> {
    public void onTick(Integer data) {
      synchronized (StashFileTimeSeriesBlock.this) {
        readDataMap = null;
        readCacheTimeoutHandler = null;
      }
    }
  }

  /*
   * Subclass the output object data stream to support appending to existing
   * files.
   * 
   * TODO: Avoid writing the class descriptor for each time series data point,
   * since it should be possible to pick this up from the start of the original
   * file. This optimisation will have no benefit for native types or arrays of
   * native types, since these already have an efficient encoding. Note that this
   * potential future optimisation is the reason that an exact match to the
   * default data value type is required for all data writes, since supporting
   * multiple different derived classes would become very difficult.
   */
  private final class AppendingObjectOutputStream extends ObjectOutputStream {
    public AppendingObjectOutputStream(OutputStream out) throws IOException {
      super(out);
    }

    @Override
    protected void writeStreamHeader() throws IOException {
      // Avoid writing the header and then reset the stream state.
      reset();
    }
  }

  /*
   * Provides a data point class which encapsulates the timestamp and associated
   * data for each data point.
   */
  private final class DataPoint {
    private final long timestamp;
    private final T data;

    private DataPoint(long timestamp, T data) {
      this.timestamp = timestamp;
      this.data = data;
    }

    private long getTimestamp() {
      return timestamp;
    }

    private T getData() {
      return data;
    }
  }
}
