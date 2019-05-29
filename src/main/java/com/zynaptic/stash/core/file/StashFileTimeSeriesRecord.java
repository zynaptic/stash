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
import java.io.PrintStream;
import java.io.Serializable;
import java.lang.reflect.Array;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

import com.zynaptic.reaction.Deferrable;
import com.zynaptic.reaction.Deferred;
import com.zynaptic.reaction.DeferredConcentrator;
import com.zynaptic.reaction.Reactor;
import com.zynaptic.reaction.Threadable;
import com.zynaptic.stash.StashTimeSeriesEntry;
import com.zynaptic.stash.core.StashEntryRecord;
import com.zynaptic.stash.core.StashTimeSeriesMetadata;

/**
 * Provides a stash time series record implementation for use with the simple
 * filesystem stash backend.
 * 
 * @param <T> This is the generic type specifier which determines the class of
 *   objects which may be stored in the stash via a given stash time series
 *   record.
 * 
 * @author Chris Holgate
 */
public final class StashFileTimeSeriesRecord<T extends Serializable> extends StashEntryRecord<T>
    implements StashTimeSeriesEntry<T> {

  // Handle on the reactor component for event management.
  private final Reactor reactor;

  // Local handle on the stash service backend.
  private final StashFileServiceBackend stashServiceBackend;

  // Specify the current stash record activity state.
  private boolean stashRecordActive = false;

  // Map of starting timestamps to their associated time series blocks.
  private NavigableMap<Long, StashFileTimeSeriesBlock<T>> timeSeriesBlockMap = null;

  // Handle on the most recent time series block parameters
  private TimeSeriesBlockParams currentBlockParams = null;

  // Write queue which is used to hold pending write operations during startup.
  private LinkedList<StartupWriteQueueEntry> startupWriteQueue = null;

  // Deferred event object used to notify completion of invalidation process.
  private Deferred<Boolean> deferredInvalidate = null;

  // Object data streamer which is to be used for data formatting.
  private StashFileObjectStreamer<T> objectStreamer = null;

  /**
   * Provides static creator method which is used to initialise a new stash time
   * series record.
   * 
   * @param reactor This is a handle on the reactor component which is used for
   *   event management.
   * @param stashServiceBackend This is a handle on the main stash service backend
   *   object.
   * @param stashEntryMetadata This is the stash entry metadata which is used to
   *   generate the stash time series record.
   * @param initValue This is the initial default value which is to be assigned to
   *   the stash time series record.
   * @return Returns a deferred event object which will have its callbacks
   *   executed on completion, passing a reference to the newly created stash time
   *   series record.
   */
  static <U extends Serializable> Deferred<StashTimeSeriesEntry<U>> initRecord(Reactor reactor,
      StashFileServiceBackend stashServiceBackend, StashTimeSeriesMetadata<U> stashEntryMetadata, U initValue) {
    StashFileTimeSeriesRecord<U> stashTimeSeriesRecord = new StashFileTimeSeriesRecord<U>(reactor, stashServiceBackend,
        stashEntryMetadata);
    return stashTimeSeriesRecord.initThisRecord(initValue);
  }

  /**
   * Provides static creator method which is used to rebuild a stash time series
   * record from the supplied stash entry metadata. Note that this method can
   * block since it implements file system operations.
   * 
   * @param reactor This is a handle on the reactor component which is used for
   *   event management.
   * @param stashServiceBackend This is a handle on the main stash service backend
   *   object.
   * @param stashEntryMetadata This is the stash entry metadata which is used to
   *   generate the stash time series record.
   * @return Returns the stash time series record which was generated using the
   *   supplied metadata.
   * @throws FileNotFoundException This exception will be thrown if no stash data
   *   files could be found.
   */
  static <U extends Serializable> StashFileTimeSeriesRecord<U> rebuildRecord(Reactor reactor,
      StashFileServiceBackend stashServiceBackend, StashTimeSeriesMetadata<U> stashEntryMetadata)
      throws FileNotFoundException {
    StashFileTimeSeriesRecord<U> stashTimeSeriesRecord = new StashFileTimeSeriesRecord<U>(reactor, stashServiceBackend,
        stashEntryMetadata);
    stashTimeSeriesRecord.rebuildThisRecord();
    return stashTimeSeriesRecord;
  }

  /*
   * Provides private constructor which is used to create a new stash time series
   * record from a given set of stash entry metadata.
   */
  private StashFileTimeSeriesRecord(Reactor reactor, StashFileServiceBackend stashServiceBackend,
      StashTimeSeriesMetadata<T> stashEntryMetadata) {
    super(stashEntryMetadata);
    this.reactor = reactor;
    this.stashServiceBackend = stashServiceBackend;
  }

  /*
   * Initialises the stash time series record when it is originally registered.
   */
  private synchronized Deferred<StashTimeSeriesEntry<T>> initThisRecord(T initData) {
    stashRecordActive = true;
    try {
      setDataClassLoader(initData.getClass().getClassLoader());
    } catch (ClassNotFoundException error) {
      throw new RuntimeException("Initial stash data value should know its own class loader!", error);
    }
    return reactor.runThread(new StashTimeSeriesInitTask(), initData).addDeferrable(new StashTimeSeriesInitHandler());
  }

  /*
   * Sets up the stash time series record on a warm start.
   */
  private synchronized void rebuildThisRecord() throws FileNotFoundException {
    timeSeriesBlockMap = new TreeMap<Long, StashFileTimeSeriesBlock<T>>();

    // Determine the set of time series block files by examining the data
    // directory.
    File timeSeriesDataDir = StashFileUtils.stashIdToPath(stashServiceBackend.getStashDataDir(), getStashId());
    String[] timeSeriesBlockFiles = timeSeriesDataDir.list();

    // A null file list implies that the data directory is not present.
    if (timeSeriesBlockFiles == null) {
      throw new FileNotFoundException("No time series data directory found for stash entry " + getStashId());
    }

    // Create the time series block objects and insert them into the map.
    for (int i = 0; i < timeSeriesBlockFiles.length; i++) {
      if (timeSeriesBlockFiles[i].charAt(0) == '@') {
        try {
          long thisStartTimestamp = Long.parseLong(timeSeriesBlockFiles[i].substring(1));
          timeSeriesBlockMap.put(thisStartTimestamp,
              new StashFileTimeSeriesBlock<T>(reactor, stashServiceBackend, this, thisStartTimestamp));
        } catch (NumberFormatException error) {
          // Ignore files with invalid number formats.
        }
      }
    }

    // At least one time series data block should be present.
    if (timeSeriesBlockMap.isEmpty()) {
      throw new FileNotFoundException("No time series data block files found for stash entry " + getStashId());
    }
  }

  /*
   * Implements StashTimeSeriesEntry.getStashId()
   */
  public String getStashId() {
    return getMetadata().getStashId();
  }

  /*
   * Implements StashTimeSeriesEntry.getStashName()
   */
  public String getStashName() {
    return getMetadata().getStashName();
  }

  /*
   * Implements StashTimeSeriesEntry.getDefaultValue()
   */
  public T getDefaultValue() {
    return getDefaultData();
  }

  /*
   * Implements StashTimeSeriesEntry.getDataBlockPeriod()
   */
  public int getDataBlockPeriod() {
    StashTimeSeriesMetadata<T> timeSeriesMetadata = (StashTimeSeriesMetadata<T>) getMetadata();
    return timeSeriesMetadata.getDataBlockPeriod();
  }

  /*
   * Implements StashTimeSeriesEntry.getDataBlockSize()
   */
  public int getDataBlockSize() {
    StashTimeSeriesMetadata<T> timeSeriesMetadata = (StashTimeSeriesMetadata<T>) getMetadata();
    return timeSeriesMetadata.getDataBlockSize();
  }

  /*
   * Implements StashTimeSeriesEntry.getCurrentValue()
   */
  public Deferred<T> getCurrentValue() {
    return getDataPointValue(System.currentTimeMillis());
  }

  /*
   * Implements StashTimeSeriesEntry.getDataPointValue(...)
   */
  public synchronized Deferred<T> getDataPointValue(long timestamp) {

    // Trap requests for invalidated stash IDs.
    if (deferredInvalidate != null) {
      throw new IllegalStateException("Attempted to access a stash entry after it has been invalidated.");
    }

    // Check the parameter validity.
    if (timestamp < 0) {
      throw new IllegalArgumentException("Invalid timestamp selected when accessing data point value.");
    }

    // Get the time series block containing this timestamp.
    StashFileTimeSeriesBlock<T> readBlock = timeSeriesBlockMap.floorEntry(timestamp).getValue();

    // Read back the contents of the block file and select the individual entry
    // in the callback handler.
    return readBlock.getDataPoints().addDeferrable(new DataPointValueSelectionHandler(timestamp));
  }

  /*
   * Implements StashTimeSeriesEntry.getDataPointSet()
   */
  public Deferred<NavigableMap<Long, T>> getDataPointSet() {
    return getDataPointSet(0, Long.MAX_VALUE);
  }

  /*
   * Implements StashTimeSeriesEntry.getDataPointSet(...)
   */
  public synchronized Deferred<NavigableMap<Long, T>> getDataPointSet(long startTimestamp, long endTimestamp) {

    // Trap requests for invalidated stash IDs.
    if (deferredInvalidate != null) {
      throw new IllegalStateException("Attempted to access a stash entry after it has been invalidated.");
    }

    // Check the parameter validity.
    if ((startTimestamp < 0) || (endTimestamp < startTimestamp)) {
      throw new IllegalArgumentException("Invalid timestamp range selected when accessing data point set.");
    }

    // Get the time series block containing the start timestamp.
    DeferredConcentrator<NavigableMap<Long, T>> deferredConcentrator = reactor.newDeferredConcentrator();
    deferredConcentrator.addInputDeferred(timeSeriesBlockMap.floorEntry(startTimestamp).getValue().getDataPoints());

    // Get the set of time series blocks which span the remaining timestamp
    // range.
    NavigableMap<Long, StashFileTimeSeriesBlock<T>> subMap = timeSeriesBlockMap.subMap(startTimestamp, false,
        endTimestamp, true);
    for (StashFileTimeSeriesBlock<T> timeSeriesBlock : subMap.values()) {
      deferredConcentrator.addInputDeferred(timeSeriesBlock.getDataPoints());
    }

    // Select the appropriate subset of data point entries in the callback
    // handler.
    return deferredConcentrator.getOutputDeferred()
        .addDeferrable(new DataPointSetSelectionHandler(startTimestamp, endTimestamp));
  }

  /*
   * Implements StashTimeSeriesEntry.setCurrentValue(...)
   */
  public Deferred<Boolean> setCurrentValue(T newValue) {
    return setDataPointValue(System.currentTimeMillis(), newValue);
  }

  /*
   * Implements StashTimeSeriesEntry.setDataPointValue(...)
   */
  public synchronized Deferred<Boolean> setDataPointValue(long timestamp, T newValue) {

    // Trap requests for invalidated stash IDs.
    if (deferredInvalidate != null) {
      throw new IllegalStateException("Attempted to access a stash entry after it has been invalidated.");
    }

    // Check the parameter validity.
    if (timestamp <= 0) {
      throw new IllegalArgumentException("Invalid timestamp selected when accessing data point value.");
    } else if (newValue == null) {
      throw new NullPointerException("Null data point values are not valid.");
    }

    // Perform runtime check against the default data value. An exact match to
    // the data type is required.
    runtimeTypeCheck(newValue);

    // Determines if this is one of the first writes after a warm start, in
    // which case the block write state variables need to be set up.
    if (currentBlockParams == null) {
      if (startupWriteQueue == null) {
        startupWriteQueue = new LinkedList<StartupWriteQueueEntry>();
      }
      Deferred<Boolean> deferredResult = reactor.newDeferred();
      startupWriteQueue.add(new StartupWriteQueueEntry(deferredResult, timestamp, newValue));
      processPendingOperations();
      return deferredResult.makeRestricted();
    }

    // Perform a normal write operation.
    else {
      return doDataPointWrite(timestamp, newValue);
    }
  }

  /*
   * Implements StashEntryRecord.invalidate()
   */
  @Override
  public synchronized Deferred<Boolean> invalidate() {

    // Trap requests for invalidated stash IDs.
    if (deferredInvalidate != null) {
      throw new IllegalStateException("Attempted to access a stash entry after it has been invalidated.");
    }

    // Queue up invalidation request.
    deferredInvalidate = reactor.newDeferred();
    processPendingOperations();
    return deferredInvalidate.makeRestricted();
  }

  /*
   * Implements StashEntryRecord.reportRecordContents()
   */
  @Override
  public synchronized void reportRecordContents(PrintStream printStream) throws IOException {

    // Use a sensible set of units for the block period.
    float dataBlockPeriod = getDataBlockPeriod();
    String periodUnits = " ms";
    if (dataBlockPeriod >= (24 * 60 * 60 * 1000)) {
      dataBlockPeriod /= (24 * 60 * 60 * 1000);
      periodUnits = " days";
    } else if (dataBlockPeriod >= (60 * 60 * 1000)) {
      dataBlockPeriod /= (60 * 60 * 1000);
      periodUnits = " hours";
    } else if (dataBlockPeriod >= (60 * 1000)) {
      dataBlockPeriod /= (60 * 1000);
      periodUnits = " mins";
    } else if (dataBlockPeriod >= 1000) {
      dataBlockPeriod /= 1000;
      periodUnits = " secs";
    }

    // Add common information to the stash entry report.
    printStream.println("Stash ID : " + getStashId());
    printStream.println("  Stash name    : " + getStashName());
    printStream.println("  Entry format  : Time Series Data");
    Object defaultData = getDefaultData();
    printStream.println(
        "  Data type     : " + ((defaultData == null) ? "UNKNOWN" : defaultData.getClass().getCanonicalName()));

    // Add time series specific information to the stash entry report.
    printStream.println("  Block period  : " + dataBlockPeriod + periodUnits);
    printStream.println("  Block size    : " + getDataBlockSize());
    printStream.println("  Last update   : "
        + ((currentBlockParams == null) ? "UNKNOWN" : new Date(currentBlockParams.getEndTimestamp()).toString()));
  }

  /*
   * Gets the object streamer which is to be used for efficiently serializing and
   * deserializing the time series data.
   */
  synchronized StashFileObjectStreamer<T> getObjectStreamer() {
    if (objectStreamer == null) {
      objectStreamer = StashFileObjectStreamer.create(getDefaultData());
    }
    return objectStreamer;
  }

  /*
   * Performs runtime type checking against the default value.
   */
  void runtimeTypeCheck(Object checkedObject) {
    T defaultData = getDefaultData();

    // Check for matching base types.
    if (!checkedObject.getClass().equals(defaultData.getClass())) {
      throw new ClassCastException("Inconsistent object type for stash entry " + getStashId());
    }

    // Check that arrays contain the correct primitive data types and have
    // dimensions consistent with the default value. This does not apply to one
    // dimensional byte arrays, which are treated as opaque blocks of raw data.
    if ((defaultData.getClass().isArray()) && (!(defaultData instanceof byte[]))) {
      arrayTypeCheck(defaultData, checkedObject);
    }
  }

  /*
   * Performs recursive array checking, such that the two arrays have the same
   * dimension, number of entries and underlying data types.
   */
  private void arrayTypeCheck(Object defaultValue, Object checkedObject) {

    // Check the array entry type and size.
    if (!defaultValue.getClass().getComponentType().equals(checkedObject.getClass().getComponentType())) {
      throw new ClassCastException("Inconsistent array contents type for stash entry " + getStashId());
    }
    if (Array.getLength(defaultValue) != Array.getLength(checkedObject)) {
      throw new ArrayIndexOutOfBoundsException("Inconsistent array dimensions for stash entry " + getStashId());
    }

    // Recursively check multidimensional arrays.
    if (defaultValue.getClass().getComponentType().isArray()) {
      for (int i = 0; i < Array.getLength(defaultValue); i++) {
        arrayTypeCheck(Array.get(defaultValue, i), Array.get(checkedObject, i));
      }
    }
  }

  /*
   * Performs a data point write operation to the appropriate data block. Must be
   * called with the synchronization lock held.
   */
  private Deferred<Boolean> doDataPointWrite(long timestamp, T newValue) {

    // Get the time series block containing this timestamp.
    StashFileTimeSeriesBlock<T> writeBlock = timeSeriesBlockMap.floorEntry(timestamp).getValue();

    // Process writes to current block, creating a new time series data block
    // file if the block period for the current one has expired.
    if (writeBlock == currentBlockParams.getBlock()) {
      if ((timestamp - currentBlockParams.getStartTimestamp() >= getDataBlockPeriod())
          && (currentBlockParams.getEntryCount() >= getDataBlockSize())
          && (timestamp > currentBlockParams.getEndTimestamp())) {
        currentBlockParams = new TimeSeriesBlockParams(
            new StashFileTimeSeriesBlock<T>(reactor, stashServiceBackend, this, timestamp), 1, timestamp, timestamp);
        timeSeriesBlockMap.put(timestamp, currentBlockParams.getBlock());
        writeBlock = currentBlockParams.getBlock();
      } else {
        currentBlockParams.incrementEntryCount();
        currentBlockParams.updateEndTimestamp(timestamp);
      }
    }

    // Perform a write to the selected data block.
    return writeBlock.writeDataPoint(timestamp, newValue);
  }

  /*
   * Processes pending operations on task completion. Should be run with
   * synchronization lock held.
   */
  private void processPendingOperations() {
    if (stashRecordActive == false) {

      // Perform setup on first write operation.
      if (startupWriteQueue != null) {
        stashRecordActive = true;
        reactor.runThread(new FirstWriteSetupTask(), startupWriteQueue.peek().getDataValue())
            .addDeferrable(new FirstWriteSetupHandler(), true);
      }

      // Invalidate stash entry if requested.
      else if (deferredInvalidate != null) {
        stashRecordActive = true;

        // Invalidate all of the stash data block objects and wait for them to
        // complete.
        DeferredConcentrator<Boolean> deferredConcentrator = reactor.newDeferredConcentrator();
        for (StashFileTimeSeriesBlock<T> invalidationBlock : timeSeriesBlockMap.values()) {
          deferredConcentrator.addInputDeferred(invalidationBlock.invalidate());
        }

        // Discard the time series block map, preventing further accesses.
        timeSeriesBlockMap = null;
        deferredConcentrator.getOutputDeferred().addDeferrable(new BlockInvalidateHandler(), true);
      }
    }
  }

  /*
   * Performs initialisation task on creating a new stash time series record.
   */
  private final class StashTimeSeriesInitTask implements Threadable<T, Boolean> {
    public Boolean run(T initData) throws Exception {

      // Write the stash object metadata to the stash index.
      File metadataFile = new File(stashServiceBackend.getStashIndexDir(), getStashId());
      ObjectOutputStream metadataOutputStream = null;
      try {
        metadataOutputStream = new ObjectOutputStream(new BufferedOutputStream(new FileOutputStream(metadataFile)));
        metadataOutputStream.writeObject(getMetadata());
      } finally {
        if (metadataOutputStream != null) {
          metadataOutputStream.close();
        }
      }

      // Create the initial time series block file, writing the default value
      // at the start with timestamp zero.
      File timeSeriesDataDir = StashFileUtils.stashIdToPath(stashServiceBackend.getStashDataDir(), getStashId());
      File timeSeriesDataFile = new File(timeSeriesDataDir, "@0");
      ObjectOutputStream timeSeriesOutputStream = null;
      try {
        timeSeriesDataDir.mkdirs();
        timeSeriesOutputStream = new ObjectOutputStream(
            new BufferedOutputStream(new FileOutputStream(timeSeriesDataFile)));
        timeSeriesOutputStream.writeLong(0);
        StashFileObjectStreamer.create(initData).write(timeSeriesOutputStream, initData);
      } finally {
        if (timeSeriesOutputStream != null) {
          timeSeriesOutputStream.close();
        }
      }
      return true;
    }
  }

  /*
   * Updates internal state on completion of the initialisation task. TODO:
   * Currently continues on error - is there a better alternative?
   */
  private final class StashTimeSeriesInitHandler implements Deferrable<Boolean, StashTimeSeriesEntry<T>> {
    public StashTimeSeriesEntry<T> onCallback(Deferred<Boolean> deferred, Boolean status) {
      synchronized (StashFileTimeSeriesRecord.this) {
        currentBlockParams = new TimeSeriesBlockParams(
            new StashFileTimeSeriesBlock<T>(reactor, stashServiceBackend, StashFileTimeSeriesRecord.this, 0), 1, 0, 0);
        timeSeriesBlockMap = new TreeMap<Long, StashFileTimeSeriesBlock<T>>();
        timeSeriesBlockMap.put(Long.valueOf(0), currentBlockParams.getBlock());
        stashRecordActive = false;
        processPendingOperations();
      }
      return StashFileTimeSeriesRecord.this;
    }

    public StashTimeSeriesEntry<T> onErrback(Deferred<Boolean> deferred, Exception error) throws Exception {
      synchronized (StashFileTimeSeriesRecord.this) {
        stashRecordActive = false;
        processPendingOperations();
      }
      throw error;
    }
  }

  /*
   * Implements callback handler for selecting a single data point from the
   * navigable set provided by the time series block read method.
   */
  private final class DataPointValueSelectionHandler implements Deferrable<NavigableMap<Long, T>, T> {
    private final long timestamp;

    private DataPointValueSelectionHandler(long timestamp) {
      this.timestamp = timestamp;
    }

    public T onCallback(Deferred<NavigableMap<Long, T>> deferred, NavigableMap<Long, T> dataMap) {
      Map.Entry<Long, T> dataMapEntry = dataMap.floorEntry(timestamp);
      return dataMapEntry.getValue();
    }

    public T onErrback(Deferred<NavigableMap<Long, T>> deferred, Exception error) throws Exception {
      throw error;
    }
  }

  /*
   * Implements callback handler for selecting a subset of data points from the
   * navigable sets provided by the concurrent time series block read methods.
   */
  private final class DataPointSetSelectionHandler
      implements Deferrable<List<NavigableMap<Long, T>>, NavigableMap<Long, T>> {
    private final long startTimestamp;
    private final long endTimestamp;

    private DataPointSetSelectionHandler(long startTimestamp, long endTimestamp) {
      this.startTimestamp = startTimestamp;
      this.endTimestamp = endTimestamp;
    }

    public NavigableMap<Long, T> onCallback(Deferred<List<NavigableMap<Long, T>>> deferred,
        List<NavigableMap<Long, T>> sourceMapList) {
      TreeMap<Long, T> resultMap = new TreeMap<Long, T>();
      for (NavigableMap<Long, T> sourceMap : sourceMapList) {
        resultMap.putAll(sourceMap.subMap(startTimestamp, true, endTimestamp, true));
      }
      return resultMap;
    }

    public NavigableMap<Long, T> onErrback(Deferred<List<NavigableMap<Long, T>>> deferred, Exception error)
        throws Exception {
      throw error;
    }
  }

  /*
   * Initialises the current time series block information on the first write
   * after stash warm start.
   */
  private final class FirstWriteSetupTask implements Threadable<T, TimeSeriesBlockParams> {
    public TimeSeriesBlockParams run(T templateData) throws FileNotFoundException, IOException, ClassNotFoundException {
      TimeSeriesBlockParams finalBlockParams;

      // Identify the final block in the generated map.
      synchronized (StashFileTimeSeriesRecord.this) {
        Map.Entry<Long, StashFileTimeSeriesBlock<T>> lastEntry = timeSeriesBlockMap.lastEntry();
        finalBlockParams = new TimeSeriesBlockParams(lastEntry.getValue(), 0, lastEntry.getKey(), lastEntry.getKey());
      }

      // Scan through the data file associated with the final time series
      // block in order to determine the current block end timestamp and the
      // total number of entries.
      File timeSeriesDataDir = StashFileUtils.stashIdToPath(stashServiceBackend.getStashDataDir(), getStashId());
      File objectDataFile = new File(timeSeriesDataDir, "@" + finalBlockParams.getStartTimestamp());
      ObjectInputStream objectDataInputStream = null;

      // Attempt to read back the input data objects.
      try {
        objectDataInputStream = new ResolvedObjectInputStream(
            new BufferedInputStream(new FileInputStream(objectDataFile)), getDataClassLoader());
        StashFileObjectStreamer<T> stashObjectStreamer = StashFileObjectStreamer.create(templateData);
        while (true) {
          Long nextTimestamp = objectDataInputStream.readLong();
          T rawObjectData = stashObjectStreamer.read(objectDataInputStream);

          // Performs runtime type checking on the time series data.
          runtimeTypeCheck(rawObjectData);

          // Update current block entry count and end timestamp.
          finalBlockParams.incrementEntryCount();
          finalBlockParams.updateEndTimestamp(nextTimestamp);
        }
      }

      // Terminate at end of file condition. We assume that this is a clean
      // EOF and that no data corruption has occurred at the end of the file.
      // This has the side effect that corrupted data at the end of a file may
      // be silently discarded.
      catch (EOFException eofDetected) {
        // Exits while(true) loop.
      } finally {
        if (objectDataInputStream != null) {
          objectDataInputStream.close();
        }
      }
      return finalBlockParams;
    }
  }

  /*
   * Callback after performing block information setup on the first write after
   * stash warm start. Initiates the actual write operation.
   */
  private final class FirstWriteSetupHandler implements Deferrable<TimeSeriesBlockParams, Boolean> {
    public Boolean onCallback(Deferred<TimeSeriesBlockParams> deferred, TimeSeriesBlockParams finalBlockParams) {
      synchronized (StashFileTimeSeriesRecord.this) {
        currentBlockParams = finalBlockParams;
        for (StartupWriteQueueEntry queueEntry : startupWriteQueue) {
          doDataPointWrite(queueEntry.getTimestamp(), queueEntry.getDataValue())
              .addDeferrable(new FirstWriteCompletionHandler(queueEntry.getDeferredWrite()), true);
        }
        startupWriteQueue = null;
        stashRecordActive = false;
        processPendingOperations();
        return null;
      }
    }

    public Boolean onErrback(Deferred<TimeSeriesBlockParams> deferred, Exception error) {
      synchronized (StashFileTimeSeriesRecord.this) {
        for (StartupWriteQueueEntry queueEntry : startupWriteQueue) {
          queueEntry.getDeferredWrite().errback(error);
        }
        startupWriteQueue = null;
        stashRecordActive = false;
        processPendingOperations();
        return null;
      }
    }
  }

  /*
   * Callback after the first data point write after stash warm start. Notifies
   * write completion.
   */
  private final class FirstWriteCompletionHandler implements Deferrable<Boolean, Boolean> {
    private final Deferred<Boolean> deferredResult;

    private FirstWriteCompletionHandler(Deferred<Boolean> deferredResult) {
      this.deferredResult = deferredResult;
    }

    public Boolean onCallback(Deferred<Boolean> deferred, Boolean status) {
      deferredResult.callback(status);
      return null;
    }

    public Boolean onErrback(Deferred<Boolean> deferred, Exception error) {
      deferredResult.errback(error);
      return null;
    }
  }

  /*
   * Implements callback handling when all stash time series blocks have been
   * invalidated. Initiates time series record invalidation.
   */
  private final class BlockInvalidateHandler implements Deferrable<List<Boolean>, Boolean> {
    public Boolean onCallback(Deferred<List<Boolean>> deferred, List<Boolean> data) {
      reactor.runThread(new StashTimeSeriesInvalidateTask(), true).addDeferrable(new StashTimeSeriesInvalidateHandler(),
          true);
      return null;
    }

    public Boolean onErrback(Deferred<List<Boolean>> deferred, Exception error) throws Exception {
      deferredInvalidate.errback(error);
      return null;
    }
  }

  /*
   * Performs invalidation of time series record files.
   */
  private final class StashTimeSeriesInvalidateTask implements Threadable<Boolean, Boolean> {
    public Boolean run(Boolean status) {

      // Rename the stash index file to mark it as invalidated.
      File stashIndexDir = stashServiceBackend.getStashIndexDir();
      File metadataFile = new File(stashIndexDir, getStashId());
      File invalidatedFile = new File(stashIndexDir, "~" + getStashId());
      invalidatedFile.delete();
      metadataFile.renameTo(invalidatedFile);

      // Perform common cleanup of the data subdirectories.
      stashServiceBackend.cleanupInvalidatedEntry(getStashId(), invalidatedFile);
      return status;
    }
  }

  /*
   * Implements completion handling for stash invalidation.
   */
  private final class StashTimeSeriesInvalidateHandler implements Deferrable<Boolean, Boolean> {
    public Boolean onCallback(Deferred<Boolean> deferred, Boolean status) {
      deferredInvalidate.callback(status);
      return null;
    }

    public Boolean onErrback(Deferred<Boolean> deferred, Exception error) {
      deferredInvalidate.errback(error);
      return null;
    }
  }

  /*
   * Data class which is used to encapsulate the information associated with a
   * single startup write transaction.
   */
  private final class StartupWriteQueueEntry {
    private final Deferred<Boolean> deferredWrite;
    private final long timestamp;
    private final T dataValue;

    private StartupWriteQueueEntry(Deferred<Boolean> deferredWrite, long timestamp, T dataValue) {
      this.deferredWrite = deferredWrite;
      this.timestamp = timestamp;
      this.dataValue = dataValue;
    }

    private Deferred<Boolean> getDeferredWrite() {
      return deferredWrite;
    }

    private long getTimestamp() {
      return timestamp;
    }

    private T getDataValue() {
      return dataValue;
    }
  }

  /*
   * Data class which is used to encapsulate derived parameters associated with a
   * given time series data block.
   */
  private final class TimeSeriesBlockParams {
    private final StashFileTimeSeriesBlock<T> block;
    private long entryCount;
    private final long startTimestamp;
    private long endTimestamp;

    private TimeSeriesBlockParams(StashFileTimeSeriesBlock<T> block, long entryCount, long startTimestamp,
        long endTimestamp) {
      this.block = block;
      this.entryCount = entryCount;
      this.startTimestamp = startTimestamp;
      this.endTimestamp = endTimestamp;
    }

    private void incrementEntryCount() {
      entryCount += 1;
    }

    private void updateEndTimestamp(long updatedTimestamp) {
      if (updatedTimestamp > endTimestamp) {
        endTimestamp = updatedTimestamp;
      }
    }

    private StashFileTimeSeriesBlock<T> getBlock() {
      return block;
    }

    private long getEntryCount() {
      return entryCount;
    }

    private long getStartTimestamp() {
      return startTimestamp;
    }

    private long getEndTimestamp() {
      return endTimestamp;
    }
  }
}
