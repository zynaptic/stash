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
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintStream;
import java.io.Serializable;
import java.util.logging.Level;

import com.zynaptic.reaction.Deferrable;
import com.zynaptic.reaction.Deferred;
import com.zynaptic.reaction.DeferredSplitter;
import com.zynaptic.reaction.Reactor;
import com.zynaptic.reaction.Threadable;
import com.zynaptic.stash.StashObjectEntry;
import com.zynaptic.stash.core.StashEntryRecord;
import com.zynaptic.stash.core.StashObjectMetadata;

/**
 * Provides a stash object record implementation for use with the simple
 * filesystem stash backend.
 * 
 * @param <T> This is the generic type specifier which determines the class of
 *   objects which may be stored in the stash via a given stash object record.
 * 
 * @author Chris Holgate
 */
public final class StashFileObjectRecord<T extends Serializable> extends StashEntryRecord<T>
    implements StashObjectEntry<T> {

  // Handle on the reactor component for event management.
  private final Reactor reactor;

  // Local handle on the stash service backend.
  private final StashFileServiceBackend stashServiceBackend;

  // Specify the current stash record activity state.
  private boolean stashRecordActive = false;

  // File version counter. This is a signed long integer which is set to zero on
  // initialisation and incremented for each new write operation. This places an
  // implicit limit on the number of updates to each stash object value of 2^63
  // consecutive writes.
  private long currentVersion = 0;

  // Deferred splitter used for notifying completion of read operations.
  private DeferredSplitter<T> deferredRead = null;

  // Deferred splitter used for notifying completion of write operations.
  private DeferredSplitter<Boolean> deferredWrite = null;

  // Deferred event object used to notify completion of invalidation process.
  private Deferred<Boolean> deferredInvalidate = null;

  // Temporary handle on pending write data.
  private T pendingWriteData = null;

  /**
   * Provides static creator method which is used to initialise a new stash object
   * record.
   * 
   * @param reactor This is a handle on the reactor component which is used for
   *   event management.
   * @param stashServiceBackend This is a handle on the main stash service backend
   *   object.
   * @param stashEntryMetadata This is the stash entry metadata which is used to
   *   generate the stash object record.
   * @param initValue This is the initial value which is to be assigned to the
   *   stash object record.
   * @return Returns a deferred event object which will have its callbacks
   *   executed on completion, passing a reference to the newly created stash
   *   object record.
   */
  static <U extends Serializable> Deferred<StashObjectEntry<U>> initRecord(Reactor reactor,
      StashFileServiceBackend stashServiceBackend, StashObjectMetadata<U> stashEntryMetadata, U initValue) {
    StashFileObjectRecord<U> stashObjectRecord = new StashFileObjectRecord<U>(reactor, stashServiceBackend,
        stashEntryMetadata);
    return stashObjectRecord.initThisRecord(initValue);
  }

  /**
   * Provides static creator method which is used to rebuild a stash object record
   * from the supplied stash entry metadata. Note that this method can block since
   * it implements file system operations.
   * 
   * @param reactor This is a handle on the reactor component which is used for
   *   event management.
   * @param stashServiceBackend This is a handle on the main stash service backend
   *   object.
   * @param stashEntryMetadata This is the stash entry metadata which is used to
   *   generate the stash object record.
   * @return Returns the stash object record which was generated using the
   *   supplied metadata.
   * @throws FileNotFoundException This exception will be thrown if no stash data
   *   files could be found.
   */
  static <U extends Serializable> StashFileObjectRecord<U> rebuildRecord(Reactor reactor,
      StashFileServiceBackend stashServiceBackend, StashObjectMetadata<U> stashEntryMetadata)
      throws FileNotFoundException {
    StashFileObjectRecord<U> stashObjectRecord = new StashFileObjectRecord<U>(reactor, stashServiceBackend,
        stashEntryMetadata);
    stashObjectRecord.rebuildThisRecord();
    return stashObjectRecord;
  }

  /*
   * Provides private constructor which is used to create a new stash object
   * record from a given set of stash entry metadata.
   */
  private StashFileObjectRecord(Reactor reactor, StashFileServiceBackend stashServiceBackend,
      StashObjectMetadata<T> stashEntryMetadata) {
    super(stashEntryMetadata);
    this.reactor = reactor;
    this.stashServiceBackend = stashServiceBackend;
  }

  /*
   * Initialises the stash object record when it is originally registered.
   */
  private synchronized Deferred<StashObjectEntry<T>> initThisRecord(T initValue) {
    stashRecordActive = true;
    try {
      setDataClassLoader(initValue.getClass().getClassLoader());
    } catch (ClassNotFoundException error) {
      throw new RuntimeException("Initial stash data value should know its own class loader!", error);
    }
    return reactor.runThread(new StashObjectInitTask(), initValue).addDeferrable(new StashObjectInitHandler());
  }

  /*
   * Sets up the stash object record on a warm start.
   */
  private synchronized void rebuildThisRecord() throws FileNotFoundException {

    // Determine the most recent version number by examining the data directory.
    File objectDataDir = StashFileUtils.stashIdToPath(stashServiceBackend.getStashDataDir(), getStashId());
    String[] dataVersions = objectDataDir.list();

    // A null file list implies that the data directory is not present.
    if (dataVersions == null) {
      throw new FileNotFoundException("No object data directory found for stash entry " + getStashId());
    }

    // No ordering is assumed for the directory file list, so we just do a
    // search for the highest version number present.
    boolean dataFileFound = false;
    for (int i = 0; i < dataVersions.length; i++) {
      if (dataVersions[i].charAt(0) == '#') {
        try {
          long thisVersionNumber = Long.parseLong(dataVersions[i].substring(1));
          if (thisVersionNumber >= currentVersion) {
            currentVersion = thisVersionNumber;
            dataFileFound = true;
          }
        } catch (NumberFormatException error) {
          // Ignore files with invalid number formats.
        }
      }
    }

    // There should be at least one data file present in the data directory.
    if (!dataFileFound) {
      throw new FileNotFoundException("No object data files found for stash entry " + getStashId());
    }
  }

  /*
   * Implements StashObjectEntry.getStashId()
   */
  public String getStashId() {
    return getMetadata().getStashId();
  }

  /*
   * Implements StashObjectEntry.getStashName()
   */
  public String getStashName() {
    return getMetadata().getStashName();
  }

  /*
   * Implements StashObjectEntry.getDefaultValue()
   */
  public T getDefaultValue() {
    return getDefaultData();
  }

  /*
   * Implements StashObjectEntry.getCurrentValue()
   */
  public synchronized Deferred<T> getCurrentValue() {
    Deferred<T> deferredValue;

    // Trap requests for invalidated stash IDs.
    if (deferredInvalidate != null) {
      throw new IllegalStateException("Attempted to access a stash entry after it has been invalidated.");
    }

    // Queue up a new read request and process pending operations if required.
    if (deferredRead == null) {
      deferredRead = reactor.newDeferredSplitter();
      deferredValue = deferredRead.getOutputDeferred();
      processPendingOperations();
    }

    // Merge with outstanding read requests.
    else {
      deferredValue = deferredRead.getOutputDeferred();
    }
    return deferredValue;
  }

  /*
   * Implements StashObjectEntry.setCurrentValue(...)
   */
  public synchronized Deferred<Boolean> setCurrentValue(T newValue) {
    Deferred<Boolean> deferredStatus;

    // Trap requests for invalidated stash IDs.
    if (deferredInvalidate != null) {
      throw new IllegalStateException("Attempted to access a stash entry after it has been invalidated.");
    }

    // Update the pending data value to the most recent write data value.
    pendingWriteData = newValue;

    // Queue up a new write request and initiate processing of pending
    // operations.
    if (deferredWrite == null) {
      deferredWrite = reactor.newDeferredSplitter();
      deferredStatus = deferredWrite.getOutputDeferred();
      processPendingOperations();
    }

    // Merge with outstanding write requests.
    else {
      deferredStatus = deferredWrite.getOutputDeferred();
    }
    return deferredStatus;
  }

  /*
   * Implements StashEntryRecord.invalidate()
   */
  @Override
  protected synchronized Deferred<Boolean> invalidate() {

    // Trap requests for invalidated stash IDs.
    if (deferredInvalidate != null) {
      throw new IllegalStateException("Attempted to access a stash entry after it has been invalidated.");
    }

    // Queue up invalidation request.
    deferredInvalidate = reactor.newDeferred();
    processPendingOperations();
    return deferredInvalidate;
  }

  /*
   * Implements StashEntryRecord.reportRecordContents()
   */
  @Override
  protected synchronized void reportRecordContents(PrintStream printStream) throws IOException {

    // Add common information to the stash entry report.
    printStream.println("Stash ID : " + getStashId());
    printStream.println("  Stash name    : " + getStashName());
    printStream.println("  Entry format  : Java Object Data");
    Object defaultData = getDefaultData();
    printStream.println(
        "  Data type     : " + ((defaultData == null) ? "UNKNOWN" : defaultData.getClass().getCanonicalName()));
    printStream.println("  Data version  : " + currentVersion);
  }

  /*
   * Processes pending operations on task completion. Should be run with
   * synchronization lock held.
   */
  private void processPendingOperations() {
    if (stashRecordActive == false) {

      // Process outstanding write operation.
      if (deferredWrite != null) {
        stashRecordActive = true;
        deferredWrite.addInputDeferred(reactor.runThread(new StashObjectWriteTask(), pendingWriteData)
            .addDeferrable(new StashObjectWriteHandler()));
        deferredWrite = null;
      }

      // Process outstanding read operation.
      else if (deferredRead != null) {
        stashRecordActive = true;
        deferredRead.addInputDeferred(
            reactor.runThread(new StashObjectReadTask(), true).addDeferrable(new StashObjectReadHandler()));
        deferredRead = null;
      }

      // Invalidate stash entry if requested.
      else if (deferredInvalidate != null) {
        stashRecordActive = true;
        reactor.runThread(new StashObjectInvalidateTask(), true).addDeferrable(new StashObjectInvalidateHandler(),
            true);
      }
    }
  }

  /*
   * Performs initialisation task on creating a new stash object record.
   */
  private final class StashObjectInitTask implements Threadable<T, Boolean> {
    public Boolean run(T initValue) throws FileNotFoundException, IOException {

      // Write the stash object metadata to the stash index.
      File metadataFile = new File(stashServiceBackend.getStashIndexDir(), getStashId());
      ObjectOutputStream metadataOutputStream = null;
      try {
        metadataOutputStream = new ObjectOutputStream(new BufferedOutputStream(new FileOutputStream(metadataFile)));
        metadataOutputStream.writeObject(getMetadata());
      } finally {
        if (metadataOutputStream != null)
          metadataOutputStream.close();
      }

      // Write the default data value to the stash data area.
      File defaultDataDir = StashFileUtils.stashIdToPath(stashServiceBackend.getStashDataDir(), getStashId());
      File defaultDataFile = new File(defaultDataDir, "#" + currentVersion);
      ObjectOutputStream defaultDataOutputStream = null;
      try {
        defaultDataDir.mkdirs();
        defaultDataOutputStream = new ObjectOutputStream(
            new BufferedOutputStream(new FileOutputStream(defaultDataFile)));
        defaultDataOutputStream.writeObject(initValue);
      } finally {
        if (defaultDataOutputStream != null)
          defaultDataOutputStream.close();
      }
      return true;
    }
  }

  /*
   * Updates internal state on completion of the initialisation task. TODO:
   * Currently continues on error - is there a better alternative?
   */
  private final class StashObjectInitHandler implements Deferrable<Boolean, StashObjectEntry<T>> {
    public StashObjectEntry<T> onCallback(Deferred<Boolean> deferred, Boolean status) {
      synchronized (StashFileObjectRecord.this) {
        stashRecordActive = false;
        processPendingOperations();
      }
      return StashFileObjectRecord.this;
    }

    public StashObjectEntry<T> onErrback(Deferred<Boolean> deferred, Exception error) throws Exception {
      synchronized (StashFileObjectRecord.this) {
        stashRecordActive = false;
        processPendingOperations();
      }
      throw error;
    }
  }

  /*
   * Performs invalidation task to delete stash entry data.
   */
  private final class StashObjectInvalidateTask implements Threadable<Boolean, Boolean> {
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
  private final class StashObjectInvalidateHandler implements Deferrable<Boolean, Boolean> {
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
   * Performs blocking writes to stash data area.
   */
  private final class StashObjectWriteTask implements Threadable<T, Boolean> {
    public Boolean run(T writeData) throws FileNotFoundException, IOException {

      // Determine data file to be used.
      File objectDataDir = StashFileUtils.stashIdToPath(stashServiceBackend.getStashDataDir(), getStashId());
      File objectDataFile = new File(objectDataDir, "#" + (currentVersion + 1));
      ObjectOutputStream objectDataOutputStream = null;

      // Attempt to write the new object version.
      try {
        objectDataOutputStream = new ObjectOutputStream(new BufferedOutputStream(new FileOutputStream(objectDataFile)));
        objectDataOutputStream.writeObject(writeData);
        currentVersion = currentVersion + 1;
      } finally {
        if (objectDataOutputStream != null)
          objectDataOutputStream.close();
      }

      // Attempt to delete the old backup file.
      File oldBackupFile = new File(objectDataDir, "#" + (currentVersion - 2));
      oldBackupFile.delete();
      return true;
    }
  }

  /*
   * Implements completion handling for writes to stash data area.
   */
  private final class StashObjectWriteHandler implements Deferrable<Boolean, Boolean> {
    public Boolean onCallback(Deferred<Boolean> deferred, Boolean status) {
      synchronized (StashFileObjectRecord.this) {
        stashRecordActive = false;
        processPendingOperations();
      }
      return status;
    }

    public Boolean onErrback(Deferred<Boolean> deferred, Exception error) throws Exception {
      synchronized (StashFileObjectRecord.this) {
        stashRecordActive = false;
        processPendingOperations();
      }
      throw error;
    }
  }

  /*
   * Performs blocking reads from stash data area.
   */
  private final class StashObjectReadTask implements Threadable<Boolean, T> {
    public T run(Boolean status) throws FileNotFoundException, ClassNotFoundException, IOException {

      // Determine data directory to be used.
      File objectDataDir = StashFileUtils.stashIdToPath(stashServiceBackend.getStashDataDir(), getStashId());
      T objectDataValue = null;

      // Attempt to read back the current version of the object data.
      try {
        objectDataValue = readObjectData(objectDataDir, currentVersion);
      }

      // Attempt to read back the backup version of the object data.
      catch (Exception error) {
        reactor.getLogger("com.zynaptic.stash").log(Level.WARNING,
            "Corrupted object data for stash ID " + getStashId() + " - reverting to backup version.");
        objectDataValue = readObjectData(objectDataDir, currentVersion - 1);
        currentVersion = currentVersion - 1;
      }
      return objectDataValue;
    }

    // Implement common object read operations.
    private T readObjectData(File objectDataDir, long version)
        throws FileNotFoundException, IOException, ClassNotFoundException {
      ObjectInputStream objectDataInputStream = null;
      Object rawObjectData = null;
      try {
        File objectDataFile = new File(objectDataDir, "#" + version);
        objectDataInputStream = new ResolvedObjectInputStream(
            new BufferedInputStream(new FileInputStream(objectDataFile)), getDataClassLoader());
        rawObjectData = objectDataInputStream.readObject();

        // Performs runtime type checking on file contents.
        if (!getDefaultData().getClass().isInstance(rawObjectData)) {
          throw new ClassCastException("Inconsistent stored object type for stash entry " + getStashId());
        }
      } finally {
        if (objectDataInputStream != null)
          objectDataInputStream.close();
      }

      // Unchecked cast is OK, since we've already done runtime type checks.
      @SuppressWarnings("unchecked")
      T objectDataValue = (T) rawObjectData;
      return objectDataValue;
    }
  }

  /*
   * Implements completion handling for reads from stash data area.
   */
  private final class StashObjectReadHandler implements Deferrable<T, T> {
    public T onCallback(Deferred<T> deferred, T data) {
      synchronized (StashFileObjectRecord.this) {
        stashRecordActive = false;
        processPendingOperations();
      }
      return data;
    }

    public T onErrback(Deferred<T> deferred, Exception error) throws Exception {
      synchronized (StashFileObjectRecord.this) {
        stashRecordActive = false;
        processPendingOperations();
      }
      throw error;
    }
  }
}
