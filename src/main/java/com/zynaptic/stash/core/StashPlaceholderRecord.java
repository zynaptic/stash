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

package com.zynaptic.stash.core;

import java.io.IOException;
import java.io.PrintStream;

import com.zynaptic.reaction.Deferred;

/**
 * Provides a placeholder stash record entry for use while setting up new stash
 * entries.
 * 
 * @author Chris Holgate
 * 
 */
public final class StashPlaceholderRecord extends StashEntryRecord<Boolean> {

  // Define the common metadata object which is used for all placeholder
  // records.
  private static final StashObjectMetadata<Boolean> PLACEHOLDER_METADATA = new StashObjectMetadata<Boolean>(
      StashObjectMetadata.RecordFormat.PLACEHOLDER, null, "PLACEHOLDER", false);

  /**
   * Provides default constructor which assigns the common placeholder metadata to
   * the superclass.
   */
  protected StashPlaceholderRecord() {
    super(PLACEHOLDER_METADATA);
  }

  /*
   * Implements StashEntryRecord.invalidate()
   */
  @Override
  public Deferred<Boolean> invalidate() {
    return null;
  }

  /*
   * Implements StashEntryRecord.reportRecordContents(...)
   */
  @Override
  public void reportRecordContents(PrintStream printStream) throws IOException {
    // No record state to report.
  }
}
