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

import java.io.File;

/**
 * This class provides a range of utility functions which are used by the stash
 * file based backend service.
 * 
 * @author Chris Holgate
 */
public final class StashFileUtils {

  /**
   * Convert a unique stash identifier to a directory path within the stash. This
   * converts the dot separators of the stash identifier string to directory
   * seperators for the directory path.
   * 
   * @param stashDataDir This is a file object which references the root data
   *   directory of the stash.
   * @param stashId This is the unique stash identifier string.
   * @return Returns a directory path for the resource associated with the stash
   *   identifier, specified as a file object.
   */
  static File stashIdToPath(File stashDataDir, String stashId) {
    String fileSeparator = File.separator;
    String subPath;

    // If the file separator is a single character we can do a simple search and
    // replace on the stash identifier string.
    if (fileSeparator.length() == 1) {
      char fileSeparatorChar = fileSeparator.charAt(0);
      char[] stashIdChars = stashId.toCharArray();
      for (int i = 0; i < stashIdChars.length; i++) {
        if (stashIdChars[i] == '.') {
          stashIdChars[i] = fileSeparatorChar;
        }
      }
      subPath = new String(stashIdChars);
    }

    // Multi-character file separators are not currently supported.
    else {
      throw new RuntimeException("Multiple character directory path separators are not currently supported.");
    }
    return new File(stashDataDir, subPath);
  }

  /**
   * Clean the data directory for a given stash ID and remove all empty parent
   * directories.
   * 
   * @param stashDataDir This is a file object which references the root data
   *   directory of the stash.
   * @param stashId This is the unique stash identifier string.
   */
  static void cleanupDataFiles(File stashDataDir, String stashId) {
    File removedDataDir = stashIdToPath(stashDataDir, stashId);
    String removedFileNames[] = removedDataDir.list();

    // Clear out unused data files.
    if (removedFileNames != null) {
      for (int i = 0; i < removedFileNames.length; i++) {
        char fileNamePrefix = removedFileNames[i].charAt(0);
        if ((fileNamePrefix == '#') || (fileNamePrefix == '@')) {
          File removedFile = new File(removedDataDir, removedFileNames[i]);
          removedFile.delete();
        }
      }

      // Move up the directory tree deleting empty directories.
      String[] remainingFileList = removedDataDir.list();
      while ((remainingFileList != null) && (remainingFileList.length == 0) && (!removedDataDir.equals(stashDataDir))) {

        // Wait for delete operation to complete before attempting to read back
        // the remaining contents of the parent directory.
        while (removedDataDir.exists()) {
          removedDataDir.delete();
          Thread.yield();
        }
        removedDataDir = removedDataDir.getParentFile();
        remainingFileList = removedDataDir.list();
      }
    }
  }
}
