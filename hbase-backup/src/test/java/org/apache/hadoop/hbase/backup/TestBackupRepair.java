/*
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
package org.apache.hadoop.hbase.backup;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.Set;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.backup.impl.BackupSystemTable;
import org.apache.hadoop.hbase.backup.impl.TableBackupClient;
import org.apache.hadoop.hbase.backup.impl.TableBackupClient.Stage;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.util.ToolRunner;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hbase.util.StringUtils;

@Category(LargeTests.class)
public class TestBackupRepair extends TestBackupBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestBackupRepair.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestBackupRepair.class);

  @Test
  public void testFullBackupWithFailuresAndRestore() throws Exception {

    autoRestoreOnFailure = false;

    conf1.set(TableBackupClient.BACKUP_CLIENT_IMPL_CLASS,
      FullTableBackupClientForTest.class.getName());
    int maxStage = Stage.values().length - 1;
    // Fail stage in loop between 0 and 4 inclusive
    for (int stage = 0; stage < maxStage; stage++) {
      LOG.info("Running stage " + stage);
      runBackupAndFailAtStageWithRestore(stage);
    }
  }

  public void runBackupAndFailAtStageWithRestore(int stage) throws Exception {

    conf1.setInt(FullTableBackupClientForTest.BACKUP_TEST_MODE_STAGE, stage);
    try (BackupSystemTable table = new BackupSystemTable(TEST_UTIL.getConnection())) {
      int before = table.getBackupHistory().size();
      String[] args = new String[] { "create", "full", BACKUP_ROOT_DIR, "-t",
        table1.getNameAsString() + "," + table2.getNameAsString() };
      // Run backup
      int ret = ToolRunner.run(conf1, new BackupDriver(), args);
      assertFalse(ret == 0);

      // Now run restore
      args = new String[] { "repair" };

      ret = ToolRunner.run(conf1, new BackupDriver(), args);
      assertTrue(ret == 0);

      List<BackupInfo> backups = table.getBackupHistory();
      int after = table.getBackupHistory().size();

      assertTrue(after == before + 1);
      for (BackupInfo data : backups) {
        String backupId = data.getBackupId();
        assertFalse(checkSucceeded(backupId));
      }
      Set<TableName> tables = table.getIncrementalBackupTableSet(BACKUP_ROOT_DIR);
      assertTrue(tables.size() == 0);
    }
  }

  @Test
  public void testRepairSnapshotHandlingDuringDelete() throws Exception {
    // Create test tables and backup data
    TableName[] tables = new TableName[] { table1, table2 };
    String backupId = backupTables(tables);
    
    try (BackupSystemTable systemTable = new BackupSystemTable(TEST_UTIL.getConnection())) {
      // Simulate failed delete operation
      systemTable.startBackupExclusiveOperation(backupId);
      BackupSystemTable.createSnapshot(TEST_UTIL.getConnection());
      
      // Run repair
      String[] args = new String[] { "repair" };
      int ret = ToolRunner.run(conf1, new BackupDriver(), args);
      assertTrue(ret == 0);
      
      // Verify snapshot was not deleted after delete repair
      assertTrue("Snapshot should exist after delete repair",
        BackupSystemTable.snapshotExists(TEST_UTIL.getConnection()));
      
      // Verify backup system table is consistent
      assertFalse("Backup exclusive operation should be finished",
        systemTable.hasOngoingExclusiveOperation());
    }
  }

  @Test
  public void testRepairSnapshotHandlingDuringMerge() throws Exception {
    // Create test tables and backup data
    TableName[] tables = new TableName[] { table1, table2 };
    String backupId1 = backupTables(tables);
    String backupId2 = backupTables(tables);
    
    try (BackupSystemTable systemTable = new BackupSystemTable(TEST_UTIL.getConnection())) {
      // Simulate failed merge operation
      systemTable.startBackupExclusiveOperation(backupId1 + "," + backupId2);
      BackupSystemTable.createSnapshot(TEST_UTIL.getConnection());
      systemTable.startMergeOperation(new String[] { backupId1, backupId2 });
      
      // Run repair
      String[] args = new String[] { "repair" };
      int ret = ToolRunner.run(conf1, new BackupDriver(), args);
      assertTrue(ret == 0);
      
      // Verify snapshot was deleted after merge repair
      assertFalse("Snapshot should be deleted after merge repair",
        BackupSystemTable.snapshotExists(TEST_UTIL.getConnection()));
      
      // Verify backup system table is consistent
      assertFalse("Backup exclusive operation should be finished",
        systemTable.hasOngoingExclusiveOperation());
      assertFalse("Merge operation should be finished",
        systemTable.hasOngoingMergeOperation());
    }
  }

  @Test
  public void testRepairSnapshotHandlingWithNoFailures() throws Exception {
    try (BackupSystemTable systemTable = new BackupSystemTable(TEST_UTIL.getConnection())) {
      // Create snapshot
      BackupSystemTable.createSnapshot(TEST_UTIL.getConnection());
      
      // Run repair with no failures
      String[] args = new String[] { "repair" };
      int ret = ToolRunner.run(conf1, new BackupDriver(), args);
      assertTrue(ret == 0);
      
      // Verify snapshot was deleted when no repairs needed
      assertFalse("Snapshot should be deleted when no repairs needed",
        BackupSystemTable.snapshotExists(TEST_UTIL.getConnection()));
    }
  }

  // Helper method to create backup
  private String backupTables(TableName[] tables) throws Exception {
    String[] args = new String[] { "create", "full", BACKUP_ROOT_DIR, "-t",
      StringUtils.join(tables, ",") };
    int ret = ToolRunner.run(conf1, new BackupDriver(), args);
    assertTrue(ret == 0);
    
    try (BackupSystemTable systemTable = new BackupSystemTable(TEST_UTIL.getConnection())) {
      List<BackupInfo> backups = systemTable.getBackupHistory();
      return backups.get(backups.size() - 1).getBackupId();
    }
  }

}
