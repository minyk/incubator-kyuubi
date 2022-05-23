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

package org.apache.kyuubi.plugin.spark.authz

object OperationType extends Enumeration {

  type OperationType = Value

  val ALTERDATABASE, ALTERDATABASE_LOCATION, ALTERTABLE_ADDCOLS, ALTERTABLE_ADDPARTS,
      ALTERTABLE_RENAMECOL, ALTERTABLE_REPLACECOLS, ALTERTABLE_DROPPARTS, ALTERTABLE_RENAMEPART,
      ALTERTABLE_RENAME, ALTERTABLE_PROPERTIES, ALTERTABLE_SERDEPROPERTIES, ALTERTABLE_LOCATION,
      ALTERVIEW_AS, ALTERVIEW_RENAME, ANALYZE_TABLE, CREATEDATABASE, CREATETABLE,
      CREATETABLE_AS_SELECT, CREATEFUNCTION, CREATEVIEW, DESCDATABASE, DESCFUNCTION, DESCTABLE,
      DROPDATABASE, DROPFUNCTION, DROPTABLE, DROPVIEW, EXPLAIN, LOAD, MSCK, QUERY, RELOADFUNCTION,
      SHOWCONF, SHOW_CREATETABLE, SHOWCOLUMNS, SHOWDATABASES, SHOWFUNCTIONS, SHOWPARTITIONS,
      SHOWTABLES, SHOW_TBLPROPERTIES, SWITCHDATABASE, TRUNCATETABLE = Value

  /**
   * Mapping Spark plan's nodeName to operation type
   * @param clzName nodeName
   * @return
   */
  def apply(clzName: String): OperationType = {
    clzName match {
      case "AddArchivesCommand" => EXPLAIN
      case "AddFilesCommand" => EXPLAIN
      case "AddJarsCommand" => EXPLAIN
      case "AddPartitions" => ALTERTABLE_ADDPARTS
      case "AlterColumn" => ALTERTABLE_REPLACECOLS
      case "AlterDatabasePropertiesCommand" |
          "SetNamespaceProperties" => ALTERDATABASE
      case "AlterDatabaseSetLocationCommand" |
          "SetNamespaceLocation" => ALTERDATABASE_LOCATION
      case "AlterTableAddColumnsCommand" |
          "AlterHoodieTableAddColumnsCommand" |
          "AddColumns" |
          "DropColumns" => ALTERTABLE_ADDCOLS
      case "AlterTableAddPartitionCommand" => ALTERTABLE_ADDPARTS
      case "AlterTableChangeColumnCommand" |
          "ReplaceColumns" => ALTERTABLE_REPLACECOLS
      case "AlterTableDropPartitionCommand" | "DropPartitions" => ALTERTABLE_DROPPARTS
      case "AlterTableRenameCommand" |
          "RenameTable" => ALTERTABLE_RENAME
      case "AlterTableRecoverPartitionsCommand" |
          "RecoverPartitions" |
          "RepairTableCommand" |
          "RepairTable" => MSCK
      case "AlterTableRenamePartitionCommand" | "RenamePartitions" => ALTERTABLE_RENAMEPART
      case "AlterTableSerDePropertiesCommand" |
          "SetTableSerDeProperties" => ALTERTABLE_SERDEPROPERTIES
      case "AlterTableSetLocationCommand" |
          "SetTableLocation" => ALTERTABLE_LOCATION
      case "AlterTableSetPropertiesCommand" |
          "AlterTableUnsetPropertiesCommand" |
          "SetTableProperties" |
          "UnsetTableProperties" |
          "SetViewProperties" |
          "UnsetViewProperties" => ALTERTABLE_PROPERTIES
      case ava if ava.contains("AlterViewAs") => ALTERVIEW_AS
      case ac if ac.startsWith("Analyze") => ANALYZE_TABLE
      case "AppendData" => QUERY
      case "CreateDatabaseCommand" | "CreateNamespace" => CREATEDATABASE
      case "CreateFunctionCommand" | "CreateFunction" => CREATEFUNCTION
      case "CreateTableAsSelect" |
          "CreateDataSourceTableAsSelectCommand" |
          "CreateHiveTableAsSelectCommand" |
          "OptimizedCreateHiveTableAsSelectCommand" |
          "ReplaceTableAsSelect" => CREATETABLE_AS_SELECT
      case "CreateTableCommand" |
          "CreateDataSourceTableCommand" |
          "CreateTableLikeCommand" |
          "CreateV2Table" |
          "ReplaceTable" => CREATETABLE
      case "CreateViewCommand" |
          "CacheTableCommand" |
          "CreateTempViewUsing" |
          "CacheTable" |
          "CacheTableAsSelect" => CREATEVIEW
      case "DescribeDatabaseCommand" | "DescribeNamespace" => DESCDATABASE
      case "DescribeFunctionCommand" | "DescribeFunction" => DESCFUNCTION
      case "DescribeColumnCommand" | "DescribeTableCommand" => DESCTABLE
      case "DropDatabaseCommand" | "DropNamespace" => DROPDATABASE
      case "DropFunctionCommand" | "DropFunction" => DROPFUNCTION
      case "DropTableCommand" |
          "DropTable" => DROPTABLE
      case "DropView" => DROPVIEW
      case "ExplainCommand" => EXPLAIN
      case "InsertIntoDataSourceCommand" |
          "InsertIntoDataSourceDirCommand" |
          "InsertIntoHiveTable" |
          "InsertIntoHiveDirCommand" |
          "SaveIntoDataSourceCommand" => QUERY
      case "LoadDataCommand" => LOAD
      case "SetCommand" => SHOWCONF
      case "RefreshFunctionCommand" | "RefreshFunction" => RELOADFUNCTION
      case "RefreshTableCommand" | "RefreshTable" => QUERY
      case "RenameColumn" => ALTERTABLE_RENAMECOL
      case "SetCatalogCommand" |
          "SetCatalogAndNamespace" |
          "SetNamespaceCommand" |
          "SetDatabaseCommand" => SWITCHDATABASE
      case "ShowCatalogsCommand" |
          "ShowCurrentNamespaceCommand" => SHOWDATABASES
      case "ShowTablesCommand" |
          "ShowViewsCommand" |
          "ShowTables" |
          "ShowTableExtended" |
          "ShowViews" => SHOWTABLES
      case "ShowColumnsCommand" | "ShowColumns" => SHOWCOLUMNS
      case "ShowCreateTableAsSerdeCommand" |
          "ShowCreateTableCommand" |
          "ShowCreateTable" => SHOW_CREATETABLE
      case "ShowFunctionsCommand" | "ShowFunctions" => SHOWFUNCTIONS
      case "ShowPartitionsCommand" | "ShowPartitions" => SHOWPARTITIONS
      case "ShowTablePropertiesCommand" |
          "ShowTableProperties" => SHOW_TBLPROPERTIES
      case "TruncateTableCommand" |
          "TruncateTable" |
          "TruncatePartition" => TRUNCATETABLE
      case "UncacheTableCommand" => DROPVIEW
      case _ => QUERY
    }
  }

}
