CREATE OR REPLACE PROCEDURE DEV_PS_RAW_DB.CORE_REFERENCE.COPY_FROM_S3_TO_RAW_VALIDAITON("CLIENT_NAME" VARCHAR(20), "PROCESS_NAME" VARCHAR(20), "FILE_NAME" VARCHAR(255) DEFAULT null)
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS 
$$
var client_name = CLIENT_NAME;
var process_name = PROCESS_NAME;
var file_name = FILE_NAME;
var success_rows = [];
var failure_rows = [];
var email_sent = false;
var no_files_found = false;
var foundRecords = false;
var last_executed_query = "";
var last_checkpoint = "";
var failure_reason = "";

function sendEmail(subject, body, contentType) {
  try {
    subject = subject || 'No Subject';
    body = body || 'No Body';
    contentType = contentType || 'text/plain';
    var email_sql = "CALL SYSTEM$SEND_EMAIL('EMAIL_INT_TESTT', 'abc@xyz.com,EDAAdmins@xyz.com', ?, ?, ?)";
    var stmt = snowflake.createStatement({
      sqlText: email_sql,
      binds: [subject, body, contentType]
    });
    stmt.execute();
    email_sent = true;
  } catch (email_err) {
  }
}

function processFiles() {
  try {
    last_checkpoint = "Starting processFiles function";

    var config_query = "SELECT * FROM DEV_PS_RAW_DB.CORE_REFERENCE.LOAD_CONFIG_TABLE WHERE CLIENT_NAME = ? AND PROCESS_NAME = ? AND ACTIVE_FLAG = 'TRUE'";
    last_executed_query = config_query + " [Binds: " + client_name + ", " + process_name + "]";
    last_checkpoint = "Executing config query";
    
    var config_stmt = snowflake.createStatement({sqlText: config_query, binds: [client_name, process_name]});
    var config_result = config_stmt.execute();
        
    while (config_result.next()) {
      foundRecords = true;
      last_checkpoint = "Found config record, processing configuration";
      
      var start_time = new Date();
      
      var storage_integration = config_result.getColumnValue(3);
      var stage_name = config_result.getColumnValue(4);
      var s3_location = config_result.getColumnValue(5);
      var s3_location_archive = config_result.getColumnValue(6);
      var file_name_pattern = config_result.getColumnValue(7);
      var raw_table_name = config_result.getColumnValue(8);
      var exclude_columns = config_result.getColumnValue(9);
      var file_format = config_result.getColumnValue(10);
      var is_archive = config_result.getColumnValue(11);
      var log_table = config_result.getColumnValue(12);
      var truncate_flag = config_result.getColumnValue(13);
      var infer_schema_file_format = config_result.getColumnValue(16);
      var s3_location_failfile = config_result.getColumnValue(17);
      var check_header = config_result.getColumnValue(18);
      var s3foldername = config_result.getColumnValue(19);
      var loadmetadata = config_result.getColumnValue(20);
      var check_datatype = config_result.getColumnValue(21);
      
      last_checkpoint = "Retrieved config values - stage=" + stage_name + ", table=" + raw_table_name;
      
      var getFolderPath = s3_location.indexOf(s3foldername) + s3foldername.length;
      var folderPath = s3_location.substring(getFolderPath);
      var getFailFileFolderPath = s3_location_failfile.indexOf(s3foldername) + s3foldername.length;
      var FailFileFolderPath = s3_location_failfile.substring(getFailFileFolderPath);
     
      var tableParts = raw_table_name.split('.');
      var db = tableParts[0];
      var schema = tableParts[1];
      var table = tableParts[2];
      var exclude_columns_array = exclude_columns.split(',');
      var formatted_exclude_columns = exclude_columns_array.map(function(col) { return "'" + col.trim() + "'"; }).join(',');
      
      var tableNameParts = raw_table_name.split('.');
      var tableName = tableNameParts[2];
      var schemaName = tableNameParts[1];
      var databaseName = tableNameParts[0];
      
      var columns_query = "SELECT LISTAGG(COLUMN_NAME, ',') WITHIN GROUP (ORDER BY ORDINAL_POSITION) AS COLUMN_NAMES FROM " + databaseName + ".INFORMATION_SCHEMA.COLUMNS WHERE (COLUMN_NAME NOT IN (" + formatted_exclude_columns + ")) AND (TABLE_SCHEMA = ?) AND TABLE_NAME = ?";
      last_executed_query = columns_query + " [Binds: " + schemaName + ", " + tableName + "]";
      last_checkpoint = "About to execute columns query";
      
      
      try {
        var columns_stmt = snowflake.createStatement({ sqlText: columns_query, binds: [schemaName, tableName] });
        var columns_result = columns_stmt.execute();
        last_checkpoint = "Columns query executed successfully";
      } catch (columns_err) {
        last_checkpoint = "CRITICAL: Columns query failed";
        failure_reason = "Columns query failed: " + columns_err.message;
        var column_error = 'Failed to read table columns - Code: ' + (columns_err.code || 'N/A') + ', Message: ' + (columns_err.message || 'Unknown error') + ', Table: ' + raw_table_name;
        
        try {
          var insert_stmt = snowflake.createStatement({
            sqlText: "INSERT INTO DEV_PS_RAW_DB.CORE_REFERENCE.MANIFEST_FILE_IMPORT (CLIENT_NAME, PROCESS_NAME, RAW_TABLE_NAME, ERROR_DESCRIPTION) VALUES (?, ?, ?, ?)",
            binds: [client_name, process_name, raw_table_name, column_error]
          });
          insert_stmt.execute();
        } catch (manifest_err) {
        }
        
        failure_rows.push({
          client_name: client_name,
          process_name: process_name,
          file_name: 'N/A',
          error: column_error
        });
        continue;
      }
      
        if (columns_result.next()) {
        
          var columns_raw = columns_result.getColumnValue(1);
          // Split columns and wrap each in double quotes
          var columns_array = columns_raw.split(',');
          var quoted_columns_array = columns_array.map(function(col) { return '"' + col.trim() + '"'; });
          var columns = quoted_columns_array.join(',');
          last_checkpoint = "Retrieved columns successfully: " + columns_raw + " -> Quoted: " + columns;
        
        var getArchiveFolderPath = s3_location_archive.indexOf(s3foldername) + s3foldername.length;
        var archiveFolderPath = s3_location_archive.substring(getArchiveFolderPath);
        
        var list_files_query;
        if (file_name) {
          list_files_query = "LIST @" + stage_name + folderPath + file_name;
        } else {
          list_files_query = "LIST @" + stage_name + folderPath + " PATTERN = '" + file_name_pattern + "'";
        }
        
        last_executed_query = list_files_query;
        last_checkpoint = "About to execute file listing query";
        
        try {
          var list_files_stmt = snowflake.createStatement({ sqlText: list_files_query });
          var list_files_result = list_files_stmt.execute();
          last_checkpoint = "File listing query executed successfully";
        } catch (list_err) {
          last_checkpoint = "CRITICAL: File listing query failed";
          failure_reason = "File listing failed: " + list_err.message;
          var list_error = 'Failed to list files in stage - Code: ' + (list_err.code || 'N/A') + ', Message: ' + (list_err.message || 'Unknown error') + ', Location: ' + s3_location;
          
          try {
            var insert_stmt = snowflake.createStatement({
              sqlText: "INSERT INTO DEV_PS_RAW_DB.CORE_REFERENCE.MANIFEST_FILE_IMPORT (CLIENT_NAME, PROCESS_NAME, RAW_TABLE_NAME, ERROR_DESCRIPTION) VALUES (?, ?, ?, ?)",
              binds: [client_name, process_name, raw_table_name, list_error]
            });
            insert_stmt.execute();
          } catch (manifest_err) {
          }
          
          failure_rows.push({
            client_name: client_name,
            process_name: process_name,
            file_name: 'N/A',
            error: list_error
          });
          continue;
        }
        
        var file_exists = false;
        var file_paths = [];
        
        last_checkpoint = "Collecting file paths from listing result";
        
        while (list_files_result.next()) {
          file_exists = true;
          file_paths.push(list_files_result.getColumnValue(1));
        }
        
        last_checkpoint = "File collection completed - file_exists=" + file_exists + ", total_files=" + file_paths.length;
        
        if (file_exists && file_paths.length > 0) {
          last_checkpoint = "Starting file processing loop for " + file_paths.length + " files";
          
          for (var fileIndex = 0; fileIndex < file_paths.length; fileIndex++) {
            var currentFilePath = file_paths[fileIndex];
            var pathElements = currentFilePath.split('/');
            var processed_file_path_parts = pathElements.slice(4);
            var processedFilePath = '/' + processed_file_path_parts.join('/');
            var current_file_name = pathElements[pathElements.length - 1];
            
            last_checkpoint = "Processing file " + (fileIndex + 1) + "/" + file_paths.length + ": " + current_file_name;
            
            try {
              var infer_query = "SELECT COLUMN_NAME,SPLIT_PART(TYPE, '(', 1) AS TYPE, ORDER_ID + 1 AS ORDINAL FROM TABLE(INFER_SCHEMA(LOCATION => '@" + stage_name + processedFilePath + "', FILE_FORMAT => '" + infer_schema_file_format + "'))";
              last_executed_query = infer_query;
              last_checkpoint = "About to execute INFER_SCHEMA for file: " + current_file_name;
              
              var infer_stmt = snowflake.createStatement({
                sqlText: infer_query
              });
             
              var infer_result = infer_stmt.execute();
              last_checkpoint = "INFER_SCHEMA executed successfully for: " + current_file_name;
              
              var file_columns = [];
              var file_column_types = {};
              var file_column_ordinals = {};
              var schema_rows = 0;
              
              while (infer_result.next()) {
                schema_rows++;
                var col_name = infer_result.getColumnValue(1).toUpperCase();
                var col_type = infer_result.getColumnValue(2).toUpperCase();
                var col_ordinal = infer_result.getColumnValue(3);
                file_columns.push(col_name);
                file_column_types[col_name] = col_type;
                file_column_ordinals[col_name] = col_ordinal;
              }
              
              last_checkpoint = "Schema inference completed - found " + schema_rows + " columns: [" + file_columns.join(', ') + "]";
              
              var col_query = "SELECT COLUMN_NAME, DATA_TYPE, ORDINAL_POSITION, CHARACTER_MAXIMUM_LENGTH FROM " + db + ".INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND COLUMN_NAME NOT IN (" + formatted_exclude_columns + ") ORDER BY ORDINAL_POSITION";
              last_executed_query = col_query + " [Binds: " + schema + ", " + table + "]";
              
              var col_stmt = snowflake.createStatement({sqlText: col_query, binds: [schema, table]});
              var col_result = col_stmt.execute();
              var table_columns = [];
              var table_column_types = {};
              var table_column_ordinals = {};
              var table_column_lengths = {};
              
              while (col_result.next()) {
                var col_name = col_result.getColumnValue(1).toUpperCase();
                var col_type = col_result.getColumnValue(2).toUpperCase();
                var col_ordinal = col_result.getColumnValue(3);
                var col_max_length = col_result.getColumnValue(4);
                table_columns.push(col_name);
                table_column_types[col_name] = col_type;
                table_column_ordinals[col_name] = col_ordinal;
                table_column_lengths[col_name] = col_max_length;
              }
              
              last_checkpoint = "Table schema retrieved - columns: [" + table_columns.join(', ') + "]";
              
              // ENHANCED SCHEMA VALIDATION WITH DATATYPE FLAG
              var schema_mismatch_msgs = [];

              // Fix the check_header evaluation
              var is_header_check_enabled = false;
              if (check_header) {
                if (typeof check_header === 'boolean') {
                  is_header_check_enabled = check_header;
                } else if (typeof check_header === 'string') {
                  is_header_check_enabled = (check_header.toUpperCase() === 'TRUE');
                } else {
                  is_header_check_enabled = !!check_header;
                }
              }

              // Fix the check_datatype evaluation  
              var is_datatype_check_enabled = false;
              if (check_datatype) {
                if (typeof check_datatype === 'boolean') {
                  is_datatype_check_enabled = check_datatype;
                } else if (typeof check_datatype === 'string') {
                  is_datatype_check_enabled = (check_datatype.toUpperCase() === 'TRUE');
                } else {
                  is_datatype_check_enabled = !!check_datatype;
                }
              }


              if (is_header_check_enabled) {
                var is_match = JSON.stringify(file_columns.sort()) === JSON.stringify(table_columns.sort());
                if (!is_match) {
                  schema_mismatch_msgs.push("Column names do not match.");
                }
                
                for (var i = 0; i < table_columns.length; i++) {
                  var col = table_columns[i];
                  if (file_column_ordinals[col] !== table_column_ordinals[col]) {
                    schema_mismatch_msgs.push("Ordinal mismatch for column " + col + ": expected " + table_column_ordinals[col] + ", got " + file_column_ordinals[col]);
                  }
                }
                
                if (is_datatype_check_enabled) {
                  for (var i = 0; i < table_columns.length; i++) {
                    var col = table_columns[i];
                    if (file_column_types[col] && table_column_types[col] && file_column_types[col] !== table_column_types[col]) {
                      schema_mismatch_msgs.push("Datatype mismatch for column " + col + ": expected " + table_column_types[col] + ", got " + file_column_types[col]);
                    }
                  }
                } else {
                }
                
                for (var i = 0; i < table_columns.length; i++) {
                  var col = table_columns[i];
                  var table_col_type = table_column_types[col];
                  if (table_col_type && (table_col_type.includes('VARCHAR') || table_col_type.includes('TEXT') || table_col_type.includes('STRING'))) {
                    var table_max_length = table_column_lengths[col];
                    if (table_max_length && table_max_length > 0) {
                      var col_ordinal = file_column_ordinals[col];
                      if (col_ordinal) {
                        try {
                          var length_sql = "SELECT MAX(LENGTH($" + col_ordinal + ")) FROM @" + stage_name + processedFilePath + " (FILE_FORMAT => '" + file_format + "')";
                          var len_stmt = snowflake.createStatement({sqlText: length_sql});
                          var len_res = len_stmt.execute();
                          if (len_res.next()) {
                            var file_max_length = len_res.getColumnValue(1) || 0;
                            if (file_max_length > table_max_length) {
                              schema_mismatch_msgs.push("Data length violation in column " + col + " (" + table_col_type + "(" + table_max_length + ")): maximum data length in file is " + file_max_length + ", but column maximum is " + table_max_length + ". Data will be truncated during COPY.");
                            }
                          }
                        } catch (len_err) {
                        }
                      }
                    }
                  }
                }
              } else {
                if (file_columns.length !== table_columns.length) {
                  schema_mismatch_msgs.push("Number of columns mismatch: file has " + file_columns.length + ", table has " + table_columns.length);
                } else {
                  for (var i = 0; i < table_columns.length; i++) {
                    var table_col = table_columns[i];
                    var table_type = table_column_types[table_col];
                    var file_col = file_columns[i];
                    var file_type = file_column_types[file_col];
                    
                    if (is_datatype_check_enabled && file_type && table_type && file_type !== table_type) {
                      schema_mismatch_msgs.push("Datatype mismatch at position " + (i + 1) + ": expected " + table_type + ", got " + file_type);
                    }
                    
                    if (table_type && (table_type.includes('VARCHAR') || table_type.includes('TEXT') || table_type.includes('STRING'))) {
                      var table_max_length = table_column_lengths[table_col];
                      if (table_max_length && table_max_length > 0) {
                        var col_ordinal = file_column_ordinals[file_col];
                        if (col_ordinal) {
                          try {
                            var length_sql = "SELECT MAX(LENGTH($" + col_ordinal + ")) FROM @" + stage_name + processedFilePath + " (FILE_FORMAT => '" + file_format + "')";
                            var len_stmt = snowflake.createStatement({sqlText: length_sql});
                            var len_res = len_stmt.execute();
                            if (len_res.next()) {
                              var file_max_length = len_res.getColumnValue(1) || 0;
                              if (file_max_length > table_max_length) {
                                schema_mismatch_msgs.push("Data length violation in column " + table_col + " (" + table_type + "(" + table_max_length + ")): maximum data length in file is " + file_max_length + ", but column maximum is " + table_max_length + ". Data will be truncated during COPY.");
                              }
                            }
                          } catch (len_err) {
                          }
                        }
                      }
                    }
                  }
                  
                  if (!is_datatype_check_enabled) {
                  }
                }
              }
              
              last_checkpoint = "Schema validation completed for " + current_file_name + " - mismatch count=" + schema_mismatch_msgs.length;
              
              if (schema_mismatch_msgs.length > 0) {
                var error_text = schema_mismatch_msgs.join('\\\\n');
                
                last_checkpoint = "File " + current_file_name + " rejected due to schema mismatch";
                failure_reason = "Schema validation failed: " + error_text;
                
                try {
                  var insert_stmt = snowflake.createStatement({
                    sqlText: "INSERT INTO DEV_PS_RAW_DB.CORE_REFERENCE.MANIFEST_FILE_IMPORT (CLIENT_NAME, PROCESS_NAME, RAW_TABLE_NAME, FILE_NAME, ERROR_DESCRIPTION) VALUES (?, ?, ?, ?, ?)",
                    binds: [client_name, process_name, raw_table_name, current_file_name, error_text]
                  });
                  insert_stmt.execute();
                } catch (manifest_err) {
                }
                
                failure_rows.push({
                  client_name: client_name,
                  process_name: process_name,
                  file_name: current_file_name,
                  error: error_text
                });
                
                try {
                  var copyFailedSql = "COPY FILES INTO @" + stage_name + FailFileFolderPath + " FROM @" + stage_name + processedFilePath;
                  snowflake.createStatement({ sqlText: copyFailedSql }).execute();
                  var removeSql = "REMOVE @" + stage_name + processedFilePath;
                  snowflake.createStatement({ sqlText: removeSql }).execute();
                } catch (move_err) {
                }
                
                continue;
              }
              
              last_checkpoint = "Schema validation passed for " + current_file_name + ", proceeding with data load";
              
              var import_load_type = (truncate_flag === false) ? "Append" : "Truncate and Load";
              if (truncate_flag) {
                try {
                  var truncate_query = "TRUNCATE TABLE " + raw_table_name;
                  last_executed_query = truncate_query;
                  last_checkpoint = "Truncating table: " + raw_table_name;
                  
                  var truncate_stmt = snowflake.createStatement({ sqlText: truncate_query });
                  truncate_stmt.execute();
                  
                  last_checkpoint = "Table truncated successfully";
                } catch (truncate_err) {
                }
              }
              
              var copy_query = "COPY INTO " + raw_table_name + "(" + columns + ") FROM @" + stage_name + processedFilePath + " FILE_FORMAT = (format_name = '" + file_format + "' TRIM_SPACE = TRUE SKIP_BLANK_LINES = TRUE) FORCE=TRUE";
              last_executed_query = copy_query;
              last_checkpoint = "About to execute COPY command for " + current_file_name;
              
              var copy_stmt = snowflake.createStatement({ sqlText: copy_query });
              
              try {
                var copy_result = copy_stmt.execute();
                last_checkpoint = "COPY command executed successfully for " + current_file_name;
                
                if (copy_result.next()) {
                  var columnCount = copy_result.getColumnCount();
                  
                  if (columnCount > 2) {
                    var processed_file_name = copy_result.getColumnValue(1);
                    var rows_loaded = copy_result.getColumnValue(4);
                    var processed_file_parts = processed_file_name.split('/');
                    var final_file_name = processed_file_parts[processed_file_parts.length - 1];
                    
                    last_checkpoint = "Successfully loaded " + rows_loaded + " rows from " + final_file_name;
                    
                    if (loadmetadata === true) {
                      try {
                        var updateTableStatement = "UPDATE " + raw_table_name + " SET LOADED_FILE_NAME = ? WHERE LOADED_FILE_NAME IS NULL";
                        snowflake.createStatement({ sqlText: updateTableStatement, binds: [final_file_name] }).execute();
                        var updateTableStatement2 = "UPDATE " + raw_table_name + " SET LOADED_TIMESTAMP = CURRENT_TIMESTAMP() WHERE LOADED_TIMESTAMP IS NULL";
                        snowflake.createStatement({ sqlText: updateTableStatement2 }).execute();
                      } catch (metadata_err) {
                      }
                    }
                    
                    try {
                      var insert_audit = "INSERT INTO DEV_PS_RAW_DB.CORE_REFERENCE.MANIFEST_FILE_IMPORT (CLIENT_NAME, PROCESS_NAME, RAW_TABLE_NAME, FILE_NAME, RECORD_COUNT, IMPORT_LOAD_TYPE) VALUES('" + client_name + "', '" + process_name + "', '" + raw_table_name + "', '" + final_file_name + "', '" + rows_loaded + "', '" + import_load_type + "')";
                      snowflake.createStatement({ sqlText: insert_audit }).execute();
                    } catch (audit_err) {
                    }
                    
                    try {
                      var copyArchiveSql = "COPY FILES INTO @" + stage_name + archiveFolderPath + " FROM @" + stage_name + processedFilePath;
                      snowflake.createStatement({ sqlText: copyArchiveSql }).execute();
                      var removeSql = "REMOVE @" + stage_name + processedFilePath;
                      snowflake.createStatement({ sqlText: removeSql }).execute();
                    } catch (archive_err) {
                    }
                    
                    var end_time = new Date();
                    
                    success_rows.push({
                      client_name: client_name,
                      process_name: process_name,
                      file_name: final_file_name,
                      table: raw_table_name,
                      start_time: start_time,
                      end_time: end_time,
                      row_count: rows_loaded
                    });
                    last_checkpoint = "File processing completed successfully for: " + final_file_name;
                 } else {
                   last_checkpoint = "COPY result has unexpected column count: " + columnCount;
                   failure_reason = "COPY result has unexpected column count: " + columnCount;
                 }
               } else {
                 last_checkpoint = "COPY result has no rows";
                 failure_reason = "COPY result has no rows";
               }
               
             } catch (copy_err) {
               last_checkpoint = "CRITICAL: COPY INTO failed for file " + current_file_name;
               failure_reason = "COPY command failed: " + copy_err.message;
               
               var copy_error_desc = 'COPY INTO failed - Code: ' + (copy_err.code || 'N/A') + ', State: ' + (copy_err.state || 'N/A') + ', Message: ' + (copy_err.message || 'Unknown error') + ', Stack: ' + (copy_err.stackTraceTxt || 'No stack trace');
               
               try {
                 var copyFailedSql = "COPY FILES INTO @" + stage_name + FailFileFolderPath + " FROM @" + stage_name + processedFilePath;
                 snowflake.createStatement({ sqlText: copyFailedSql }).execute();
                 var removeSql = "REMOVE @" + stage_name + processedFilePath;
                 snowflake.createStatement({ sqlText: removeSql }).execute();
               } catch (move_err) {
               }
               
               try {
                 var insert_stmt = snowflake.createStatement({
                   sqlText: "INSERT INTO DEV_PS_RAW_DB.CORE_REFERENCE.MANIFEST_FILE_IMPORT (CLIENT_NAME, PROCESS_NAME, RAW_TABLE_NAME, FILE_NAME, ERROR_DESCRIPTION) VALUES (?, ?, ?, ?, ?)",
                   binds: [client_name, process_name, raw_table_name, current_file_name, copy_error_desc]
                 });
                 insert_stmt.execute();
               } catch (manifest_err) {
               }
               
               failure_rows.push({
                 client_name: client_name,
                 process_name: process_name,
                 file_name: current_file_name,
                 error: copy_error_desc
               });
             }
             
           } catch (file_err) {
             last_checkpoint = "CRITICAL: File processing error for " + current_file_name;
             failure_reason = "File processing failed: " + file_err.message;
             
             failure_rows.push({
               client_name: client_name,
               process_name: process_name,
               file_name: current_file_name,
               error: 'File processing failed: ' + file_err.message
             });
             continue;
           }
         }
         
         last_checkpoint = "Completed processing all files. Success=" + success_rows.length + ", Failed=" + failure_rows.length;
         
       } else {
         last_checkpoint = "No files found matching pattern";
         failure_reason = "No files found matching pattern '" + file_name_pattern + "' in location '" + s3_location + "'";
         
         var no_files_error = "No files found matching pattern '" + file_name_pattern + "' in location '" + s3_location + "'";
         
         try {
           var insert_stmt = snowflake.createStatement({
             sqlText: "INSERT INTO DEV_PS_RAW_DB.CORE_REFERENCE.MANIFEST_FILE_IMPORT (CLIENT_NAME, PROCESS_NAME, RAW_TABLE_NAME, ERROR_DESCRIPTION) VALUES (?, ?, ?, ?)",
             binds: [client_name, process_name, raw_table_name, no_files_error]
           });
           insert_stmt.execute();
         } catch (manifest_err) {
         }
         
         sendEmail("No Files Found for " + client_name + " - " + process_name, "No files were found matching the pattern: " + file_name_pattern + "\n\nLocation: " + s3_location);
         
         no_files_found = true;
         return;
       }
     } else {
       last_checkpoint = "CRITICAL: No columns found after executing columns query";
       failure_reason = "No columns found for table after applying exclusions - Table: " + raw_table_name + ", Excluded: " + exclude_columns;
       
       var no_columns_error = 'No columns found for table after applying exclusions - Table: ' + raw_table_name + ', Excluded: ' + exclude_columns;
       
       try {
         var insert_stmt = snowflake.createStatement({
           sqlText: "INSERT INTO DEV_PS_RAW_DB.CORE_REFERENCE.MANIFEST_FILE_IMPORT (CLIENT_NAME, PROCESS_NAME, RAW_TABLE_NAME, ERROR_DESCRIPTION) VALUES (?, ?, ?, ?)",
           binds: [client_name, process_name, raw_table_name, no_columns_error]
         });
         insert_stmt.execute();
       } catch (manifest_err) {
       }
       
       failure_rows.push({
         client_name: client_name,
         process_name: process_name,
         file_name: 'N/A',
         error: no_columns_error
       });
     }
   }
   
   if (!foundRecords) {
     last_checkpoint = "CRITICAL: No active configuration found";
     failure_reason = "No active configuration found for CLIENT_NAME='" + client_name + "' and PROCESS_NAME='" + process_name + "' in LOAD_CONFIG_TABLE";
     
     var no_config_error = "No active configuration found for CLIENT_NAME='" + client_name + "' and PROCESS_NAME='" + process_name + "' in LOAD_CONFIG_TABLE";
     
     try {
       var insert_stmt = snowflake.createStatement({
         sqlText: "INSERT INTO DEV_PS_RAW_DB.CORE_REFERENCE.MANIFEST_FILE_IMPORT (CLIENT_NAME, PROCESS_NAME, ERROR_DESCRIPTION) VALUES (?, ?, ?)",
         binds: [client_name, process_name, no_config_error]
       });
       insert_stmt.execute();
     } catch (manifest_err) {
     }
     
     failure_rows.push({
       client_name: client_name,
       process_name: process_name,
       file_name: 'N/A',
       error: no_config_error
     });
   }
   
 } catch (process_err) {
   last_checkpoint = "CRITICAL: Processing function error";
   failure_reason = "Processing function error: " + process_err.message;
 }
}

// Call the processing function
processFiles();

if (failure_reason) {
}

// Log all failures for debugging
if (failure_rows.length > 0) {
 for (var i = 0; i < failure_rows.length; i++) {
 }
}

// Send email only if not already sent and not in no-files scenario
if (!email_sent && !no_files_found) {
 try {
   var html_body = '<html><body><h2>Data Load Summary</h2>';
   
   html_body += '<h3>Successful Loads</h3>';
   html_body += '<table border="1" cellpadding="5" cellspacing="0" style="border-collapse: collapse;">';
   html_body += '<tr style="background-color: #d4edda;">';
   html_body += '<th>Client</th><th>Process</th><th>File Name</th><th>Table</th><th>Start Time</th><th>End Time</th><th>Row Count</th>';
   html_body += '</tr>';
   
   if (success_rows.length > 0) {
     for (var i = 0; i < success_rows.length; i++) {
       var r = success_rows[i];
       html_body += '<tr>';
       html_body += '<td>' + r.client_name + '</td>';
       html_body += '<td>' + r.process_name + '</td>';
       html_body += '<td>' + r.file_name + '</td>';
       html_body += '<td>' + r.table + '</td>';
       html_body += '<td>' + r.start_time + '</td>';
       html_body += '<td>' + r.end_time + '</td>';
       html_body += '<td style="text-align:right;">' + r.row_count + '</td>';
       html_body += '</tr>';
     }
   } else {
     html_body += '<tr><td colspan="7" style="text-align:center;">No successful loads</td></tr>';
   }
   html_body += '</table>';
   
   html_body += '<h3>Failed Loads</h3>';
   html_body += '<table border="1" cellpadding="5" cellspacing="0" style="border-collapse: collapse;">';
   html_body += '<tr style="background-color: #f8d7da;">';
   html_body += '<th>Client</th><th>Process</th><th>File Name</th><th>Error Description</th>';
   html_body += '</tr>';
   
   if (failure_rows.length > 0) {
     for (var j = 0; j < failure_rows.length; j++) {
       var f = failure_rows[j];
       html_body += '<tr>';
       html_body += '<td>' + f.client_name + '</td>';
       html_body += '<td>' + f.process_name + '</td>';
       html_body += '<td>' + f.file_name + '</td>';
       html_body += '<td>' + f.error + '</td>';
       html_body += '</tr>';
     }
   } else {
     html_body += '<tr><td colspan="4" style="text-align:center;">No failed loads</td></tr>';
   }
   html_body += '</table>';
   html_body += '</body></html>';
   var summary_subject = 'PROCESSING SUMMARY: ' + client_name + ' - ' + process_name;
   
   sendEmail(summary_subject, html_body, 'text/html');
 } catch (email_build_err) {
 }
}

// ALWAYS return detailed diagnostic information

var return_message = "";
if (no_files_found) {
 return_message = "Copy finished: No files found for matching pattern";
} else if (success_rows.length > 0 && failure_rows.length === 0) {
 return_message = "Copy finished: Success: " + success_rows.length + "; Failed: 0";
} else if (success_rows.length === 0 && failure_rows.length > 0) {
 return_message = "Copy finished: Success: 0; Failed: " + failure_rows.length;
} else if (success_rows.length > 0 && failure_rows.length > 0) {
 return_message = "Copy finished: Success: " + success_rows.length + "; Failed: " + failure_rows.length;
} else if (!foundRecords) {
 return_message = "Copy finished: No active configuration found";
} else {
 return_message = "Copy finished: Success: 0; Failed: 0 (No files processed)";
}

// Add diagnostic information when 0 files are processed
if (success_rows.length === 0 && !no_files_found) {
 return_message += " | DIAGNOSTIC: Last checkpoint: " + last_checkpoint + " | Last query: " + last_executed_query + " | Reason: " + (failure_reason || "Unknown - check debug logs");
}

return return_message;
$$;
