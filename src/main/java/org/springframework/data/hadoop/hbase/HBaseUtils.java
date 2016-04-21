/*
 * Copyright 2011-2013 the original author or authors.
 * 
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
 */
package org.springframework.data.hadoop.hbase;

import java.io.IOException;
import java.nio.charset.Charset;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.springframework.dao.DataAccessException;
import org.springframework.util.StringUtils;

/**
 * Helper class featuring methods for HBase table handling and exception translation.
 *
 * @author Costin Leau
 * @author zhuhyc
 */
public class HBaseUtils {

    /**
     * Converts the given (HBase) exception to an appropriate exception from <tt>org.springframework.dao</tt> hierarchy.
     *
     * @param ex HBase exception that occurred
     * @return the corresponding DataAccessException instance
     */
    public static DataAccessException convertHBaseException(Exception ex) {
        return new HBaseSystemException(ex);
    }

    /**
     * Retrieves an HBase table instance identified by its name  using the given connection.
     *
     * @param tableName  table name
     * @param connection connection factory
     * @return table instance
     */
    public static Table getHTable(String tableName, Connection connection) {
//		if (HBaseSynchronizationManager.hasResource(tableName)) {
//			return (HTable) HBaseSynchronizationManager.getResource(tableName);
//		}
        try {
            return connection.getTable(TableName.valueOf(tableName));
        } catch (Exception ex) {
            throw convertHBaseException(ex);
        }
    }

    static Charset getCharset(String encoding) {
        return (StringUtils.hasText(encoding) ? Charset.forName(encoding) : Charset.forName("UTF-8"));
    }


    /**
     * Releases (or closes) the given table, created via the given configuration if it is not managed externally (or bound to the thread).
     *
     * @param table        table
     */
    public static void releaseTable(Table table) {
        try {
            doReleaseTable(table);
        } catch (IOException ex) {
            throw HBaseUtils.convertHBaseException(ex);
        }
    }

    private static void doReleaseTable(Table table)
            throws IOException {
        if (table == null) {
            return;
        }
        table.close();
    }

}