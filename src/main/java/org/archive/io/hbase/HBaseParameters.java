/*
 *  This file is part of the Heritrix web crawler (crawler.archive.org).
 *
 *  Licensed to the Internet Archive (IA) by one or more individual 
 *  contributors. 
 *
 *  The IA licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.archive.io.hbase;

import org.archive.io.ArchiveFileConstants;

/**
 * Configures the values of the column family/qualifier used
 * for the crawl. Also contains a full set of default values that
 * are the same as the previous Heritrix2 implementation.
 *
 * Meant to be configured within the Spring framework either inline
 * of HBaseWriterProcessor or as a named bean and references later on.
 *
 * <pre>
 * {@code
 * <bean id="hbaseParameterSettings" class="org.archive.io.hbase.HBaseParameters">
 *   <property name="contentColumnFamily" value="newcontent" />
 *   <!-- Overwrite more options here -->
 * </bean>
 * }
 * </pre>
 *
 * @see org.archive.modules.writer.HBaseWriterProcessor
 *  {@link org.archive.modules.writer.HBaseWriterProcessor} for a full example
 *
 * @author greglu
 */
public class HBaseParameters implements ArchiveFileConstants {

    public static final String CONTENT_TABLE_NAME = "content";
    public static final String URL_TABLE_NAME = "url";

    // "content" column family and qualifiers
    public static final String CONTENT_COLUMN_FAMILY = "c";
    public static final String CONTENT_COLUMN_NAME = "r";

    // "curi" column family and qualifiers
    public static final String CURI_COLUMN_FAMILY = "u";
    public static final String IP_COLUMN_NAME = "i";
    public static final String PATH_FROM_SEED_COLUMN_NAME = "p";
    public static final String VIA_COLUMN_NAME = "v";
    public static final String URL_COLUMN_NAME = "u";
    public static final String REQUEST_COLUMN_NAME = "req";
    public static final String RESPONSE_COLUMN_NAME = "rsp";
    public static final String MIMETYPE_COLUMN_NAME = "m";
    public static final String HASH_COLUMN_NAME = "h";
    public static final String STATUS_COLUMN_NAME = "s";
    public static final String SOURCE_TAG_COLUMN_NAME = "st";

    // the zk client port name, this has to match what is in hbase-site.xml for the clientPort config attribute.
    public static String ZOOKEEPER_CLIENT_PORT = "hbase.zookeeper.property.clientPort";

    private String contentTableName = CONTENT_TABLE_NAME;
    private String urlTableName = URL_TABLE_NAME;

    private String contentColumnFamily = CONTENT_COLUMN_FAMILY;
    private String contentColumnName = CONTENT_COLUMN_NAME;

    private String curiColumnFamily = CURI_COLUMN_FAMILY;
    private String ipColumnName = IP_COLUMN_NAME;
    private String pathFromSeedColumnName = PATH_FROM_SEED_COLUMN_NAME;
    private String viaColumnName = VIA_COLUMN_NAME;
    private String urlColumnName = URL_COLUMN_NAME;
    private String requestColumnName = REQUEST_COLUMN_NAME;
    private String responseColumnName = RESPONSE_COLUMN_NAME;
    private String mimeTypeColumnName = MIMETYPE_COLUMN_NAME;
    private String hashColumnName = HASH_COLUMN_NAME;
    private String statusColumnName = STATUS_COLUMN_NAME;
    private String sourceTagColumnName = SOURCE_TAG_COLUMN_NAME;

    public String getContentTableName() {
      return contentTableName;
    }

    public void setContentTableName(String contentTableName) {
      this.contentTableName = contentTableName;
    }

    public String getUrlTableName() {
      return urlTableName;
    }

    public void setUrlTableName(String urlTableName) {
      this.urlTableName = urlTableName;
    }

    public String getContentColumnFamily() {
        return contentColumnFamily;
    }

    public void setContentColumnFamily(String contentColumnFamily) {
        this.contentColumnFamily = contentColumnFamily;
    }

    public String getContentColumnName() {
        return contentColumnName;
    }

    public void setContentColumnName(String contentColumnName) {
        this.contentColumnName = contentColumnName;
    }

    public String getCuriColumnFamily() {
        return curiColumnFamily;
    }

    public void setCuriColumnFamily(String curiColumnFamily) {
        this.curiColumnFamily = curiColumnFamily;
    }

    public String getIpColumnName() {
        return ipColumnName;
    }

    public void setIpColumnName(String ipColumnName) {
        this.ipColumnName = ipColumnName;
    }

    public String getPathFromSeedColumnName() {
        return pathFromSeedColumnName;
    }

    public void setPathFromSeedColumnName(String pathFromSeedColumnName) {
        this.pathFromSeedColumnName = pathFromSeedColumnName;
    }

    public String getViaColumnName() {
        return viaColumnName;
    }

    public void setViaColumnName(String viaColumnName) {
        this.viaColumnName = viaColumnName;
    }

    public String getUrlColumnName() {
        return urlColumnName;
    }

    public void setUrlColumnName(String urlColumnName) {
        this.urlColumnName = urlColumnName;
    }

    public String getRequestColumnName() {
        return requestColumnName;
    }

    public void setRequestColumnName(String requestColumnName) {
        this.requestColumnName = requestColumnName;
    }

    public String getResponseColumnName() {
      return responseColumnName;
  }

    public void setResponseColumnName(String responseColumnName) {
      this.responseColumnName = requestColumnName;
    }

    public String getMimeTypeColumnName() {
      return mimeTypeColumnName;
    }

    public void setMimeTypeColumnName(String mimeTypeColumnName) {
      this.mimeTypeColumnName = mimeTypeColumnName;
    }

    public String getHashColumnName() {
      return hashColumnName;
    }

    public void setHashColumnName(String hashColumnName) {
      this.hashColumnName = hashColumnName;
    }

    public String getStatusColumnName() {
      return statusColumnName;
    }

    public void setStatusColumnName(String statusColumnName) {
      this.statusColumnName = statusColumnName;
    }

    public String getSourceTagColumnName() {
      return sourceTagColumnName;
    }

    public void setSourceTagColumnName(String sourceTagColumnName) {
      this.sourceTagColumnName = sourceTagColumnName;
    }

    public String getZookeeperClientPort() {
      return ZOOKEEPER_CLIENT_PORT;
    }
}
