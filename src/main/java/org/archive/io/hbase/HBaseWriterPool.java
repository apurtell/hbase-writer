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

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.archive.io.WriterPool;
import org.archive.io.WriterPoolMember;

/**
 * @author stack
 */
public class HBaseWriterPool extends WriterPool {

    private Configuration conf;
    private HBaseParameters parameters;

    public HBaseWriterPool(final AtomicInteger serial, final String zkQuorum,
        final int zkClientPort, final HBaseParameters parameters,
        final int poolMaximumActive, final int poolMaximumWait) {
        super(
            // a serial 
            serial, 
            new HBaseWriterPoolSettings(),
            // maximum active writers in the writer pool
            poolMaximumActive,
            // maximum wait time
            poolMaximumWait);

        this.conf = HBaseConfiguration.create();

        this.parameters = parameters;

        // set the zk quorum list
        if (zkQuorum != null && zkQuorum.length() > 0) {
            this.conf.setStrings(HConstants.ZOOKEEPER_QUORUM, zkQuorum.split(","));
        }

        // set the client port
        if (zkClientPort > 0) {
            this.conf.setInt("hbase.zookeeper.property.clientPort", zkClientPort);
        }
    }

    @Override
    protected WriterPoolMember makeWriter() {
      try {
        return new HBaseWriter(this.conf, parameters);
      } catch (IOException e) {
        return null;
      }
    }
}
