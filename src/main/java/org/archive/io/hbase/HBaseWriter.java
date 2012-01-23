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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IOUtils;
import org.archive.io.RecordingInputStream;
import org.archive.io.RecordingOutputStream;
import org.archive.io.ReplayInputStream;
import org.archive.io.WriterPoolMember;
import org.archive.modules.CrawlURI;

/**
 * HBase implementation.
 */
public class HBaseWriter extends WriterPoolMember {
    private HBaseParameters hbaseOptions;
    private final HTable crawlTable;
    private final HTable urlTable;

    private static final Pattern URI_RE_PARSER =
      Pattern.compile("^([^:/?#]+://(?:[^/?#@]+@)?)([^:/?#]+)(.*)$");

    /**
     * @see org.archive.io.hbase.HBaseParameters
     */
    public HBaseParameters getHbaseOptions() {
        return hbaseOptions;
    }

    public HTable getCrawlTable() {
      return crawlTable;
    }

    public HTable getUrlTable() {
      return urlTable;
    }

    /**
     * Instantiates a new HBaseWriter for the WriterPool to use in heritrix2.
     * 
     * @throws IOException Signals that an I/O exception has occurred.
     */
    public HBaseWriter(final Configuration conf, 
        final HBaseParameters parameters) throws IOException {
      super(null, new HBaseWriterPoolSettings(), null);
      this.hbaseOptions = parameters;
      this.crawlTable = new HTable(conf, hbaseOptions.getCrawlTableName());
      this.crawlTable.setAutoFlush(false);
      this.urlTable = new HTable(conf, hbaseOptions.getUrlTableName());
      this.urlTable.setAutoFlush(false);
    }

    /**
     * This is a stub method and is here to allow extension/overriding for
     * custom content parsing, data manipulation and to populate new columns.
     * 
     * For Example : html parsing, text extraction, analysis and transformation
     * and storing the results in new column families/columns using the batch
     * update object. Or even saving the values in other custom hbase tables 
     * or other remote data sources. (a.k.a. anything you want)
     * 
     * @param put the stateful put object containing all the row data to be written.
     * @param replayInputStream the replay input stream containing the raw content gotten by heritrix crawler.
     * @param streamSize the stream size
     * 
     * @throws IOException Signals that an I/O exception has occurred.
     */
    protected void processContent(Put put, ReplayInputStream replayInputStream, int streamSize) throws IOException {
        // Below is just an example of a typical use case of overriding this method.
        // I.E.: The goal below is to process the raw content array and parse it to a new byte array.....
        // byte[] rowKey = put.getRow();
        // byte[] rawContent = this.getByteArrayFromInputStream(replayInputStream, streamSize)
        // // process rawContent and create output to store in new columns. 
        // byte[] someParsedByteArray = userDefinedMethondToProcessRawContent(rawContent);
        // put.add(Bytes.toBytes("some_column_family"), Bytes.toBytes("a_new_column_name"), someParsedByteArray);
    }

    /**
     * Write the crawled output to the configured HBase table.
     * Write each row key as the url with reverse domain and optionally process any content.
     * 
     * @param curi URI of crawled document
     * @param ip IP of remote machine.
     * @param recordingOutputStream recording input stream that captured the response
     * @param recordingInputStream recording output stream that captured the GET request
     * 
     * @throws IOException Signals that an I/O exception has occurred.
     */
    public void write(final CrawlURI curi, final String ip, final RecordingOutputStream recordingOutputStream, 
            final RecordingInputStream recordingInputStream) throws IOException {
        // generate the target url of the crawled document
        String url = curi.toString();

        // create the hbase friendly rowkey
        String rowKey = HBaseWriter.createURLKey(url);

        byte[] curiFamily =
            Bytes.toBytes(getHbaseOptions().getCuriColumnFamily());
        byte[] contentFamily =
            Bytes.toBytes(getHbaseOptions().getContentColumnFamily());

        // create an hbase updateable object (the put object)
        // Constructor takes the rowkey as the only argument
        Put curiPut = new Put(Bytes.toBytes(rowKey));

        // status
        curiPut.add(curiFamily,
            Bytes.toBytes(getHbaseOptions().getStatusColumnName()),
            Bytes.toBytes(curi.getFetchStatus()));

        // write the target url to the url column
        curiPut.add(curiFamily,
            Bytes.toBytes(getHbaseOptions().getUrlColumnName()),
            Bytes.toBytes(url));

        // write the target ip to the ip column
        curiPut.add(curiFamily, 
            Bytes.toBytes(getHbaseOptions().getIpColumnName()),
            Bytes.toBytes(ip));

        // path from seed
        String pathFromSeed = curi.getPathFromSeed();
        if (pathFromSeed != null) {
          pathFromSeed = pathFromSeed.trim();
          if (pathFromSeed.length() > 0) {
            curiPut.add(curiFamily,
                Bytes.toBytes(getHbaseOptions().getPathFromSeedColumnName()),
                Bytes.toBytes(curi.getPathFromSeed().trim()));
          }
        }

        // via
        if (curi.getVia() != null) {
          String viaStr = curi.getVia().toString().trim();
          if (viaStr.length() > 0) {
            curiPut.add(curiFamily,
                Bytes.toBytes(getHbaseOptions().getViaColumnName()),
                Bytes.toBytes(HBaseWriter.createURLKey(viaStr)));
          }
        }

        // source tag
        String sourceTag = curi.getSourceTag();
        if (sourceTag != null) {
          curiPut.add(curiFamily,
              Bytes.toBytes(getHbaseOptions().getSourceTagColumnName()),
              Bytes.toBytes(sourceTag));
        }

        // content type
        String contentType = curi.getContentType();
        if (contentType != null) {
          // add the mime type of the response 
          curiPut.add(curiFamily,
              Bytes.toBytes(getHbaseOptions().getMimeTypeColumnName()),
              Bytes.toBytes(contentType));
        }

        // request
        if (recordingOutputStream.getSize() > 0) {
          ReplayInputStream request = recordingOutputStream.getReplayInputStream();
          try {
            ByteArrayOutputStream os = new ByteArrayOutputStream();
            request.readContentTo(os);
            curiPut.add(curiFamily,
                Bytes.toBytes(getHbaseOptions().getRequestColumnName()),
                os.toByteArray());
          } finally {
            IOUtils.closeStream(request);
          }
        }

        // response

        ReplayInputStream response = recordingInputStream.getReplayInputStream();
        try {

          // headers
          if (response.getHeaderSize() > 0) {
            ByteArrayOutputStream os = new ByteArrayOutputStream();
            response.readHeaderTo(os);
            curiPut.add(curiFamily,
                Bytes.toBytes(getHbaseOptions().getResponseColumnName()),
                os.toByteArray());
          }

          // content
          if (response.getContentSize() > 0) {
            ByteArrayOutputStream os = new ByteArrayOutputStream();            
            response.readContentTo(os);
            byte[] content = os.toByteArray();

            String hashKey = HBaseWriter.createHashKey(content);

            curiPut.add(curiFamily,
                Bytes.toBytes(getHbaseOptions().getHashColumnName()), 
                Bytes.toBytes(hashKey));

            Put contentPut = new Put(Bytes.toBytes(hashKey));

            contentPut.add(contentFamily, 
                Bytes.toBytes(getHbaseOptions().getContentColumnName()),
                content);

            contentPut.add(curiFamily,
                Bytes.toBytes(getHbaseOptions().getUrlColumnName()),
                Bytes.toBytes(rowKey));

            getCrawlTable().put(contentPut);
          }
        } finally {
          IOUtils.closeStream(response);
        }

        getUrlTable().put(curiPut);
    }

    @Override
    public void close() throws IOException {
        this.crawlTable.close();
        this.urlTable.close();
        super.close();
    }

    private static Matcher getMatcher(final String u) {
      if (u == null || u.length() <= 0) {
        return null;
      }
      return URI_RE_PARSER.matcher(u);
    }

    private static String reverseHostname(final String hostname) {
      if (hostname == null) {
        return "";
      }
      StringBuilder sb = new StringBuilder(hostname.length());
      for (StringTokenizer st = new StringTokenizer(hostname, ".", false);
          st.hasMoreElements();) {
        Object next = st.nextElement();
        if (sb.length() > 0) {
          sb.insert(0, ".");
        }
        sb.insert(0, next);
      }
      return sb.toString();
    }

    public static String createURLKey(final String u) {
      Matcher m = getMatcher(u);
      if (m == null || !m.matches()) {
        // dns "URLs" don't match as them
        if (u.startsWith("dns:")) {
          return reverseHostname(u.substring(4));
        }
        // If no match, return original String.
        return u;
      }
      //String scheme = m.group(1);
      String host = m.group(2);
      String path = m.group(3);
      if (path.isEmpty()) {
        path = "/";
      }
      return reverseHostname(host) + path;
    }

    private static final char[] HEX_CHARS = "0123456789abcdef".toCharArray();

    static String asHex(byte[] buf) {
      char[] chars = new char[2 * buf.length];
      for (int i = 0; i < buf.length; ++i) {
        chars[2 * i] = HEX_CHARS[(buf[i] & 0xF0) >>> 4];
        chars[2 * i + 1] = HEX_CHARS[buf[i] & 0x0F];
      }
      return new String(chars);
    }

    public static String createHashKey(byte[] content) throws IOException {
      try {
        return asHex(MessageDigest.getInstance("SHA1").digest(content));
      } catch (NoSuchAlgorithmException e) {
        throw new IOException(e);
      }      
    }
}