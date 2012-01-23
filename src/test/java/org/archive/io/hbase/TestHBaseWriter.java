package org.archive.io.hbase;

import java.io.IOException;

import org.testng.Assert;
import org.testng.annotations.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.archive.io.hbase.HBaseWriter;
import org.archive.io.hbase.HBaseParameters;

/**
 * The Class TestHBaseWriter.
 */
public class TestHBaseWriter {
	
	/** The zkQuorum. */
	String zkQuorum = "localhost";
	
	/** The zkClientPort. */
	int zkClientPort = 2181;
	
	/** The table. */
	String crawlTable = "testCrawl";
	String dnsTable = "testDNS";

	/** The pool maximum active. */
	int poolMaximumActive = 10;
	
	/** The pool maximum wait. */
	int poolMaximumWait = 20;

	/** The hw. */
	HBaseWriter hw;

	/**
	 * Test that bad table values cannot be used when creating an instance of
	 * HbaseWriter.
	 * 
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	@Test()
	public void testCreateHBaseWriter() throws IOException {
	  Configuration conf = HBaseConfiguration.create();
		// Test
		try {
			hw = new HBaseWriter(conf, null);
			Assert.assertNull(hw);
		} catch (IllegalArgumentException e) {
			Assert.assertNotNull(e);
		}

		try {
			hw = new HBaseWriter(conf, new HBaseParameters());
			Assert.assertNull(hw);
		} catch (IllegalArgumentException e) {
			Assert.assertNotNull(e);
		}

	}
}
