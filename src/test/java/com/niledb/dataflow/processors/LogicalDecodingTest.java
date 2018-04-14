/**
 * Copyright (C) 2018 NileDB, Inc.
 *
 *    This program is free software: you can redistribute it and/or modify
 *    it under the terms of the GNU General Public License, version 3,
 *    as published by the Free Software Foundation.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 *    GNU General Public License for more details.
 *
 *    You should have received a copy of the GNU General Public License
 *    along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package com.niledb.dataflow.processors;

import java.io.IOException;
import java.util.HashMap;
//import java.util.List;

//import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;

//import static org.junit.Assert.*;

public class LogicalDecodingTest {
	
	/**
	 * Test of onTrigger method, of class JsonProcessor.
	 */
	@org.junit.Test
	public void testOnTrigger() throws IOException {
		
		// Instantiate TestRunner
		TestRunner runner = TestRunners.newTestRunner(new LogicalDecoding());
		
		// Add ControllerServices
		
		// Add properties (configure processor)
		runner.setProperty(LogicalDecoding.DB_NAME, "niledb");
		runner.setProperty(LogicalDecoding.DB_HOST, "localhost");
		runner.setProperty(LogicalDecoding.DB_PORT, "5433");
		runner.setProperty(LogicalDecoding.DB_USERNAME, "postgres");
		runner.setProperty(LogicalDecoding.DB_PASSWORD, "postgres");
		runner.setProperty(LogicalDecoding.LSN_FILE_NAME, "lsn.txt");
		runner.setProperty(LogicalDecoding.DB_SSL, "false");
		runner.setProperty(LogicalDecoding.DB_SSL_MODE, "verify-ca");
		runner.setProperty(LogicalDecoding.DB_SSL_ROOT_CERT, "misc/ssl/BaltimoreCyberTrustRoot.crt.pem");
		
		// Enqueue FlowFiles
		HashMap<String, String> attributes = new HashMap<String, String>();
		runner.enqueue("{}", attributes);
		
		// Run the Processor
		runner.run(1, true);
		
		// Validate Output
		//runner.assertQueueEmpty();
		
		/*
		List<MockFlowFile> results = runner.getFlowFilesForRelationship(GraphQL.SUCCESS);
		assertTrue(results.size() == 1);
		
		MockFlowFile result = results.get(0);
		assertTrue(result.isContentEqual("{}"));
		assertEquals(result.getAttribute("result"), "{\"data\":{\"addressList\":[{\"addressLine1\":\"Calle Montmartre, 123\"}]}}");
		*/
	}
}
