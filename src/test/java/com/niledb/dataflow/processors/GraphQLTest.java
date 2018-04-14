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

public class GraphQLTest {
	
	/**
	 * Test of onTrigger method, of class JsonProcessor.
	 */
	@org.junit.Test
	public void testOnTrigger() throws IOException {
		
		// Instantiate TestRunner
		TestRunner runner = TestRunners.newTestRunner(new GraphQL());
		
		// Add ControllerServices
		
		// Add properties (configure processor)
		runner.setProperty(GraphQL.ENDPOINT, "http://localhost:8080/graphql");
		runner.setProperty(GraphQL.QUERY, "query {addressList {addressLine1}}");
		runner.setProperty(GraphQL.ATTRIBUTE_NAMES, "authorization,guay");
		runner.setProperty(GraphQL.RESPONSE_TARGET_ATTRIBUTE_NAME, "result");
		
		// Enqueue FlowFiles
		HashMap<String, String> attributes = new HashMap<String, String>();
		attributes.put("authorization", "eyJhbGciOiJIUzI1NiJ9.eyJ1c2VybmFtZSI6InBvc3RncmVzIn0.NueDcaT-wX6DuO1t_91-UeqX-xXZoHpXXkC1VGG_TlU");
		runner.enqueue("{}", attributes);
		
		// Run the Processor
		runner.run();
		
		// Validate Output
		runner.assertQueueEmpty();
		
		/*
		List<MockFlowFile> results = runner.getFlowFilesForRelationship(GraphQL.SUCCESS);
		assertTrue(results.size() == 1);
		
		MockFlowFile result = results.get(0);
		assertTrue(result.isContentEqual("{}"));
		assertEquals(result.getAttribute("result"), "{\"data\":{\"addressList\":[{\"addressLine1\":\"Calle Montmartre, 123\"}]}}");
		*/
	}
}
