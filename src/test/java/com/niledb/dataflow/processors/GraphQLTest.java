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
		runner.setProperty(GraphQL.ENDPOINT, "http://localhost/graphql");
		runner.setProperty(GraphQL.QUERY, "{ Souk_ItemList(where: { brand: {EQ: \"${brand}\"} }) { brand name }}");
		//runner.setProperty(GraphQL.ATTRIBUTE_NAMES, "name,surname");
		runner.setProperty(GraphQL.RESPONSE_TARGET_ATTRIBUTE_NAME, "result");
		
		// Enqueue FlowFiles
		HashMap<String, String> attributes = new HashMap<String, String>();
		attributes.put("brand", "Gucci");
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
