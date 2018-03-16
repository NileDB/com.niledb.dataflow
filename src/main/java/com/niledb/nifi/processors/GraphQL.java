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
package com.niledb.nifi.processors;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

@SideEffectFree
@Tags({ "GraphQL", "API", "NileDB", "Invoke", "Service", "niledb.com" })
@CapabilityDescription("Invokes NileDB's GraphQL services populating GraphQL variables with NIFI attributes.")
public class GraphQL extends AbstractProcessor {
	
	private List<PropertyDescriptor> properties;
	private Set<Relationship> relationships;
	
	public static final PropertyDescriptor QUERY = new PropertyDescriptor.Builder()
			.name("query")
			.displayName("GraphQL query")
			.description("GraphQL query. It can be copied from GraphiQL or GraphQL Playground editors.")
			.expressionLanguageSupported(false)
			.required(true)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
			.build();
	
	public static final PropertyDescriptor ENDPOINT = new PropertyDescriptor.Builder()
			.name("endpoint")
			.displayName("GraphQL endpoint")
			.description("GraphQL service endpoint (i.e. https://niledb.com/graphql.")
			.defaultValue("http://niledb/graphql")
			.expressionLanguageSupported(false)
			.required(true)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
			.build();

	public static final PropertyDescriptor RESPONSE_TARGET_ATTRIBUTE_NAME = new PropertyDescriptor.Builder()
			.name("responseTargetAttributeName")
			.displayName("Response target attribute name")
			.description("Attribute where the GraphQL services response must be stored.")
			.required(false)
			.defaultValue("response")
			.addValidator(StandardValidators.ATTRIBUTE_KEY_VALIDATOR)
			.build();

	public static final Relationship SUCCESS = new Relationship.Builder().name("SUCCESS")
			.description("Success relationship").build();
	
	@Override
	public void init(final ProcessorInitializationContext context) {
		List<PropertyDescriptor> properties = new ArrayList<>();
		properties.add(QUERY);
		properties.add(ENDPOINT);
		properties.add(RESPONSE_TARGET_ATTRIBUTE_NAME);
		this.properties = Collections.unmodifiableList(properties);
		
		Set<Relationship> relationships = new HashSet<>();
		relationships.add(SUCCESS);
		this.relationships = Collections.unmodifiableSet(relationships);
	}
	
	@Override
	public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
		
		FlowFile flowFile = session.get();
		
		String query = context.getProperty("query").getValue();
		String endpoint = context.getProperty("endpoint").getValue();
		String responseTargetAttributeName = context.getProperty("responseTargetAttributeName").getValue();
		//String sourceAddress = context.getProperty("sourceAddress").evaluateAttributeExpressions(flowFile).getValue();
		
		try {
			if (query != null && !query.equals("")
					&& endpoint != null && !endpoint.equals("")) {
				System.out.println("!!!!!! Query: " + query);
				System.out.println("!!!!!! Endpoint: " + endpoint);
				
				if (responseTargetAttributeName != null && !responseTargetAttributeName.equals("")) {
					session.putAttribute(flowFile, responseTargetAttributeName, "TO-DO");
				}
			}
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		session.transfer(flowFile, SUCCESS);
	}
	
	@Override
	public Set<Relationship> getRelationships() {
		return relationships;
	}
	
	@Override
	public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
		return properties;
	}
}
