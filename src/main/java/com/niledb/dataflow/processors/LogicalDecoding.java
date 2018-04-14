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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.annotation.lifecycle.OnUnscheduled;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.postgresql.PGProperty;
import org.postgresql.jdbc.PgConnection;
import org.postgresql.replication.LogSequenceNumber;
import org.postgresql.replication.PGReplicationStream;
import org.postgresql.replication.fluent.logical.ChainedLogicalStreamBuilder;

@SideEffectFree
@Tags({ "PostgreSQL", "Logical Decoding", "Data Publication/Subscription", "Subscribe to replication slot", "data streaming", "niledb.com" })
@CapabilityDescription("Subscribes to PostgreSQL's replication slot and receives data from NileDB platform.")
@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
public class LogicalDecoding extends AbstractProcessor {

	static final AllowableValue SSL_MODE_DISABLE = new AllowableValue("disable");
	static final AllowableValue SSL_MODE_ALLOW = new AllowableValue("allow");
	static final AllowableValue SSL_MODE_PREFER = new AllowableValue("prefer");
	static final AllowableValue SSL_MODE_REQUIRE = new AllowableValue("require");
	static final AllowableValue SSL_MODE_VERIFY_CA = new AllowableValue("verify-ca");
	static final AllowableValue SSL_MODE_VERIFY_FULL = new AllowableValue("verify-full");
	
	private List<PropertyDescriptor> properties;
	private Set<Relationship> relationships;

	private static PgConnection connection = null;
	private static PGReplicationStream stream = null;
	private static StringBuffer transaction = null;
	
	public static final PropertyDescriptor DB_NAME = new PropertyDescriptor.Builder()
			.name("dbName")
			.displayName("Database name")
			.description("The name of the database.")
			.defaultValue("niledb")
			.expressionLanguageSupported(false)
			.required(true)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
			.build();
	
	public static final PropertyDescriptor DB_HOST = new PropertyDescriptor.Builder()
			.name("hostname")
			.displayName("Host name")
			.description("The name/address of the host.")
			.defaultValue("db")
			.expressionLanguageSupported(false)
			.required(true)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
			.build();
	
	public static final PropertyDescriptor DB_PORT = new PropertyDescriptor.Builder()
			.name("port")
			.displayName("Port")
			.description("The port number where PostgreSQL server is listening.")
			.defaultValue("5432")
			.expressionLanguageSupported(false)
			.required(true)
			.addValidator(StandardValidators.PORT_VALIDATOR)
			.build();
	
	public static final PropertyDescriptor DB_REPLICATION_SLOT = new PropertyDescriptor.Builder()
			.name("replicationSlot")
			.displayName("Replication slot")
			.description("The replication slot to subscribe to.")
			.defaultValue("slot")
			.expressionLanguageSupported(false)
			.required(true)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
			.build();
	
	public static final PropertyDescriptor DB_USERNAME = new PropertyDescriptor.Builder()
			.name("username")
			.displayName("Username")
			.description("The username to be connected as.")
			.defaultValue("postgres")
			.expressionLanguageSupported(false)
			.required(true)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
			.build();
	
	public static final PropertyDescriptor DB_PASSWORD = new PropertyDescriptor.Builder()
			.name("password")
			.displayName("Password")
			.description("The password to be used.")
			.defaultValue("postgres")
			.expressionLanguageSupported(false)
			.required(true)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
			.build();
	
	public static final PropertyDescriptor LSN_FILE_NAME = new PropertyDescriptor.Builder()
			.name("lsnFileName")
			.displayName("LSN (Log Sequence Number) file name")
			.description("The file where the last LSN is going to be stored.")
			.expressionLanguageSupported(false)
			.required(false)
			.addValidator(StandardValidators.NON_BLANK_VALIDATOR)
			.build();
	
	public static final PropertyDescriptor DB_SSL = new PropertyDescriptor.Builder()
			.name("ssl")
			.displayName("SSL")
			.description("It indicates if the connection uses SSL.")
			.defaultValue("false")
			.expressionLanguageSupported(false)
			.required(true)
			.addValidator(StandardValidators.BOOLEAN_VALIDATOR)
			.build();
	
	public static final PropertyDescriptor DB_SSL_MODE = new PropertyDescriptor.Builder()
			.name("sslMode")
			.displayName("SSL mode")
			.description("The SSL mode.")
			.defaultValue(SSL_MODE_VERIFY_CA.getValue())
			.expressionLanguageSupported(false)
			.required(true)
			.allowableValues(SSL_MODE_DISABLE, SSL_MODE_ALLOW, SSL_MODE_PREFER, SSL_MODE_REQUIRE, SSL_MODE_VERIFY_CA, SSL_MODE_VERIFY_FULL)
			.build();
	
	public static final PropertyDescriptor DB_SSL_ROOT_CERT = new PropertyDescriptor.Builder()
			.name("sslRootCert")
			.displayName("SSL root certificate")
			.description("The SSL root certificate path")
			.defaultValue("misc/ssl/BaltimoreCyberTrustRoot.crt.pem")
			.expressionLanguageSupported(false)
			.required(true)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
			.build();
	
	public static final PropertyDescriptor RESPONSE_TARGET_ATTRIBUTE_NAME = new PropertyDescriptor.Builder()
			.name("responseTargetAttributeName")
			.displayName("Response target attribute name")
			.description("Attribute where the GraphQL services response must be stored.")
			.required(false)
			.defaultValue("transaction")
			.addValidator(StandardValidators.ATTRIBUTE_KEY_VALIDATOR)
			.build();
	
	public static final Relationship SUCCESS = new Relationship.Builder().name("SUCCESS")
			.description("Success relationship").build();
	
	@Override
	public void init(final ProcessorInitializationContext context) {
		List<PropertyDescriptor> properties = new ArrayList<>();
		properties.add(DB_NAME);
		properties.add(DB_HOST);
		properties.add(DB_PORT);
		properties.add(DB_REPLICATION_SLOT);
		properties.add(DB_USERNAME);
		properties.add(DB_PASSWORD);
		properties.add(LSN_FILE_NAME);
		properties.add(DB_SSL);
		properties.add(DB_SSL_MODE);
		properties.add(DB_SSL_ROOT_CERT);
		properties.add(RESPONSE_TARGET_ATTRIBUTE_NAME);
		this.properties = Collections.unmodifiableList(properties);
		
		Set<Relationship> relationships = new HashSet<>();
		relationships.add(SUCCESS);
		this.relationships = Collections.unmodifiableSet(relationships);
	}
	
	private synchronized PgConnection getConnection(final ProcessContext context) throws Exception {
		if (connection == null) {

			String dbName = context.getProperty("dbName").getValue();
			String hostname = context.getProperty("hostname").getValue();
			String port = context.getProperty("port").getValue();
			String username = context.getProperty("username").getValue();
			String password = context.getProperty("password").getValue();
			String ssl = context.getProperty("ssl").getValue();
			String sslMode = context.getProperty("sslMode").getValue();
			String sslRootCert = context.getProperty("sslRootCert").getValue();

			Properties properties = new Properties();
			PGProperty.ASSUME_MIN_SERVER_VERSION.set(properties, "10.0");
			PGProperty.REPLICATION.set(properties, "database");
			PGProperty.PREFER_QUERY_MODE.set(properties, "simple");
			PGProperty.USER.set(properties, username);
			PGProperty.PASSWORD.set(properties, password);
			
			if ((Boolean) Boolean.parseBoolean(ssl)) {
				PGProperty.SSL.set(properties, true);
				PGProperty.SSL_MODE.set(properties, sslMode);
				PGProperty.SSL_ROOT_CERT.set(properties, sslRootCert);
			}
			
			Class.forName("org.postgresql.Driver");
			connection = (PgConnection) DriverManager.getConnection("jdbc:postgresql://" + hostname + ":" + port + "/" + dbName, properties);
		}
		return connection;
	}

	private synchronized PGReplicationStream getStream(ProcessContext context) throws Exception {
		ComponentLog log = getLogger();
		if (stream == null) {
			String replicationSlot = context.getProperty("replicationSlot").getValue();
			String lsnFileName = context.getProperty("lsnFileName").getValue();
			
			ChainedLogicalStreamBuilder builder = getConnection(context).getReplicationAPI()
					.replicationStream()
					.logical()
					.withSlotName(replicationSlot);
			
			if (lsnFileName != null) {
				File lsnFile = new File(lsnFileName);
				if (lsnFile.exists()) {
					try {
						BufferedReader br = new BufferedReader(new FileReader(lsnFile));
						builder.withStartPosition(LogSequenceNumber.valueOf(br.readLine()));
						br.close();
					}
					catch (Exception e) {
						log.error(e.getMessage(), e);
						e.printStackTrace();
					}
				}
			}
			
			stream = builder.start();
		}
		return stream;
	}
	
    @Override
	public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
		ComponentLog log = getLogger();

		try {
			PGReplicationStream stream = getStream(context);
			ByteBuffer buffer = stream.readPending();
			
			if (buffer == null) {
				context.yield();
				return;
			}
			
			String line = StandardCharsets.UTF_8.decode(buffer).toString();
			String responseTargetAttributeName = context.getProperty("responseTargetAttributeName").getValue();
			if (responseTargetAttributeName != null
					&& !responseTargetAttributeName.equals("")) {
				if (transaction == null)
					transaction = new StringBuffer();
				transaction.append(line + "\n");
				if (line != null && line.startsWith("COMMIT")) {
			    	FlowFile flowFile = session.get();
			    	if (flowFile == null) {
			    		flowFile = session.create();
			    	}
					session.putAttribute(flowFile, responseTargetAttributeName, transaction.toString());
					session.transfer(flowFile, SUCCESS);
					transaction = null;
				}
			}
		}
		catch (Exception e) {
			log.error(e.getMessage(), e);
			e.printStackTrace();
			session.rollback();
		}
	}
	
	@Override
	public Set<Relationship> getRelationships() {
		return relationships;
	}
	
	@Override
	public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
		return properties;
	}
	
	@OnStopped
	public void close() {
		ComponentLog log = getLogger();
		try {
			if (stream != null) {
				stream.close();
				stream = null;
			}
			if (connection != null) {
				connection.close();
				connection = null;
			}
		}
		catch (Exception e) {
			log.error(e.getMessage(), e);
			e.printStackTrace();
		}
	}
	
	@OnUnscheduled
	public void interruptActiveThreads(ProcessContext context) throws Exception {
		ComponentLog log = getLogger();
		String lsnFileName = context.getProperty("lsnFileName").getValue();
		if (lsnFileName != null) {
			PrintWriter pw = new PrintWriter(new File(lsnFileName));
			pw.println(stream.getLastReceiveLSN().asString());
			pw.close();
			log.debug("State saved to " + lsnFileName);
		}
	}
}
