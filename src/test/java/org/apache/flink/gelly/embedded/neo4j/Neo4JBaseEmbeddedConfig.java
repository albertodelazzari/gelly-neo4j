package org.apache.flink.gelly.embedded.neo4j;

import java.util.HashMap;
import java.util.Map;

import org.apache.flink.streaming.connectors.neo4j.Neo4JDriverWrapper;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.neo4j.driver.v1.AuthToken;
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Config.EncryptionLevel;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Session;
import org.neo4j.harness.junit.Neo4jRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author Alberto De Lazzari
 *
 */
public class Neo4JBaseEmbeddedConfig extends GellyBaseConfig {

	private static final Logger LOGGER = LoggerFactory.getLogger(Neo4JBaseEmbeddedConfig.class);

	@ClassRule
	public static Neo4jRule neo4jRule;

	public static Session embeddedSession;

	public static Map<String, String> neo4JConfig;

	static {
		neo4jRule = new Neo4jRule().withFixture("create (i:Item {description:'an item'})");
	}

	@Before
	public void init() {
		neo4JConfig = new HashMap<>();
		neo4JConfig.put(Neo4JDriverWrapper.URL, neo4jRule.boltURI().toString());
		neo4JConfig.put(Neo4JDriverWrapper.USERNAME_PARAM, "neo4j");
		neo4JConfig.put(Neo4JDriverWrapper.PASSWORD_PARAM, "password");
		neo4JConfig.put(Neo4JDriverWrapper.SESSION_LIVENESS_TIMEOUT, "4000");

		StringBuilder statement = new StringBuilder();

		// Create 3 nodes
		statement.append("MERGE (v1:VERTEX {description:\"an item\", rate:" + Math.floor(Math.random() * 10)
				+ ", percentage:" + Math.random() + "})\n");
		statement.append("MERGE (v2:VERTEX {description:\"an item\", rate:" + Math.floor(Math.random() * 10)
				+ ", percentage:" + Math.random() + "})\n");
		statement.append("MERGE (v3:VERTEX {description:\"an item\", rate:" + Math.floor(Math.random() * 10)
				+ ", percentage:" + Math.random() + "})\n");

		// relationship v1 --> v2
		statement.append("MERGE (v1)-[:EDGE {distance:" + Math.random() + "}]->(v2)\n");
		// relationship v2 --> v3
		statement.append("MERGE (v2)-[:EDGE {distance:" + Math.random() + "}]->(v3)");

		LOGGER.debug("Set up graph with statement: {}", statement.toString());

		AuthToken authToken = AuthTokens.basic("neo4j", "passsword");
		Config config = Config.build().withEncryptionLevel(EncryptionLevel.NONE).toConfig();

		Driver driver = GraphDatabase.driver(neo4jRule.boltURI(), authToken, config);

		embeddedSession = driver.session();
		embeddedSession.run(statement.toString());
	}

	@After
	public void tearDown() {
		if (embeddedSession.isOpen()) {
			embeddedSession.close();
		}
	}
}
