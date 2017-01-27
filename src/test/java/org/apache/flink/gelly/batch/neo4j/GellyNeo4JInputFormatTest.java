package org.apache.flink.gelly.batch.neo4j;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.gelly.embedded.neo4j.Neo4JBaseEmbeddedConfig;
import org.apache.flink.gelly.mapping.neo4j.GellyNeo4JSerializationDefaultMappingStrategy;
import org.apache.flink.gelly.mapping.neo4j.GellySerializationDefaultMapper;
import org.apache.flink.gelly.utilities.ExecutionEnvironmentUtils;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.EdgeDirection;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.ReduceEdgesFunction;
import org.apache.flink.graph.Vertex;
import org.apache.flink.hadoop.shaded.com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;
import org.neo4j.driver.internal.value.NodeValue;
import org.neo4j.driver.internal.value.RelationshipValue;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.StatementResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GellyNeo4JInputFormatTest extends Neo4JBaseEmbeddedConfig {

	private static final Logger LOGGER = LoggerFactory.getLogger(GellyNeo4JInputFormatTest.class);

	@Test
	public void testInputFormat() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();

		ExecutionEnvironmentUtils.setDefaultSerializers(env);

		GellySerializationDefaultMapper<Map<String, Object>, Map<String, Object>> defaultMapper = new GellySerializationDefaultMapper<>();

		// We match a simple path v1 --> v2
		String statement = "MATCH path = (v1:VERTEX)-[e:EDGE]->(v2:VERTEX) return path";
		GellyNeo4JSerializationDefaultMappingStrategy<Map<String, Object>, Map<String, Object>> defaultMappingStrategy = new GellyNeo4JSerializationDefaultMappingStrategy<>(
				statement, defaultMapper);

		GellyNeo4JInputFormat<Map<String, Object>, Map<String, Object>> inputFormat = new GellyNeo4JInputFormatMock(
				defaultMappingStrategy, neo4JConfig);

		DataSource<Tuple2<Collection<Vertex<Long, Map<String, Object>>>, Collection<Edge<Long, Map<String, Object>>>>> dataSource = env
				.createInput(inputFormat, ExecutionEnvironmentUtils.typeInformationOfVerticesAndEdges());

		List<Tuple2<Collection<Vertex<Long, Map<String, Object>>>, Collection<Edge<Long, Map<String, Object>>>>> items = Lists
				.newArrayList();
		dataSource
				.output(new LocalCollectionOutputFormat<Tuple2<Collection<Vertex<Long, Map<String, Object>>>, Collection<Edge<Long, Map<String, Object>>>>>(
						items));

		env.execute();

		// Iterate for each sub-graph that it has been extracted by the cypher
		// query above
		for (Tuple2<Collection<Vertex<Long, Map<String, Object>>>, Collection<Edge<Long, Map<String, Object>>>> item : items) {
			Collection<Vertex<Long, Map<String, Object>>> vertices = item.f0;
			Collection<Edge<Long, Map<String, Object>>> edges = item.f1;

			LOGGER.debug("Vertices: {}", vertices);
			LOGGER.debug("Edges: {}", edges);

			// We should have (for each sub-graph) 2 vertices and an edge
			// connecting them
			Assert.assertTrue(vertices.size() == 2);
			Assert.assertTrue(edges.size() == 1);

			// Matching vertices by rate property
			vertices.forEach(v -> {
				// We should have a record
				StatementResult result = embeddedSession.run("MATCH (n {rate:" + v.f1.get("rate") + "}) return n");
				Assert.assertTrue(result.hasNext());

				// We should match only a result
				Record record = result.next();
				Assert.assertTrue(record.values().size() == 1);

				// The node should have a label named "VERTEX"
				NodeValue node = (NodeValue) record.values().get(0);
				Assert.assertTrue(node.asNode().labels().iterator().next().equals("VERTEX"));
			});

			// Matching edges by distance property
			edges.forEach(e -> {
				StatementResult result = embeddedSession
						.run("MATCH (n)-[r {distance:" + e.f2.get("distance") + "}]-(m) return r");
				Assert.assertTrue(result.hasNext());

				// We should match only a result
				Record record = result.next();
				Assert.assertTrue(record.values().size() == 1);

				// The relationship should have a type named "EDGE"
				RelationshipValue relationship = (RelationshipValue) record.values().get(0);
				Assert.assertTrue(relationship.asRelationship().type().equals("EDGE"));
			});
		}

		/**
		 * Here we do some silly graph computation
		 */
		Graph<Long, Map<String, Object>, Map<String, Object>> graph = Graph.fromCollection(vertices, edges, env);

		// We want to select the incoming edge with minimum distance
		DataSet<Tuple2<Long, Map<String, Object>>> minDistances = graph.reduceOnEdges(new MinDistanceReduceFunction(),
				EdgeDirection.IN);

		// Only vertices v2 and v3 have an incoming edge, v1 has not
		final List<Long> v2v3 = Lists.newArrayList(new Long(2), new Long(3));
		minDistances.collect().forEach(tuple -> {
			Long vertexId = tuple.f0;
			
			LOGGER.debug("Vertex {} has an incoming edge with min distance", vertexId);
			Assert.assertTrue(v2v3.contains(vertexId));
		});

		// We have 3 vertices but only two of them have an incoming edge
		Assert.assertTrue(minDistances.count() == 2);
	}

	private static final class MinDistanceReduceFunction implements ReduceEdgesFunction<Map<String, Object>> {

		private static final long serialVersionUID = 1L;

		@Override
		public Map<String, Object> reduceEdges(Map<String, Object> firstEdgeValue,
				Map<String, Object> secondEdgeValue) {

			double d1 = (double) firstEdgeValue.get("distance");
			double d2 = (double) secondEdgeValue.get("distance");

			return d1 < d2 ? firstEdgeValue : secondEdgeValue;
		}

	}
}
