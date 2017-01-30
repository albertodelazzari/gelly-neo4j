package org.apache.flink.gelly.utilities.dataset;

import java.util.List;
import java.util.Map;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataPointGeneratorTest {

	private static final Logger LOGGER = LoggerFactory.getLogger(DataPointGeneratorTest.class);

	private static final int NUM_POINTS = 20;

	@Test
	public void testGeneratePointsWithNormalizedDistribution() {
		List<Tuple2<Long, Map<String, Object>>> points = DataPointGenerator
				.generatePointsWithNormalizedDistribution(NUM_POINTS);

		Assert.assertTrue(points.size() == NUM_POINTS);

		points.forEach(point -> {
			Assert.assertTrue(point.f0 <= NUM_POINTS);
			Assert.assertNotNull(point.f1);
			Assert.assertNotNull(point.f1.get("x"));
			Assert.assertNotNull(point.f1.get("y"));
		});

		LOGGER.debug("generated points: {}", points);
	}

	@Test
	public void testGenerateVerticesAsPointsWithNormalizedDistribution() {
		List<Vertex<Long, Map<String, Object>>> vertices = DataPointGenerator
				.generateVerticesAsPointsWithNormalizedDistribution(NUM_POINTS);

		Assert.assertTrue(vertices.size() == NUM_POINTS);

		vertices.forEach(vertex -> {
			Assert.assertTrue(vertex.f0 <= NUM_POINTS);
			Assert.assertNotNull(vertex.f1);
			Assert.assertNotNull(vertex.f1.get("x"));
			Assert.assertNotNull(vertex.f1.get("y"));
		});

		LOGGER.debug("generated vertices: {}", vertices);
	}

	@Test
	public void testcollectDistanceBetweenPoints() {
		List<Tuple3<Long, Long, Map<String, Object>>> distances = DataPointGenerator
				.collectDistanceBetweenPoints(DataPointGenerator.generatePointsWithNormalizedDistribution(NUM_POINTS));

		// It's like a two-dimensional matrix minus the diagonal elements
		Assert.assertTrue(distances.size() == NUM_POINTS * NUM_POINTS - NUM_POINTS);

		distances.forEach(distance1 -> {
			distances.forEach(distance2 -> {
				if (distance1.f0 == distance2.f1 && distance1.f1 == distance2.f0) {
					Assert.assertTrue(((Double) distance1.f2.get("distance"))
							.doubleValue() == ((Double) distance2.f2.get("distance")).doubleValue());
				}
			});
		});
	}

	@Test
	public void testCollectEdgesAsDistancesBetweenPoints() {
		List<Edge<Long, Map<String, Object>>> edges = DataPointGenerator.collectEdgesAsDistancesBetweenPoints(
				DataPointGenerator.generateVerticesAsPointsWithNormalizedDistribution(NUM_POINTS));

		// It's like a two-dimensional matrix minus the diagonal elements
		Assert.assertTrue(edges.size() == NUM_POINTS * NUM_POINTS - NUM_POINTS);

		edges.forEach(edge1 -> {
			edges.forEach(edge2 -> {
				if (edge1.f0 == edge2.f1 && edge1.f1 == edge2.f0) {
					Assert.assertTrue(((Double) edge1.f2.get("distance"))
							.doubleValue() == ((Double) edge2.f2.get("distance")).doubleValue());
				}
			});
		});
	}
}
