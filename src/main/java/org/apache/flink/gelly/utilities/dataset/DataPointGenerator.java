package org.apache.flink.gelly.utilities.dataset;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.math3.ml.distance.EuclideanDistance;
import org.apache.commons.math3.random.GaussianRandomGenerator;
import org.apache.commons.math3.random.JDKRandomGenerator;
import org.apache.commons.math3.random.NormalizedRandomGenerator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Vertex;

/**
 * This class is a simple utility class used to test the Flink-Neo4j connector.
 * This class has two goals:
 * <ul>
 * <li>have some useful methods for big random data-set generation.</li>
 * <li>have some data-sets of two-dimensional points in order to test Flink and
 * Ne4J clustering algorithms.</li>
 * </ul>
 * 
 * This class uses the Apache Commons Math package
 * 
 * 
 * 
 * @author Alberto De Lazzari
 *
 */

public final class DataPointGenerator {

	private DataPointGenerator() {
		super();
	}

	/**
	 * 
	 * @param normalizedRandomGenerator
	 * @return
	 */
	private static Map<String, Object> generatePoint(NormalizedRandomGenerator normalizedRandomGenerator) {
		Map<String, Object> coordinates = new HashMap<>();
		coordinates.put("x", normalizedRandomGenerator.nextNormalizedDouble());
		coordinates.put("y", normalizedRandomGenerator.nextNormalizedDouble());

		coordinates.put("labels", Collections.singletonList("Point"));

		return coordinates;
	}

	/**
	 * 
	 * @param coordinatesMap
	 * @return
	 */
	private static double[] getCoordinatesAsArrayFromPropertyMap(Map<String, Object> coordinatesMap) {
		return new double[] { (Double) coordinatesMap.get("x"), (Double) coordinatesMap.get("y") };
	}

	/**
	 * Generate a labeled set of two-dimensional points with normalized
	 * distribution. The label is a simple long number starting from 1 to
	 * numOfPoints
	 * 
	 * @param numOfPoints
	 *            the number of points you want to generate
	 * @return a list of Tuple2 instances representing a set of DoublePoint
	 * 
	 * @see org.apache.commons.math3.random.GaussianRandomGenerator
	 */
	public static final List<Tuple2<Long, Map<String, Object>>> generatePointsWithNormalizedDistribution(
			int numOfPoints) {
		NormalizedRandomGenerator gaussianRandomGenerator = new GaussianRandomGenerator(new JDKRandomGenerator());

		List<Tuple2<Long, Map<String, Object>>> points = new ArrayList<>();

		for (long i = 1; i <= numOfPoints; i++) {
			points.add(new Tuple2<>(i, generatePoint(gaussianRandomGenerator)));
		}

		return points;
	}

	/**
	 * Generate a set of vertices (as Vertex class instances) with normalized
	 * distribution. The id of each vertex is generated starting from 1 to
	 * numOfPoints.
	 * 
	 * @param numOfPoints
	 *            the number of point you want to generate
	 * @return
	 * 
	 * @see org.apache.commons.math3.random.GaussianRandomGenerator
	 */
	public static final List<Vertex<Long, Map<String, Object>>> generateVerticesAsPointsWithNormalizedDistribution(
			int numOfPoints) {
		GaussianRandomGenerator gaussianRandomGenerator = new GaussianRandomGenerator(new JDKRandomGenerator());

		List<Vertex<Long, Map<String, Object>>> vertices = new ArrayList<>();

		for (long i = 1; i <= numOfPoints; i++) {
			vertices.add(new Vertex<>(i, generatePoint(gaussianRandomGenerator)));
		}

		return vertices;
	}

	/**
	 * Collect all the distances for a given list of labeled points. Each
	 * element of the list is a Tuple3 object where the first two fields (f0 and
	 * f1) will be the labels of the points for whom the distance has been
	 * computed, the latter field (f2) will be the distance itself.
	 * 
	 * @param points
	 * @return
	 * 
	 * @see org.apache.commons.math3.ml.clustering.DoublePoint
	 * @see org.apache.commons.math3.ml.distance.EuclideanDistance
	 */
	public static final List<Tuple3<Long, Long, Map<String, Object>>> collectDistanceBetweenPoints(
			final List<Tuple2<Long, Map<String, Object>>> points) {

		List<Tuple3<Long, Long, Map<String, Object>>> distances = new ArrayList<>();
		EuclideanDistance euclideanDistance = new EuclideanDistance();

		// This is will be a matrix with null diagonal
		points.forEach(point1 -> {
			points.forEach(point2 -> {
				if (point1.f0 != point2.f0) {
					double distance = euclideanDistance.compute(getCoordinatesAsArrayFromPropertyMap(point1.f1),
							getCoordinatesAsArrayFromPropertyMap(point2.f1));

					Map<String, Object> distanceProperty = new HashMap<>();
					distanceProperty.put("distance", distance);
					distanceProperty.put("type", "Distance");

					distances.add(new Tuple3<>(point1.f0, point2.f0, distanceProperty));
				}
			});
		});

		return distances;
	}

	/**
	 * @param vertices
	 * @return
	 * 
	 * @see org.apache.commons.math3.ml.clustering.DoublePoint
	 * @see org.apache.commons.math3.ml.distance.EuclideanDistance
	 */
	public static final List<Edge<Long, Map<String, Object>>> collectEdgesAsDistancesBetweenPoints(
			final List<Vertex<Long, Map<String, Object>>> vertices) {

		List<Edge<Long, Map<String, Object>>> edges = new ArrayList<>();
		EuclideanDistance euclideanDistance = new EuclideanDistance();

		vertices.forEach(vertex1 -> {
			vertices.forEach(vertex2 -> {
				if (vertex1.f0 != vertex2.f0) {
					double distance = euclideanDistance.compute(getCoordinatesAsArrayFromPropertyMap(vertex1.f1),
							getCoordinatesAsArrayFromPropertyMap(vertex2.f1));

					Map<String, Object> distanceProperty = new HashMap<>();
					distanceProperty.put("distance", distance);
					distanceProperty.put("type", "Distance");

					edges.add(new Edge<>(vertex1.f0, vertex2.f0, distanceProperty));
				}
			});
		});

		return edges;
	}
}
