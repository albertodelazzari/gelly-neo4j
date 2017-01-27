/**
 * 
 */
package org.apache.flink.gelly.utilities;

import java.util.Collection;
import java.util.Map;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.gelly.batch.neo4j.GellyNeo4JInputFormat;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Vertex;

import de.javakaffee.kryoserializers.UnmodifiableCollectionsSerializer;

/**
 * @author Alberto De Lazzari
 *
 */
public final class ExecutionEnvironmentUtils {

	private static final String UNMODIFIABLE_COLLECTION_CLASS_NAME = "java.util.Collections$UnmodifiableCollection";

	private static final String UNMODIFIABLE_MAP_CLASS_NAME = "java.util.Collections$UnmodifiableMap";

	private ExecutionEnvironmentUtils() {
		super();
	}

	/**
	 * Set a bunch of useful Kryo serializers.</br>
	 * Add support for:
	 * <ul>
	 * <li>UnmodifiableCollection</li>
	 * <li>UnmodifiableMap</li>
	 * </ul>
	 * in the java.util.Collections class
	 * 
	 * @see java.util.Collections
	 * 
	 * @param executionEnvironment
	 *            the current execution environment
	 * @return
	 * @throws ClassNotFoundException
	 */
	public static ExecutionEnvironment setDefaultSerializers(ExecutionEnvironment executionEnvironment)
			throws ClassNotFoundException {
		Class<?> unmodCollection = Class.forName(UNMODIFIABLE_COLLECTION_CLASS_NAME);
		Class<?> unmodMap = Class.forName(UNMODIFIABLE_MAP_CLASS_NAME);
		executionEnvironment.addDefaultKryoSerializer(unmodCollection, UnmodifiableCollectionsSerializer.class);
		executionEnvironment.addDefaultKryoSerializer(unmodMap, UnmodifiableCollectionsSerializer.class);

		return executionEnvironment;
	}

	/**
	 * Returns the default TypeInformation for a Tuple2 consisting of a list of
	 * vertices and a list of edges. This TypeInformation should be used by
	 * GellyNeo4JInputFormat
	 * 
	 * @see GellyNeo4JInputFormat
	 * 
	 * @return a TypeInformation object that Flink uses to serialize a Tuple2 of
	 *         vertices and edges
	 */
	public static TypeInformation<Tuple2<Collection<Vertex<Long, Map<String, Object>>>, Collection<Edge<Long, Map<String, Object>>>>> typeInformationOfVerticesAndEdges() {
		return TypeInformation.of(
				new TypeHint<Tuple2<Collection<Vertex<Long, Map<String, Object>>>, Collection<Edge<Long, Map<String, Object>>>>>() {
				});
	}
}