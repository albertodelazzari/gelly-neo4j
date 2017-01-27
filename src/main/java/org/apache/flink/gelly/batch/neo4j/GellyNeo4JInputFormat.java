/**
 * 
 */
package org.apache.flink.gelly.batch.neo4j;

import java.util.Collection;
import java.util.Map;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.batch.neo4j.Neo4JInputFormat;
import org.apache.flink.gelly.mapping.neo4j.GellyNeo4JSerializationDefaultMappingStrategy;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Vertex;

/**
 * @author Alberto De Lazzari
 *
 */
public class GellyNeo4JInputFormat<V extends Map<String, Object>, E extends Map<String, Object>>
		extends Neo4JInputFormat<Tuple2<Collection<Vertex<Long, V>>, Collection<Edge<Long, E>>>> {

	private static final long serialVersionUID = 1L;

	/**
	 * @param mappingStrategy
	 * @param config
	 */
	public GellyNeo4JInputFormat(GellyNeo4JSerializationDefaultMappingStrategy<V, E> mappingStrategy,
			final Map<String, String> config) {
		super(mappingStrategy, config);
	}

}