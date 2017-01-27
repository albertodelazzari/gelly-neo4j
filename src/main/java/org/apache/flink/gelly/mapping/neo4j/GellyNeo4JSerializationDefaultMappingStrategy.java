/**
 * 
 */
package org.apache.flink.gelly.mapping.neo4j;

import java.util.Map;

/**
 * This class defines a serialization strategy from a Flink DataSet to Neo4J
 * entities, establishing a bound for the type of vertices and edges. A vertex
 * must be an object with Long id and a set of properties (a Map<String, Object>
 * instance) and the edge must be an similar object.
 * 
 * @author Alberto De Lazzari
 *
 */
public class GellyNeo4JSerializationDefaultMappingStrategy<V extends Map<String, Object>, E extends Map<String, Object>>
		extends GellyNeo4JSerializationMappingStrategy<V, E> {

	private static final long serialVersionUID = 1L;

	public GellyNeo4JSerializationDefaultMappingStrategy(String templateStatement,
			GellySerializationDefaultMapper<V, E> mapper) {
		super(templateStatement, mapper);
	}
}
