/**
 * 
 */
package org.apache.flink.gelly.mapping.neo4j;

import java.util.Collection;
import java.util.Map;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Vertex;

/**
 * This class defines a deserialization strategy from Neo4J entities to a Flink
 * DataSet, establishing a bound for the type of vertices and edges. A vertex
 * must be an object with Long id and a set of properties (a Map<String, Object>
 * instance) and the edge must be an similar object.
 * 
 * @author Alberto De Lazzari
 *
 */
public class GellyNeo4JDeserializationDefaultMappingStrategy<V extends Map<String, Object>, E extends Map<String, Object>>
		extends GellyNeo4JDeserializationMappingStrategy<V, E> {

	private static final long serialVersionUID = 1L;
	
	private static final String LABEL_KEY = "labels";

	public GellyNeo4JDeserializationDefaultMappingStrategy(String templateStatement,
			GellyDeserializationDefaultMapper<V, E> mapper) {
		super(templateStatement, mapper);
	}

	/**
	 * This method is similar to the other constructor but it accepts an item
	 * instead of a query statement. The item is used to create a default import
	 * statement
	 * 
	 * @param item
	 * @param mapper
	 */
	public GellyNeo4JDeserializationDefaultMappingStrategy(
			Tuple2<Collection<Vertex<Long, V>>, Collection<Edge<Long, E>>> item,
			GellyDeserializationDefaultMapper<V, E> mapper) {
		super(null, mapper);
		setStatement(createDefaultImportTemplateStatement(item));
	}

	/**
	 * Create a default cypher query template from a Flink Tuple2 (that is a
	 * representation of a Graph)
	 * 
	 * @return the cypher query template that will be used to insert the
	 *         entities into Neo4J
	 */
	@SuppressWarnings("unchecked")
	private String createDefaultImportTemplateStatement(
			Tuple2<Collection<Vertex<Long, V>>, Collection<Edge<Long, E>>> item) {
		Collection<Vertex<Long, V>> vertices = item.f0;
		Collection<Edge<Long, E>> edges = item.f1;

		StringBuilder builder = new StringBuilder();
		builder.append("UNWIND {nodes} as node\n");
		builder.append("CREATE (n)\n");
		builder.append("SET n = node, ");

		Object labels = vertices.iterator().next().f1.get(LABEL_KEY);
		StringBuilder labelsDefinition = new StringBuilder();
		if (labels != null) {
			builder.append("n");
			((Collection<String>) labels).forEach(label -> labelsDefinition.append(":" + label));
			builder.append(labelsDefinition.toString());
			builder.append(", ");
		}
		// TODO: should throw an error if there isn't at least a label
		
		builder.append("n.labels = null\n");
		builder.append("WITH n\n");

		builder.append("UNWIND {relationships} as relationship\n");
		builder.append("MATCH (n {id: relationship.startNode}), (v2" + labelsDefinition.toString()
				+ " {id: relationship.endNode})\n");
		builder.append("MERGE (n)-[r:");

		// TODO: There should be always a type otherwise throw an error
		Object type = edges.iterator().next().f2.get("type");
		builder.append(type);

		builder.append("]->(v2)\n");
		builder.append("SET r = relationship, ");
		// Null all the property that will be useless after creation
		builder.append("r.type = null, r.startNode = null, r.endNode = null\n");
		builder.append("return r, n, v2");

		return builder.toString();
	}
}