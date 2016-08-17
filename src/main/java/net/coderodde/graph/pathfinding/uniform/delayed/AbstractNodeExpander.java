package net.coderodde.graph.pathfinding.uniform.delayed;

import java.util.List;

/**
 * This abstract class defines the API for the subclasses that generate all the
 * neighbors of a given node ("<i>expand a node</i>").
 * 
 * @author Rodion "rodde" Efremov
 * @version 1.6 (Aug 4, 2016)
 * @param <N> the actual node type.
 */
public abstract class AbstractNodeExpander<N> {

    /**
     * Expands the argument node, or namely, generates all the neighbors of the 
     * node {@code node}.
     * 
     * @param node the node whose neighbors to generate.
     * @return the list of neighbor nodes or {@code null} if the node 
     *         {@code node} is invalid.
     */
    public abstract List<N> expand(final N node);

    /**
     * Checks that the input node {@code node} is a valid node in the graph.
     * 
     * @param node the node to check.
     * @return {@code true} only if {@code node} is a valid node.
     */
    public abstract boolean isValidNode(final N node);
}
