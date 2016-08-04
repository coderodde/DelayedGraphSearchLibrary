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
     * Expands the argument node, i.e., generates all the neighbors of the node
     * {@code node}.
     * 
     * @param node the node whose neighbors to generate.
     * @return the list of neighbor nodes.
     */
    public abstract List<N> expand(final N node);
}
