package net.coderodde.graph.pathfinding.uniform.delayed;

import java.util.List;

/**
 * This interface provides the API for anyone interested in the search progress
 * of the shortest path finders.
 * 
 * @author Rodion "rodde" Efremov
 * @version 1.6
 * @param <N> the actual node type.
 */
public class ProgressLogger<N> {

    /**
     * This method should be called whenever the search is initiated.
     * @param source the source node.
     * @param target the target node.
     */
    public void onBeginSearch(final N source, final N target) {}

    /**
     * This method should be called whenever the search expands {@code node}.
     * 
     * @param node the expanded node.
     */
    public void onExpansion(final N node) {}

    /**
     * This method should be called whenever the search is generating a neighbor
     * node of the being expanded.
     * 
     * @param node the generated neighbor node.
     */
    public void onNeighborGeneration(final N node) {}

    /**
     * This method should be called whenever the search has found a shortest 
     * path.
     * 
     * @param path the shortest path found. 
     */
    public void onShortestPath(final List<N> path) {}

    /**
     * This method should be called whenever the target node is not reachable 
     * from the source node and the search process must stop without finding a 
     * path.
     * 
     * @param source the requested source node.
     * @param target the requested target node.
     */
    public void onTargetUnreachable(final N source, final N target) {}
}
