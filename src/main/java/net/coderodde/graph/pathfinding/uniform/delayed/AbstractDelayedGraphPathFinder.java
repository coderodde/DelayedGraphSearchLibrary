package net.coderodde.graph.pathfinding.uniform.delayed;

import java.util.List;

/**
 * This abstract class defines the API for algorithms searching for shortest, 
 * <b>unweighted</b> paths in a graph with slow ("<i>delayed</i>") node 
 * expansion operations.
 * 
 * @author Rodion "rodde" Efremov
 * @version 1.6 (Aug 4, 2016)
 * @param <N> the actual node type.
 */
public abstract class AbstractDelayedGraphPathFinder<N> {
    
    /**
     * Searches for a shortest unweighted path from {@code source} to 
     * {@code target}. If a path is found, returns the list of nodes that are 
     * ordered in the list in the same manner as they appear on a shortest path.
     * <p>
     * If {@code target} is not reachable from {@code source}, an empty list is
     * returned.
     * <p>
     * In case the graph to search is undirected (edges have no direction), the
     * client programmer may pass the same {@link AbstractNodeExpander} for both
     * {@code forwardSearchExpander} and {@code backwardSearchExpander}.
     * <p>
     * What comes to progress logging, {@code forwardSearchProgressLogger} will
     * log the progress for forward search direction only, 
     * {@code backwardSearchProgressLogger} will log the progress for backward
     * direction only, and {@code sharedSearchProgressLogger} will log 
     * everything related to the entire search such as beginning of the search,
     * and the result of it.
     * <p>
     * Any progress logger may be set to {@code null} so that the respective 
     * parts of progress will not be logged.
     * <p>
     * The following table summarizes
     * 
     * <p>
     * Since bidirectional search outperforms the unidirectional search, this
     * abstract class assumes that all implementing classes implement
     * bidirectional search, which is reflected in this very class. In a
     * bidirectional search, we run simultaneously two search frontiers: one 
     * in a normal fashion from the source node, and the another one in 
     * "opposite" direction starting from the target node. Note that if the 
     * graph is directed, the forward search traverses each edge from tail node
     * to head node, and the backward search traverses each node from head node
     * to tail node.
     * 
     * @param source                       the source node.
     * @param target                       the target node.
     * @param forwardSearchExpander        the expander generating all the 
     *                                     child nodes.
     * @param backwardSearchExpander       the expander generating all the 
     *                                     parent nodes.                           
     * @param forwardSearchProgressLogger  the forward search -related logger.
     * @param backwardSearchProgressLogger the backward search -related logger.
     * @param sharedSearchProgressLogger   the shared logger.
     * @return the shortest path as a list of nodes, or an empty list if the 
     *         target is not reachable from the source.
     */
    public abstract List<N> 
        search(final N source,
               final N target,
               final AbstractNodeExpander<N> forwardSearchExpander,
               final AbstractNodeExpander<N> backwardSearchExpander,
               final ProgressLogger<N> forwardSearchProgressLogger,
               final ProgressLogger<N> backwardSearchProgressLogger,
               final ProgressLogger<N> sharedSearchProgressLogger);
}