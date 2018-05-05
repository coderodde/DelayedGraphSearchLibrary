package net.coderodde.graph.pathfinding.uniform.delayed.support;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import net.coderodde.graph.pathfinding.uniform.delayed.AbstractDelayedGraphPathFinder;
import net.coderodde.graph.pathfinding.uniform.delayed.AbstractNodeExpander;
import net.coderodde.graph.pathfinding.uniform.delayed.ProgressLogger;

/**
 * This class implements a shortest path search in unweighted graphs, where 
 * expanding a node requires a substantial delay (tens of milliseconds). The 
 * idea of the algorithm is to deploy <code>n</code> threads for simply crawling
 * the graph from the source node forwards, and to have <code>n</code> threads
 * for crawling the graph from the target node backwards. Finally, only one 
 * thread is dedicated to performing the actual bidirectional path search, which
 * allows us to more easily to reason about the entire algorithm.
 * <p>
 * When the actual search thread tries to expand a node whose children/parents
 * are not known, the search thread is blocked until the data in question
 * becomes available.
 * 
 * @author Rodion "rodde" Efremov
 * @version 1.6 (Apr 6, 2018)
 * @param <N> the actual graph node type.
 */
public class LookAheadBidirectionalPathFinder<N> 
extends AbstractDelayedGraphPathFinder<N> {

    @Override
    public List<N> 
        search(final N source,
               final N target, 
               final AbstractNodeExpander<N> forwardSearchNodeExpander, 
               final AbstractNodeExpander<N> backwardSearchNodeExpander, 
               final ProgressLogger<N> forwardSearchProgressLogger, 
               final ProgressLogger<N> backwardSearchProgressLogger, 
               final ProgressLogger<N> sharedSearchProgressLogger) {
        return null;
    }
        
    private static final class CrawlerGraphDataHolder<N> {
        
        /**
         * Maps a node to the list of its neighbours. For the forward crawlers,
         * a node is mapped to its children. For the backward crawlers, a node
         * is mapped to its parents.
         */
        private final Map<N, List<N>> concurrentMap = new ConcurrentHashMap<>();
        
        public Map<N, List<N>> getNodeMap() {
            return concurrentMap;
        }
    }
        
    /**
     * This thread class is responsible for crawling the graph in the forward 
     * direction.
     * 
     * @param <N> the actual graph node type.
     */
    private static final class ForwardCrawlerThread<N> extends Thread {
        
        @Override
        public void run() {
            
        }
    }
    
    /**
     * This thread class is responsible for crawling the graph in the backward
     * direction.
     * 
     * @param <N> the actual graph node type.
     */
    private static final class BackwardCrawlerThread<N> extends Thread {
        
        @Override
        public void run() {
            
        }
    }
    
    private static final class ForwardGraphProvider<N> {
        
        public List<N> expand(N node) {
            return null;
        }
    }
    
    private static final class BackwardGraphProvider<N> {
        
        public List<N> expand(N node) {
            return null;
        }
        
    }
    
    private List<N> searchProcedure(
            final N source,
            final N target,
            final ForwardGraphProvider<N> forwardGraphProvider,
            final BackwardGraphProvider<N> backwardGraphProvider) {
        Deque<N> queueForward  = new ArrayDeque<>();
        Deque<N> queueBackward = new ArrayDeque<>();
        Map<N, N> parentsForward  = new HashMap<>();
        Map<N, N> parentsBackward = new HashMap<>();
        Map<N, Integer> distancesForward  = new HashMap<>();
        Map<N, Integer> distancesBackward = new HashMap<>();
        
        int bestCost = Integer.MAX_VALUE;
        N touchNode = null;
        
        while (!queueForward.isEmpty() && !queueBackward.isEmpty()) {
            int distanceForward = 
                    distancesForward.get(queueForward.getFirst());
            
            int distanceBackward = 
                    distancesBackward.get(queueBackward.getFirst());
            
            if (touchNode != null 
                    && bestCost < distanceForward + distanceBackward) {
                return tracebackPath(touchNode,
                                     parentsForward,
                                     parentsBackward);
            }
            
            if (distanceForward < distanceBackward) {
                N currentNode = queueForward.removeFirst();
                
                if (distancesBackward.containsKey(currentNode) 
                        && bestCost > distanceForward + distanceBackward) {
                    bestCost = distanceForward + distanceBackward;
                    touchNode = currentNode;
                }
                
                for (N childNode : forwardGraphProvider.expand(currentNode)) {
                    if (!distancesForward.containsKey(childNode)) {
                        distancesForward.put(
                                childNode,
                                distancesForward.get(currentNode) + 1);
                        
                        parentsForward.put(childNode, currentNode);
                        queueForward.addLast(childNode);
                    }
                }
            } else {
                N currentNode = queueBackward.removeFirst();
                
                if (distancesForward.containsKey(currentNode) 
                        && bestCost > distanceForward + distanceBackward) {
                    bestCost = distanceForward + distanceBackward;
                    touchNode = currentNode;
                }
                
                for (N parentNode : backwardGraphProvider.expand(currentNode)) {
                    if (!parentsBackward.containsKey(parentNode)) {
                        distancesBackward.put(
                                parentNode, 
                                distancesBackward.get(currentNode) + 1);
                        
                        parentsBackward.put(parentNode, currentNode);
                        queueBackward.addLast(parentNode);
                    }
                }
            }
            
            // Empty list denotes the situation where the target node is not 
            // reachable from the source node.
            return new ArrayList<>();
        }
        
        return null;
    }
    
    private static <N> List<N> tracebackPath(N touchNode,
                                             Map<N, N> parentsForward,
                                             Map<N, N> parentsBackward) {
        return null;
    }
}
