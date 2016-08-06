package net.coderodde.graph.pathfinding.uniform.delayed.support;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import net.coderodde.graph.pathfinding.uniform.delayed.AbstractNodeExpander;
import org.junit.Test;
import static org.junit.Assert.*;

public class ThreadPoolBidirectionalPathFinderTest {
    
    // This test makes sure that the thread pool bidirectional path finder 
    // computes the path of the same length as a simple BFS. Also, this test
    // demonstrates that the thread pool version is inferior in the problem 
    // setting in which node expansion works fast.
    @Test
    public void testSearch() {
        final long seed = System.nanoTime();
        final Random random = new Random(seed);
        final List<DirectedGraphNode> graph = createRandomGraph(5000, 
                                                                30000, 
                                                                random);
        
        final DirectedGraphNode source = 
                graph.get(random.nextInt(graph.size()));
        
        final DirectedGraphNode target = 
                graph.get(random.nextInt(graph.size()));
        
        long startTime = System.nanoTime();
        final List<DirectedGraphNode> actualPath = bfs(source, target);
        long endTime = System.nanoTime();
        
        System.out.println("BFS in " + (endTime - startTime) / 1e6 + 
                " milliseconds.");
        
        startTime = System.nanoTime();
        final List<DirectedGraphNode> testPath =
                new ThreadPoolBidirectionalPathFinder<DirectedGraphNode>(16)
                .search(source, 
                        target, 
                        new ForwardNodeExpander(), 
                        new BackwardNodeExpander(),
                        null, 
                        null, 
                        null);
        endTime = System.nanoTime();
        
        System.out.println("ThreadPool in " + (endTime - startTime) / 1e6 + 
                " milliseconds.");
        
        System.out.println("BFS path length: " + actualPath.size());
        System.out.println("ThreadPool path length: " + testPath.size());
        
        assertEquals(actualPath.size(), testPath.size());
    }
    
    static class ForwardNodeExpander 
    extends AbstractNodeExpander<DirectedGraphNode> {

        @Override
        public List<DirectedGraphNode> expand(DirectedGraphNode node) {
            return new ArrayList<>(node.getChildren());
        }

        @Override
        public boolean isValidNode(DirectedGraphNode node) {
            return node != null;
        }
    }
    
    static class BackwardNodeExpander 
    extends AbstractNodeExpander<DirectedGraphNode> {

        @Override
        public List<DirectedGraphNode> expand(DirectedGraphNode node) {
            return new ArrayList<>(node.getParents());
        }

        @Override
        public boolean isValidNode(DirectedGraphNode node) {
            return node != null;
        }
    }
    
    static List<DirectedGraphNode> bfs(final DirectedGraphNode source,
                                       final DirectedGraphNode target) {
        final Deque<DirectedGraphNode> queue = new ArrayDeque<>();
        final Map<DirectedGraphNode, DirectedGraphNode> parents = 
                new HashMap<>();
        
        queue.addLast(source);
        parents.put(source, null);
        
        while (!queue.isEmpty()) {
            final DirectedGraphNode current = queue.removeFirst();
            
            if (current.equals(target)) {
                final List<DirectedGraphNode> path = new ArrayList<>();
                DirectedGraphNode node = target;
                
                while (node != null) {
                    path.add(node);
                    node = parents.get(node);
                }
                
                Collections.reverse(path);
                return path;
            }
            
            for (final DirectedGraphNode child : current.getChildren()) {
                if (!parents.containsKey(child)) {
                    parents.put(child, current);
                    queue.addLast(child);
                }
            }
        }
        
        return new ArrayList<>();
    }
    
    private static final class DirectedGraphNode {
        
        private final int id;
        private final Set<DirectedGraphNode> children = new HashSet<>();
        private final Set<DirectedGraphNode> parents  = new HashSet<>();
        
        DirectedGraphNode(final int id) {
            this.id = id;
        }
        
        public void connectTo(final DirectedGraphNode node) {
            children.add(node);
            node.parents.add(this);
        }
        
        public Set<DirectedGraphNode> getChildren() {
            return children;
        }
        
        public Set<DirectedGraphNode> getParents() {
            return parents;
        }
        
        @Override
        public int hashCode() {
            return id;
        }
        
        @Override
        public boolean equals(final Object o) {
            if (o == null) {
                return false;
            }
            
            if (!getClass().equals(o.getClass())) {
                return false;
            }
            
            return id == ((DirectedGraphNode) o).id;
        }
    }
    
    static class Edge {
        int head;
        int tail;
        
        Edge(int head, int tail) {
            this.head = head;
            this.tail = tail;
        }
    }
    
    private static List<DirectedGraphNode> 
        createRandomGraph(final int nodes, 
                          final int edges, 
                          final Random random) {
        final List<DirectedGraphNode> graph = new ArrayList<>(nodes);
        
        for (int id = 0; id < nodes; ++id) {
            graph.add(new DirectedGraphNode(id));
        }
        
        final List<Edge> edgeList = new ArrayList<>(nodes * nodes);
        
        for (int id1 = 0; id1 < nodes; ++id1) {
            for (int id2 = 0; id2 < nodes; ++id2) {
                edgeList.add(new Edge(id1, id2));
            }
        }
        
        Collections.shuffle(edgeList, random);
        
        for (int i = 0; i < Math.min(edges, edgeList.size()); ++i) {
            final Edge edge = edgeList.get(i);
            graph.get(edge.tail).connectTo(graph.get(edge.head));
        }
        
        return graph;
    }
}
