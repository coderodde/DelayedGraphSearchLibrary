package net.coderodde.graph.pathfinding.uniform.delayed.support;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import net.coderodde.graph.pathfinding.uniform.delayed.AbstractDelayedGraphPathFinder;
import net.coderodde.graph.pathfinding.uniform.delayed.AbstractNodeExpander;
import net.coderodde.graph.pathfinding.uniform.delayed.ProgressLogger;

/**
 * This class implements a parallel, bidirectional breadth-first search in order
 * to find an unweighted shortest path from a given source node to a given 
 * target node. The underlying algorithm is the bidirectional breadth-first
 * search. However, multiple threads may work on a single search direction in
 * order to speed up the computation: for each search direction (forward and 
 * backward), the algorithm maintains concurrent state, such as the frontier 
 * queue; many threads may pop the queue, expand the node and append the 
 * neighbors to that queue.
 * 
 * @author Rodion "rodde" Efremov
 * @version 1.6 (Aug 4, 2016)
 * @param <N> the actual graph node type.
 */
public class ThreadPoolBidirectionalPathFinder<N> 
extends AbstractDelayedGraphPathFinder<N> {

    /**
     * The number of milliseconds a slave thread sleeps when the queue is emtpy.
     */
    private static final int DEFAULT_SLAVE_THREAD_SLEEP_DURATION = 20;
    
    private final int threadsPerSearchDirection;
    
    /**
     * The duration of sleeping in milliseconds.
     */
    private final int sleepDuration;
    
    public ThreadPoolBidirectionalPathFinder(
            final int threadsPerSearchDirection,
            final int sleepDuration) {
        this.threadsPerSearchDirection = 
                Math.max(threadsPerSearchDirection, 1);
        this.sleepDuration = Math.max(sleepDuration, 1);
    }
    
    public ThreadPoolBidirectionalPathFinder(
            final int threadsPerSearchDirection) {
        this(threadsPerSearchDirection, DEFAULT_SLAVE_THREAD_SLEEP_DURATION);
    }
    
    /**
     * {@inheritDoc }
     */
    @Override
    public List<N> 
        search(final N source, 
               final N target, 
               final AbstractNodeExpander<N> forwardSearchNodeExpander, 
               final AbstractNodeExpander<N> backwardSearchNodeExpander, 
               final ProgressLogger<N> forwardSearchProgressLogger, 
               final ProgressLogger<N> backwardSearchProgressLogger, 
               final ProgressLogger<N> sharedSearchProgressLogger) {
        Objects.requireNonNull(forwardSearchNodeExpander, 
                               "The forward search node expander is null.");
        
        Objects.requireNonNull(backwardSearchNodeExpander,
                               "The backward search node expander is null.");
        
        if (!forwardSearchNodeExpander.isValidNode(source)) {
            throw new IllegalArgumentException(
                    "The source node (" + source + ") was rejected by the " +
                    "forward search node expander.");
        }
        
        if (!backwardSearchNodeExpander.isValidNode(target)) {
            throw new IllegalArgumentException(
                    "The target node (" + target + ") was rejected by the " +
                    "backward search node expander.");
        }
        
        if (sharedSearchProgressLogger != null) {
            sharedSearchProgressLogger.onBeginSearch(source, target);
        }
        
        this.duration = System.currentTimeMillis();
        
        // Create the state object shared by all the threads working on forward
        // direction:
        final SearchState<N> forwardSearchState  = 
                new SearchState<>(source, threadsPerSearchDirection);
        
        // Create the state object shared by all the threads working on backward
        // direction:
        final SearchState<N> backwardSearchState = 
                new SearchState<>(target, threadsPerSearchDirection);
        
        // Create the state object shared by both the search direction:
        final SharedSearchState<N> sharedSearchState = 
                new SharedSearchState(source, 
                                      target, 
                                      forwardSearchState,
                                      backwardSearchState,
                                      sharedSearchProgressLogger);
        
        final ForwardSearchThread[] forwardSearchThreads =
                new ForwardSearchThread[threadsPerSearchDirection];
        
        // Below, the value of 'sleepDuration' since the thread being created
        // is a master thread that never sleeps.
        forwardSearchThreads[0] = 
                new ForwardSearchThread(0, 
                                        forwardSearchNodeExpander,
                                        forwardSearchState,
                                        sharedSearchState,
                                        true,
                                        forwardSearchProgressLogger,
                                        sleepDuration);
        
        forwardSearchState.introduceThread(forwardSearchThreads[0]);
        forwardSearchThreads[0].start();
        
        for (int i = 1; i < threadsPerSearchDirection; ++i) {
            forwardSearchThreads[i] = 
                    new ForwardSearchThread(i,
                                            forwardSearchNodeExpander,
                                            forwardSearchState,
                                            sharedSearchState,
                                            false,
                                            forwardSearchProgressLogger,
                                            sleepDuration);
            
            forwardSearchState.introduceThread(forwardSearchThreads[i]);
            forwardSearchThreads[i].start();
        }
            
        final BackwardSearchThread[] backwardSearchThreads =
                new BackwardSearchThread[threadsPerSearchDirection];
        
        // Below, the value of 'sleepDuration' since the thread being created
        // is a master thread that never sleeps.
        backwardSearchThreads[0] = 
                new BackwardSearchThread(forwardSearchThreads.length,
                                         backwardSearchNodeExpander,
                                         backwardSearchState,
                                         sharedSearchState,
                                         true,
                                         backwardSearchProgressLogger,
                                         sleepDuration);
        
        backwardSearchState.introduceThread(backwardSearchThreads[0]);
        backwardSearchThreads[0].start();
        
        for (int i = 1; i < threadsPerSearchDirection; ++i) {
            backwardSearchThreads[i] = 
                    new BackwardSearchThread(forwardSearchThreads.length + i,
                                             backwardSearchNodeExpander,
                                             backwardSearchState,
                                             sharedSearchState,
                                             false,
                                             backwardSearchProgressLogger,
                                             sleepDuration);
            
            backwardSearchState.introduceThread(backwardSearchThreads[i]);
            backwardSearchThreads[i].start();
        }
        
        try {
            for (final ForwardSearchThread thread : forwardSearchThreads) {
                thread.join();
            }
        } catch (final InterruptedException ex) {
            throw new IllegalStateException("The forward thread threw " +
                    ex.getClass().getSimpleName() + ": " +
                    ex.getMessage(), ex);
        }
        
        try {
            for (final BackwardSearchThread thread : backwardSearchThreads) {
                thread.join();
            }
        } catch (final InterruptedException ex) {
            throw new IllegalStateException("The backward thread threw " +
                    ex.getClass().getSimpleName() + ": " +
                    ex.getMessage(), ex);
        }
        
        this.duration = System.currentTimeMillis() - this.duration;
        this.numberOfExpandedNodes = 0;
        
        for (final ForwardSearchThread thread : forwardSearchThreads) {
            this.numberOfExpandedNodes += thread.getNumberOfExpandedNodes();
        }
        
        for (final BackwardSearchThread thread : backwardSearchThreads) {
            this.numberOfExpandedNodes += thread.getNumberOfExpandedNodes();
        }
        
        return sharedSearchState.getPath();
    }
    
    /**
     * This class holds the state shared by the two search directions.
     */
    private static final class SharedSearchState<N> {
        
        /**
         * The source node.
         */
        private final N source;
        
        /**
         * The target node. 
         */
        private final N target;
        
        /**
         * The state of all the forward search threads.
         */
        private SearchState<N> forwardSearchState;
        
        /**
         * The state of all the backward search threads.
         */
        private SearchState<N> backwardSearchState;
        
        /**
         * Caches the best known length from the source to the target nodes.
         */
        private volatile int bestPathLengthSoFar = Integer.MAX_VALUE;
        
        /**
         * The best search frontier touch node so far.
         */
        private volatile N touchNode;
        
        /**
         * Caches whether the shortest path was found.
         */
        private boolean pathIsFound;
        
        /**
         * The progress logger for reporting the progress.
         */
        private final ProgressLogger<N> sharedProgressLogger;
        
        SharedSearchState(final N source,
                          final N target,
                          final SearchState<N> forwardSearchState,
                          final SearchState<N> backwardSearchState, 
                          final ProgressLogger<N> sharedProgressLogger) {
            this.source = source;
            this.target = target;
            this.forwardSearchState   = forwardSearchState;
            this.backwardSearchState  = backwardSearchState;
            this.sharedProgressLogger = sharedProgressLogger;
        }
        
        synchronized void updateFromForwardDirection(final N current) {
            if (backwardSearchState.getDistanceMap().containsKey(current)
                    && forwardSearchState.getDistanceMap()
                                         .containsKey(current)) {
                final int currentDistance = 
                        forwardSearchState .getDistanceMap().get(current) +
                        backwardSearchState.getDistanceMap().get(current);
                
                if (bestPathLengthSoFar > currentDistance) {
                    bestPathLengthSoFar = currentDistance;
                    touchNode = current;
                }
            }
        }
        
        synchronized void updateFromBackwardDirection(final N current) {
            if (forwardSearchState.getDistanceMap().containsKey(current)
                    && backwardSearchState.getDistanceMap()
                                          .containsKey(current)) {
                final int currentDistance = 
                        forwardSearchState .getDistanceMap().get(current) +
                        backwardSearchState.getDistanceMap().get(current);
                
                if (bestPathLengthSoFar > currentDistance) {
                    bestPathLengthSoFar = currentDistance;
                    touchNode = current;
                }
            }
        }
        
        synchronized boolean pathIsOptimal(final N node) {
            if (touchNode == null) {
                // Once here, the two search trees did not meet each other yet.
                return false;
            }
            
            if (!forwardSearchState.getDistanceMap().containsKey(node)) {
                // The forward search did not reach the node 'node' yet.
                return false;
            }
            
            if (!backwardSearchState.getDistanceMap().containsKey(node)) {
                // The backward search did not reach the node 'node' yet.
                return false;
            }
            
            final int distance = 
                    forwardSearchState .getDistanceMap().get(node) +
                    backwardSearchState.getDistanceMap().get(node);
            
            if (distance > bestPathLengthSoFar) {
                forwardSearchState .requestThreadsToExit();
                backwardSearchState.requestThreadsToExit();
                pathIsFound = true;
                
                return true;
            }
            
            return false;
        }
        
        synchronized void requestExit() {
            forwardSearchState .requestThreadsToExit();
            backwardSearchState.requestThreadsToExit();
        }
        
        synchronized List<N> getPath() {
            if (!pathIsFound) {
                if (sharedProgressLogger != null) {
                    sharedProgressLogger.onTargetUnreachable(source, target);
                }
                
                return new ArrayList<>();
            }
            
            final ConcurrentMapWrapper<N, N> parentMapForward = 
                    forwardSearchState.getParentMap();
            
            final ConcurrentMapWrapper<N, N> parentMapBackward = 
                    backwardSearchState.getParentMap();
            
            final List<N> path = new ArrayList<>();
            
            N current = touchNode;
            
            while (current != null) {
                path.add(current);
                current = parentMapForward.get(current);
            }
            
            Collections.<String>reverse(path);
            current = parentMapBackward.get(touchNode);
            
            while (current != null) {
                path.add(current);
                current = parentMapBackward.get(current);
            }

            if (sharedProgressLogger != null) {
                sharedProgressLogger.onShortestPath(path);
            }
            
            return path;
        }
    }
    
    /**
     * This class holds all the state of a single search direction.
     */
    private static final class SearchState<N> {
        
        /**
         * This FIFO queue contains the queue of nodes reached but not yet 
         * expanded. It is called the <b>search frontier</b>.
         */
        private final ConcurrentQueueWrapper<N> queue = 
                new ConcurrentQueueWrapper(new ArrayDeque<>());
        
        /**
         * This map maps each discovered node to its predecessor on the shortest 
         * path.
         */
        private final ConcurrentMapWrapper<N, N> parents = 
                new ConcurrentMapWrapper<>();
        
        /**
         * This map maps each discovered node to its best distance from the 
         * start node so far.
         */
        private final ConcurrentMapWrapper<N, Integer> distance =
                new ConcurrentMapWrapper<>();
        
        /**
         * Caches the total number of threads working on the search direction 
         * specified by this state object.
         */
        private final int totalNumberOfThreads;
        
        /**
         * The set of all the threads working on this particular direction.
         */
        private final Set<StoppableThread> runningThreadSet = 
                Collections.<StoppableThread>
                        newSetFromMap(new ConcurrentHashMap<>());
        
        /**
         * The set of all <b>slave</b> threads that are currently sleeping.
         */
        private final Set<SleepingThread> sleepingThreadSet = 
                Collections.<SleepingThread>
                        newSetFromMap(new ConcurrentHashMap<>());
        
        SearchState(final N initialNode, final int totalNumberOfThreads) {
            this.totalNumberOfThreads = totalNumberOfThreads;
            queue.enqueue(initialNode);
            parents.put(initialNode, null);
            distance.put(initialNode, 0);
        }
        
        int getTotalNumberOfThreads() {
            return totalNumberOfThreads;
        }
        
        int getSleepingThreadCount() {
            return sleepingThreadSet.size();
        }
        
        /**
         * Returns the queue of the search frontier.
         * 
         * @return the queue of the search frontier.
         */
        ConcurrentQueueWrapper<N> getQueue() {
            return queue;
        }
        
        /**
         * Returns the map mapping each node to its parent.
         * 
         * @return the parent map.
         */
        ConcurrentMapWrapper<N, N> getParentMap() {
            return parents;
        }
        
        /**
         * Returns the map mapping each node to its best distance.
         * 
         * @return the distance map.
         */
        ConcurrentMapWrapper<N, Integer> getDistanceMap() {
            return distance;
        }
        
        /**
         * Introduces a new thread to this search direction.
         * 
         * @param thread the thread to introduce.
         */
        void introduceThread(final StoppableThread thread) {
            // WARNING: set instead of list here.
            runningThreadSet.add(thread);
        }
        
        void putThreadToSleep(final SleepingThread thread) {
            sleepingThreadSet.add(thread);
            thread.putThreadToSleep(true);
        }
        
        void wakeupAllThreads() {
            for (final SleepingThread thread : sleepingThreadSet) {
                thread.putThreadToSleep(false);
            }
            
            sleepingThreadSet.clear();
        }
        
        void removeThread(final StoppableThread thread) {
            runningThreadSet.remove(thread);
        }
        
        /**
         * Tells all the thread working on current direction to exit so that the
         * threads may be joined.
         */
        void requestThreadsToExit() {
            for (final StoppableThread thread : runningThreadSet) {
                thread.requestThreadToExit();
            }
        }
    }
    
    /**
     * This abstract class defines a thread that may be asked to terminate.
     */
    private abstract static class StoppableThread extends Thread {
        
        /**
         * If set to {@code true}, this thread should exit.
         */
        protected volatile boolean exit;
        
        /**
         * Sends a request to finish the work.
         */
        void requestThreadToExit() {
            exit = true;
        }
    }
    
    private abstract static class SleepingThread extends StoppableThread {
        
        protected volatile boolean sleepRequested;
        protected final int threadSleepDuration;
        
        SleepingThread(final int threadSleepDuration) {
            this.threadSleepDuration = threadSleepDuration;
        }
        
        void putThreadToSleep(final boolean toSleep) {
            this.sleepRequested = toSleep;
        }
    }
    
    private abstract static class SearchThread<N> extends SleepingThread {
        
        /**
         * The ID of this thread.
         */
        protected final int id;
        
        /**
         * Holds the reference to the class responsible for computing the 
         * neighbor nodes of a given node.
         */
        protected final AbstractNodeExpander<N> nodeExpander;
        
        /**
         * The entire state of this search thread, shared possibly with other
         * threads.
         */
        protected final SearchState<N> searchState;
        
        /**
         * The state shared by both the directions.
         */
        protected final SharedSearchState<N> sharedSearchState;
        
        /**
         * Indicates whether this thread is a master or a slave thread.
         */
        protected final boolean isMasterThread;
        
        /**
         * The progress logger.
         */
        protected final ProgressLogger<N> searchProgressLogger;
        
        /**
         * Caches the amount of nodes expanded by this thread.
         */
        protected int numberOfExpandedNodes;
        
        /**
         * Constructs a new search thread.
         * 
         * @param searchState the state object.
         * @param master      the boolean flag indicating whether this new 
         *                    thread is a master or not.
         */
        SearchThread(final int id,
                     final AbstractNodeExpander<N> nodeExpander,
                     final SearchState<N> searchState, 
                     final SharedSearchState<N> sharedSearchState,
                     final boolean isMasterThread,
                     final ProgressLogger<N> searchProgressLogger,
                     final int threadSleepDuration) {
            super(threadSleepDuration);
            this.id                   = id;
            this.nodeExpander         = nodeExpander;
            this.searchState          = searchState;
            this.sharedSearchState    = sharedSearchState;
            this.isMasterThread       = isMasterThread;
            this.searchProgressLogger = searchProgressLogger;
        }
            
        @Override
        public boolean equals(final Object other) {
            if (other == null) {
                return false;
            }
            
            if (!getClass().equals(other.getClass())) {
                return false;
            }
            
            return id == ((SearchThread) other).id;
        }
        
        @Override
        public int hashCode() {
            return id;
        }
        
        public String toString() {
            return "[Thread ID: " + id + "]";
        }
        
        SearchState<N> getSearchState() {
            return searchState;
        }
        
        int getNumberOfExpandedNodes() {
            return numberOfExpandedNodes;
        }
    }
    
    /**
     * This class implements a search thread searching in forward direction.
     */
    private static final class ForwardSearchThread<N> extends SearchThread<N> {

        /**
         * Constructs a new forward search thread.
         * 
         * @param searchState the state object.
         * @param master      the boolean flag indicating whether this search
         *                    thread is a master or a slave thread.
         */
        ForwardSearchThread(
                final int id,
                final AbstractNodeExpander<N> nodeExpander,
                final SearchState<N> searchState, 
                final SharedSearchState<N> sharedSearchState,
                final boolean isMasterThread,
                final ProgressLogger<N> searchProgressLogger,
                final int threadSleepDuration) {
            
            super(id,
                  nodeExpander,
                  searchState, 
                  sharedSearchState,
                  isMasterThread,
                  searchProgressLogger,
                  threadSleepDuration);
        }
        
        @Override
        public void run() {
            final ConcurrentQueueWrapper<N> QUEUE = searchState.getQueue();
            final ConcurrentMapWrapper<N, N> PARENTS   = 
                    searchState.getParentMap();
            
            final ConcurrentMapWrapper<N, Integer> DISTANCE = 
                    searchState.getDistanceMap();
            
            while (true) {
                if (exit) {
                    return;
                }
                
                if (sleepRequested) {
                    // Only a slave thread may get here.
                    mysleep(30);
                    continue;
                }
                
                N current = QUEUE.dequeue();
                
                if (current == null) {
                    if (isMasterThread) {
                        int trials = 0;
                        
                        while (trials < 50) {
                            mysleep(10);
                            
                            if ((current = QUEUE.dequeue()) != null) {
                                break;
                            }
                            
                            ++trials;
                        }
                        
                        if (searchState.getSleepingThreadCount()
                                == searchState.getTotalNumberOfThreads() - 1) {
                            sharedSearchState.requestExit();
                            return;
                        } else {
                            continue;
                        }
                    } else {
                        // This thread is a slave thread, make it sleep:
                        getSearchState().putThreadToSleep(this);
                        putThreadToSleep(true);
                        continue;
                    }
                } else if (!QUEUE.isEmpty()) {
                    searchState.wakeupAllThreads();
                }
                
                if (searchProgressLogger != null) {
                    searchProgressLogger.onExpansion(current);
                }
             
                sharedSearchState.updateFromForwardDirection(current);
                
                if (sharedSearchState.pathIsOptimal(current)) {
                    sharedSearchState.requestExit();
                    return;
                }
                
                numberOfExpandedNodes++; 
                
                for (final N child : nodeExpander.expand(current)) {
                    if (!PARENTS.containsKey(child)) {
                        PARENTS.put(child, current);
                        DISTANCE.put(child, DISTANCE.get(current) + 1);
                        QUEUE.enqueue(child);
                        
                        if (searchProgressLogger != null) {
                            searchProgressLogger.onNeighborGeneration(child);
                        }
                    }
                }
            }
        }
    }
    
    /**
     * This class implements a search thread searching in backward direction.
     */
    private static final class BackwardSearchThread<N> extends SearchThread<N> {

        /**
         * Constructs a new backward search thread.
         * 
         * @param searchState the state object.
         * @param master      the boolean flag indicating whether this search
         *                    thread is a master or a slave thread.
         */
        BackwardSearchThread(final int id,
                             final AbstractNodeExpander<N> nodeExpander,
                             final SearchState<N> searchState, 
                             final SharedSearchState<N> sharedSearchState,
                             final boolean isMasterThread,
                             final ProgressLogger<N> searchProgressLogger,
                             final int threadSleepDuration) {
           super(id,
                 nodeExpander,
                 searchState,
                 sharedSearchState,
                 isMasterThread,
                 searchProgressLogger,
                 threadSleepDuration);
        }
        
        @Override
        public void run() {
            final ConcurrentQueueWrapper<N> QUEUE = searchState.getQueue();
            final ConcurrentMapWrapper<N, N> PARENTS = 
                    searchState.getParentMap();
            
            final ConcurrentMapWrapper<N, Integer> DISTANCE = 
                    searchState.getDistanceMap();
            
            while (true) {
                if (exit) {
                    return;
                }
                
                if (sleepRequested) {
                    // Only a slave thread may get here.
                    mysleep(30);
                    continue;
                }
                
                N current = QUEUE.dequeue();
                
                if (current == null) {
                    if (isMasterThread) {
                        
                        int trials = 0;
                        
                        while (trials < 50) {
                            mysleep(10);
                            
                            if ((current = QUEUE.dequeue()) != null) {
                                break;
                            }
                            
                            ++trials;
                        }
                        
                        if (searchState.getSleepingThreadCount()
                                == searchState.getTotalNumberOfThreads() - 1) {
                            sharedSearchState.requestExit();
                            return;
                        } else {
                            continue;
                        }
                    } else {
                        // This thread is a slave thread, make it sleep:
                        getSearchState().putThreadToSleep(this);
                        putThreadToSleep(true);
                        continue;
                    }
                } else if (!QUEUE.isEmpty()) {
                    searchState.wakeupAllThreads();
                }
                
                if (searchProgressLogger != null) {
                    searchProgressLogger.onExpansion(current);
                }
                
                sharedSearchState.updateFromBackwardDirection(current);
                
                if (sharedSearchState.pathIsOptimal(current)) {
                    sharedSearchState.requestExit();
                    return;
                }
                
                numberOfExpandedNodes++;
                
                for (final N parent : nodeExpander.expand(current)) {
                    if (!PARENTS.containsKey(parent)) {
                        PARENTS.put(parent, current);
                        DISTANCE.put(parent, DISTANCE.get(current) + 1);
                        QUEUE.enqueue(parent);
                        
                        if (searchProgressLogger != null) {
                            searchProgressLogger.onNeighborGeneration(parent);
                        }
                    }
                }
            }
        }
    }
    
    private static final class ConcurrentMapWrapper<K, V> {
        
        private final Map<K, V> map = new HashMap<>();
        
        synchronized boolean containsKey(final K key) {
            return map.containsKey(key);
        }
        
        // Unlike java.util.concurrent.ConcurrentHashMap, this map wrapper 
        // allows 'null' values:
        synchronized void put(final K key, final V value) {
            map.put(key, value);
        }
        
        synchronized V get(final K key) {
            return map.get(key);
        }
    }
    
    private static final class ConcurrentQueueWrapper<N> {
        
        private final Deque<N> queue;
        
        ConcurrentQueueWrapper(final Deque<N> queue) {
            this.queue = queue;
        }
        
        synchronized N dequeue() {
            if (queue.isEmpty()) {
                return null;
            }
            
            return queue.removeFirst();
        }
        
        synchronized void enqueue(final N node) {
            queue.addLast(node);
        }
        
        synchronized boolean isEmpty() {
            return queue.isEmpty();
        }
    }
    
    /**
     * This method puts the calling thread to sleep for {@code milliseconds}
     * milliseconds.
     * 
     * @param milliseconds the number of milliseconds to sleep for.
     */
    private static void mysleep(final int milliseconds) {
        try {
            Thread.sleep(milliseconds);
        } catch (final InterruptedException ex) {
            
        }
    }
}

