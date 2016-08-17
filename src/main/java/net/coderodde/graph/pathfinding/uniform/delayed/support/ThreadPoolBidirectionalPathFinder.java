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
import net.coderodde.graph.pathfinding.uniform.delayed.
       AbstractDelayedGraphPathFinder;
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
     * The default number of milliseconds a master thread sleeps when it finds
     * the frontier queue empty.
     */
    private static final int DEFAULT_MASTER_THREAD_SLEEP_DURATION = 10;

    /**
     * The default number of milliseconds a slave thread sleeps when it finds
     * the frontier queue empty.
     */
    private static final int DEFAULT_SLAVE_THREAD_SLEEP_DURATION = 10;

    /**
     * The default upper bound on the number of times a master thread hibernates
     * due to the frontier queue being empty before the entire search is 
     * terminated.
     */
    private static final int DEFAULT_NUMBER_OF_TRIALS = 50;

    /**
     * The minimum number of threads to allow. One thread per each of the two
     * search directions.
     */
    private static final int MINIMUM_NUMBER_OF_THREADS = 2;

    /**
     * The minimum number of milliseconds a <b>master thread</b> sleeps when it 
     * finds the frontier queue empty.
     */
    private static final int MINIMUM_MASTER_THREAD_SLEEP_DURATION = 3;

    /**
     * The minimum number of milliseconds a <b>slave thread</b> sleeps when it 
     * finds the frontier queue empty.
     */
    private static final int MINIMUM_SLAVE_THREAD_SLEEP_DURATION = 3;

    /**
     * The lower bound on the amount of trials.
     */
    private static final int MINIMUM_NUMBER_OF_TRIALS = 1;

    /**
     * Caches the requested number of threads to use in the each search process.
     */
    private final int numberOfThreads;

    /**
     * The duration of sleeping in milliseconds for the master threads.
     */
    private final int masterThreadSleepDuration;

    /**
     * The duration of sleeping in milliseconds for the slave threads.
     */
    private final int slaveThreadSleepDuration;

    /**
     * While a master thread waits the frontier queue to become non-empty, the
     * master thread makes at most {@code masterThreadTrials} sleeping sessions
     * before giving up and terminating the search.
     */
    private final int masterThreadTrials;

    /**
     * Constructs this path finder.
     * 
     * @param requestedThreadCount      the number of threads searching.
     * @param masterThreadSleepDuration the number of milliseconds a master 
     *                                  thread sleeps whenever it discovers the
     *                                  frontier queue being empty.
     * @param slaveThreadSleepDuration  the number of milliseconds a slave
     *                                  thread sleeps whenever it discovers the
     *                                  frontier queue being empty.
     * @param masterThreadTrials        the number of times the master thread
     *                                  hibernates itself before terminating the
     *                                  entire search.
     */
    public ThreadPoolBidirectionalPathFinder(
            final int requestedThreadCount,
            final int masterThreadSleepDuration,
            final int slaveThreadSleepDuration,
            final int masterThreadTrials) {
        this.numberOfThreads = Math.max(requestedThreadCount, 
                                        MINIMUM_NUMBER_OF_THREADS);

        this.masterThreadSleepDuration = 
                Math.max(masterThreadSleepDuration,
                         MINIMUM_MASTER_THREAD_SLEEP_DURATION);

        this.slaveThreadSleepDuration = 
                Math.max(slaveThreadSleepDuration,
                         MINIMUM_SLAVE_THREAD_SLEEP_DURATION);

        this.masterThreadTrials = 
                Math.max(masterThreadTrials,
                         MINIMUM_NUMBER_OF_TRIALS);
    }

    /**
     * Construct this path finder using default sleeping duration.
     * 
     * @param requestedThreadCount the number of threads searching.
     */
    public ThreadPoolBidirectionalPathFinder(final int requestedThreadCount) {
        this(requestedThreadCount, 
             DEFAULT_MASTER_THREAD_SLEEP_DURATION,
             DEFAULT_SLAVE_THREAD_SLEEP_DURATION,
             DEFAULT_NUMBER_OF_TRIALS);
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

        // Check the validity of the source node:
        if (!forwardSearchNodeExpander.isValidNode(source)) {
            throw new IllegalArgumentException(
                    "The source node (" + source + ") was rejected by the " +
                    "forward search node expander.");
        }

        // Check the validity of the target node:
        if (!backwardSearchNodeExpander.isValidNode(target)) {
            throw new IllegalArgumentException(
                    "The target node (" + target + ") was rejected by the " +
                    "backward search node expander.");
        }

        // Possibly log the beginning of the search:
        if (sharedSearchProgressLogger != null) {
            sharedSearchProgressLogger.onBeginSearch(source, target);
        }

        // This path finder collects some performance related statistics:
        this.duration = System.currentTimeMillis();

        // Compute the numbers of threads for each of the search direction:
        final int forwardSearchThreadCount  = numberOfThreads / 2;
        final int backwardSearchThreadCount = numberOfThreads - 
                                              forwardSearchThreadCount;

        // Create the state object shared by all the threads working on forward
        // search direction:
        final SearchState<N> forwardSearchState = new SearchState<>(source);

        // Create the state object shared by all the threads working on backward
        // search direction:
        final SearchState<N> backwardSearchState = new SearchState<>(target);

        // Create the state object shared by both the search direction:
        final SharedSearchState<N> sharedSearchState = 
                new SharedSearchState(source, 
                                      target, 
                                      forwardSearchState,
                                      backwardSearchState,
                                      sharedSearchProgressLogger);

        // The array holding all forward search threads:
        final ForwardSearchThread[] forwardSearchThreads =
                new ForwardSearchThread[numberOfThreads];

        // Below, the value of 'sleepDuration' is ignored since the thread being 
        // created is a master thread that never sleeps.
        forwardSearchThreads[0] = 
                new ForwardSearchThread(0, 
                                        forwardSearchNodeExpander,
                                        forwardSearchState,
                                        sharedSearchState,
                                        true,
                                        forwardSearchProgressLogger,
                                        masterThreadSleepDuration,
                                        masterThreadTrials);

        // Spawn the forward search master thread:
        forwardSearchState.introduceThread(forwardSearchThreads[0]);
        forwardSearchThreads[0].start();

        // Create and spawn all the slave threads working on forward search 
        // direction.
        for (int i = 1; i < numberOfThreads; ++i) {
            forwardSearchThreads[i] = 
                    new ForwardSearchThread(i,
                                            forwardSearchNodeExpander,
                                            forwardSearchState,
                                            sharedSearchState,
                                            false,
                                            forwardSearchProgressLogger,
                                            slaveThreadSleepDuration,
                                            masterThreadTrials);

            forwardSearchState.introduceThread(forwardSearchThreads[i]);
            forwardSearchThreads[i].start();
        }

        // The array holding all backward search threads:
        final BackwardSearchThread[] backwardSearchThreads =
                new BackwardSearchThread[numberOfThreads];

        // Below, the value of 'sleepDuration' is ignored since the thread being
        // created is a master thread that never sleeps.
        backwardSearchThreads[0] = 
                new BackwardSearchThread(forwardSearchThreads.length,
                                         backwardSearchNodeExpander,
                                         backwardSearchState,
                                         sharedSearchState,
                                         true,
                                         backwardSearchProgressLogger,
                                         masterThreadSleepDuration,
                                         masterThreadTrials);

        // Spawn the backward search master thread:
        backwardSearchState.introduceThread(backwardSearchThreads[0]);
        backwardSearchThreads[0].start();

        // Create and spawn all the slave threads working on backward search
        // direction:
        for (int i = 1; i < numberOfThreads; ++i) {
            backwardSearchThreads[i] = 
                    new BackwardSearchThread(forwardSearchThreads.length + i,
                                             backwardSearchNodeExpander,
                                             backwardSearchState,
                                             sharedSearchState,
                                             false,
                                             backwardSearchProgressLogger,
                                             slaveThreadSleepDuration,
                                             masterThreadTrials);

            backwardSearchState.introduceThread(backwardSearchThreads[i]);
            backwardSearchThreads[i].start();
        }

        // Wait all forward search threads to finish their work:
        try {
            for (final ForwardSearchThread thread : forwardSearchThreads) {
                thread.join();
            }
        } catch (final InterruptedException ex) {
            throw new IllegalStateException("The forward thread threw " +
                    ex.getClass().getSimpleName() + ": " +
                    ex.getMessage(), ex);
        }

        // Wait all backward search threads to finish their work: 
        try {
            for (final BackwardSearchThread thread : backwardSearchThreads) {
                thread.join();
            }
        } catch (final InterruptedException ex) {
            throw new IllegalStateException("The backward thread threw " +
                    ex.getClass().getSimpleName() + ": " +
                    ex.getMessage(), ex);
        }

        // Record the duration of the search:
        this.duration = System.currentTimeMillis() - this.duration;

        // Count the number of expanded nodes over all threads:
        this.numberOfExpandedNodes = 0;

        for (final ForwardSearchThread thread : forwardSearchThreads) {
            this.numberOfExpandedNodes += thread.getNumberOfExpandedNodes();
        }

        for (final BackwardSearchThread thread : backwardSearchThreads) {
            this.numberOfExpandedNodes += thread.getNumberOfExpandedNodes();
        }

        // Construct and return the path:
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
        private final SearchState<N> forwardSearchState;

        /**
         * The state of all the backward search threads.
         */
        private final SearchState<N> backwardSearchState;

        /**
         * Caches the best known length from the source to the target nodes.
         */
        private volatile int bestPathLengthSoFar = Integer.MAX_VALUE;

        /**
         * The best search frontier touch node so far.
         */
        private volatile N touchNode;

        /**
         * The progress logger for reporting the progress.
         */
        private final ProgressLogger<N> sharedProgressLogger;

        /**
         * Constructs a shared state information object for the search.
         * 
         * @param source               the source node.
         * @param target               the target node.
         * @param forwardSearchState   the state of the forward search
         *                             direction.
         * @param backwardSearchState  the state of the backward search
         *                             direction.
         * @param sharedProgressLogger the progress logger for logging the 
         *                             overall progress of the path finder.
         */
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

        /**
         * Attempts to update the best known path.
         * 
         * @param current the touch node candidate.
         */
        synchronized void updateSearchState(final N current) {
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
        
        synchronized boolean pathIsOptimal() {
            if (touchNode == null) {
                return false;
            }

            final N forwardSearchHead = forwardSearchState.getQueue().getHead();

            if (forwardSearchHead == null) {
                return false;
            }

            final N backwardSearchHead = backwardSearchState.getQueue()
                                                            .getHead();

            if (backwardSearchHead == null) {
                return false;
            }

            final int distance =
                  forwardSearchState .getDistanceMap().get(forwardSearchHead) +
                  backwardSearchState.getDistanceMap().get(backwardSearchHead);

            return distance > bestPathLengthSoFar;
        }

        /**
         * Asks every single thread to exit.
         */
        synchronized void requestExit() {
            forwardSearchState .requestThreadsToExit();
            backwardSearchState.requestThreadsToExit();
        }

        /**
         * Constructs a shortest path and returns it as a list. If the target
         * node is unreachable from the source node, returns an empty list.
         * 
         * @return a shortest path found, or an empty list if target node is not 
         *         reachable from the source node.
         */
        synchronized List<N> getPath() {
            if (touchNode == null) {
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
                new ConcurrentMapWrapper<>(new HashMap<>());

        /**
         * This map maps each discovered node to its shortest path distance from
         * the source node.
         */
        private final ConcurrentMapWrapper<N, Integer> distance =
                new ConcurrentMapWrapper<>(new HashMap<>());

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

        /**
         * Constructs the search state object.
         * 
         * @param initialNode          the node from which the search begins. If
         *                             this state object is used in the forward
         *                             search, this node should be the source 
         *                             node. Otherwise, if this state object is
         *                             used in the backward search, this node
         *                             should be the target node.
         * @param totalNumberOfThreads the number of threads working on a 
         *                             particular search direction.
         */
        SearchState(final N initialNode) {
            queue.enqueue(initialNode);
            parents.put(initialNode, null);
            distance.put(initialNode, 0);
        }

        /**
         * Return the number of slave threads sleeping in the search direction
         * specified by this search state object.
         * 
         * @return the number of slave threads sleeping.
         */
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
         * Returns the map mapping each node to its shortest distance from the
         * starting node.
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
            runningThreadSet.add(thread);
        }

        /**
         * Asks the argument thread to go to sleep and adds it to the set of
         * sleeping slave threads.
         * 
         * @param thread the <b>slave</b> thread to hibernate.
         */
        void putThreadToSleep(final SleepingThread thread) {
            sleepingThreadSet.add(thread);
            thread.putThreadToSleep(true);
        }

        /**
         * Wakes up all the sleeping slave threads.
         */
        void wakeupAllThreads() {
            for (final SleepingThread thread : sleepingThreadSet) {
                thread.putThreadToSleep(false);
            }

            sleepingThreadSet.clear();
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

    /**
     * This abstract class defines a thread that may be asked to go to sleep.
     */
    private abstract static class SleepingThread extends StoppableThread {

        /**
         * Holds the flag indicating whether this thread is put to sleep.
         */
        protected volatile boolean sleepRequested;

        /**
         * The number of milliseconds to sleep during each hibernation.
         */
        protected final int threadSleepDuration;

        /**
         * The maximum number of times a master thread hibernates itself before
         * giving up and terminating the entire search.
         */
        protected final int threadSleepTrials;

        /**
         * Constructs this thread supporting sleeping.
         * 
         * @param threadSleepDuration the number of milliseconds to sleep each 
         *                            time.
         * @param threadSleepTrials   the maximum number of trials to hibernate
         *                            a master thread before giving up.
         */
        SleepingThread(final int threadSleepDuration,
                       final int threadSleepTrials) {
            this.threadSleepDuration = threadSleepDuration;
            this.threadSleepTrials   = threadSleepTrials;
        }

        /**
         * Sets the current sleep status of this thread.
         * 
         * @param toSleep indicates whether to put this thread to sleep or 
         *                wake it up.
         */
        void putThreadToSleep(final boolean toSleep) {
            this.sleepRequested = toSleep;
        }
    }

    /**
     * This class defines all the state that should appear in threads working in
     * both search direction.
     * 
     * @param <N> the actual node type.
     */
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
         * threads working on the same search direction.
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
         * Construct this search thread.
         * 
         * @param id                   the ID number of this thread. Must be
         *                             unique over <b>all</b> search threads.
         * @param nodeExpander         the node expander responsible for 
         *                             generating the neighbors in this search
         *                             thread.
         * @param searchState          the search state object.
         * @param sharedSearchState    the search state object shared with both
         *                             forward search threads and backward
         *                             search threads.
         * @param isMasterThread       indicates whether this search thread is a
         *                             master thread or a slave thread.
         * @param searchProgressLogger the progress logger for the search 
         *                             direction of this search thread.
         * @param threadSleepDuration  the duration of sleeping in milliseconds
         *                             always when a thread finds the frontier 
         *                             queue empty.
         * @param threadSleepTrials    the maximum number of hibernation trials
         *                             before a master thread gives up and 
         *                             terminates the entire search process. If
         *                             this thread is a slave thread, this 
         *                             parameter is ignored.
         */
        SearchThread(final int id,
                     final AbstractNodeExpander<N> nodeExpander,
                     final SearchState<N> searchState, 
                     final SharedSearchState<N> sharedSearchState,
                     final boolean isMasterThread,
                     final ProgressLogger<N> searchProgressLogger,
                     final int threadSleepDuration,
                     final int threadSleepTrials) {
            super(threadSleepDuration, threadSleepTrials);
            this.id                   = id;
            this.nodeExpander         = nodeExpander;
            this.searchState          = searchState;
            this.sharedSearchState    = sharedSearchState;
            this.isMasterThread       = isMasterThread;
            this.searchProgressLogger = searchProgressLogger;
        }

        /**
         * {@inheritDoc }
         */
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

        /**
         * {@inheritDoc }
         */
        @Override
        public int hashCode() {
            return id;
        }

        /**
         * {@inheritDoc }
         */
        public String toString() {
            return "[Thread ID: " + id + "]";
        }

        /**
         * Returns the number of nodes expanded by this search thread.
         * 
         * @return the number of nodes.
         */
        int getNumberOfExpandedNodes() {
            return numberOfExpandedNodes;
        }
    }

    /**
     * This class implements a search thread searching in forward direction.
     */
    private static final class ForwardSearchThread<N> extends SearchThread<N> {

        /**
         * Constructs a forward search thread.
         * 
         * @param id                   the ID of this thread. Must be unique 
         *                             over <b>all</b> search threads.
         * @param nodeExpander         the node expander responsible for 
         *                             generating the neighbor nodes of a given
         *                             node.
         * @param searchState          the search state object.
         * @param sharedSearchState    the shared search state object.
         * @param isMasterThread       indicates whether this thread is a master
         *                             or a slave thread.
         * @param searchProgressLogger the progress logger for logging the 
         *                             progress of this thread.
         * @param threadSleepDuration  the number of milliseconds to sleep 
         *                             whenever a thread finds the frontier 
         *                             queue empty.
         * @param threadSleepTrials    the maximum number of times a master
         *                             thread hibernates itself before giving 
         *                             up.
         */
        ForwardSearchThread(
                final int id,
                final AbstractNodeExpander<N> nodeExpander,
                final SearchState<N> searchState, 
                final SharedSearchState<N> sharedSearchState,
                final boolean isMasterThread,
                final ProgressLogger<N> searchProgressLogger,
                final int threadSleepDuration,
                final int threadSleepTrials) {
            super(id,
                  nodeExpander,
                  searchState, 
                  sharedSearchState,
                  isMasterThread,
                  searchProgressLogger,
                  threadSleepDuration,
                  threadSleepTrials);
        }

        @Override
        public void run() {
            final ConcurrentQueueWrapper<N>        QUEUE;
            final ConcurrentMapWrapper<N, N>       PARENTS;
            final ConcurrentMapWrapper<N, Integer> DISTANCE;
            
            QUEUE    = searchState.getQueue();
            PARENTS  = searchState.getParentMap();
            DISTANCE = searchState.getDistanceMap();
            
            while (true) {
                if (exit) {
                    // This thread is asked to exit:
                    return;
                }

                if (sleepRequested) {
                    // Only a slave thread may get here. Just sleep and then
                    // reiterate.
                    mysleep(threadSleepDuration);
                    continue;
                }

                N current = QUEUE.dequeue();

                if (current == null) {
                    if (isMasterThread) {
                        for (int trials = 0; 
                             trials < threadSleepTrials; 
                             trials++) {
                            // Only a master thread may get here.
                            mysleep(threadSleepDuration);

                            if ((current = QUEUE.dequeue()) != null) {
                                break;
                            }
                        }

                        if (current == null) {
                            sharedSearchState.requestExit();
                            return;
                        } else {
                            searchState.wakeupAllThreads();
                        }
                    } else {
                        // This thread is a slave thread, make it sleep:
                        searchState.putThreadToSleep(this);
                        continue;
                    }
                } else if (!QUEUE.isEmpty()) {
                    searchState.wakeupAllThreads();
                }

                // Possibly log the expansion of a node:
                if (searchProgressLogger != null) {
                    searchProgressLogger.onExpansion(current);
                }

                // Possibly improve the shortest path so far:
                sharedSearchState.updateSearchState(current);

                // If the current path is guaranteed to be optimal, terminate
                // the entire search so that the threads may be joined and the 
                // path constructed:
                if (sharedSearchState.pathIsOptimal()) {
                    sharedSearchState.requestExit();
                    return;
                }

                numberOfExpandedNodes++; 

                // Expand the current node:
                for (final N child : nodeExpander.expand(current)) {
                    if (!DISTANCE.containsKey(child)) {
                        PARENTS.put(child, current);
                        DISTANCE.put(child, DISTANCE.get(current) + 1);
                        QUEUE.enqueue(child);

                        if (searchProgressLogger != null) {
                            searchProgressLogger.onNeighborGeneration(child);
                        }
                    } else if (DISTANCE.get(child) > DISTANCE.get(current) + 1) {
                        DISTANCE.put(child, DISTANCE.get(current) + 1);
                        PARENTS.put(child, current);
                        
                        if (searchProgressLogger != null) {
                            searchProgressLogger.onNeighborImprovement(child);
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
         * Constructs a backward search thread.
         * 
         * @param id                   the ID of this thread. Must be unique 
         *                             over <b>all</b> search threads.
         * @param nodeExpander         the node expander responsible for 
         *                             generating the neighbor nodes of a given
         *                             node.
         * @param searchState          the search state object.
         * @param sharedSearchState    the shared search state object.
         * @param isMasterThread       indicates whether this thread a master or
         *                             a slave thread.
         * @param searchProgressLogger the progress logger for logging the 
         *                             progress of this thread.
         * @param threadSleepDuration  the number of milliseconds to sleep 
         *                             whenever a slave thread finds the
         *                             frontier queue empty.
         * @param threadSleepTrials    the maximum number of times a master
         *                             thread hibernates itself before giving 
         *                             up.
         */
        BackwardSearchThread(final int id,
                             final AbstractNodeExpander<N> nodeExpander,
                             final SearchState<N> searchState, 
                             final SharedSearchState<N> sharedSearchState,
                             final boolean isMasterThread,
                             final ProgressLogger<N> searchProgressLogger,
                             final int threadSleepDuration,
                             final int threadSleepTrials) {
           super(id,
                 nodeExpander,
                 searchState,
                 sharedSearchState,
                 isMasterThread,
                 searchProgressLogger,
                 threadSleepDuration,
                 threadSleepTrials);
        }

        @Override
        public void run() {
            final ConcurrentQueueWrapper<N>        QUEUE;
            final ConcurrentMapWrapper<N, N>       PARENTS;
            final ConcurrentMapWrapper<N, Integer> DISTANCE;
            
            QUEUE    = searchState.getQueue();
            PARENTS  = searchState.getParentMap();
            DISTANCE = searchState.getDistanceMap();
            
            while (true) {
                if (exit) {
                    // This thread is asked to exit:
                    return;
                }

                if (sleepRequested) {
                    // Only a slave thread may get here. Just sleep and then
                    // reiterate.
                    mysleep(threadSleepDuration);
                    continue;
                }

                N current = QUEUE.dequeue();

                if (current == null) {
                    if (isMasterThread) {
                        for (int trials = 0; 
                             trials < threadSleepTrials; 
                             trials++) {
                            mysleep(threadSleepDuration);

                            if ((current = QUEUE.dequeue()) != null) {
                                break;
                            }
                        }

                        if (current == null) {
                            sharedSearchState.requestExit();
                            return;
                        } else {
                            searchState.wakeupAllThreads();
                        }
                    } else {
                        // This thread is a slave thread, make it sleep:
                        searchState.putThreadToSleep(this);
                        continue;
                    }
                } else if (!QUEUE.isEmpty()) {
                    searchState.wakeupAllThreads();
                }

                // Possibly log the expansion of a node:
                if (searchProgressLogger != null) {
                    searchProgressLogger.onExpansion(current);
                }

                // Possibly improve the shortest path so far:
                sharedSearchState.updateSearchState(current);

                // If the current path is guaranteed to be optimal, terminate
                // the entire search so that the threads may be joined and the 
                // path constructed:
                if (sharedSearchState.pathIsOptimal()) {
                    sharedSearchState.requestExit();
                    return;
                }

                numberOfExpandedNodes++;

                // Expand the current node:
                for (final N parent : nodeExpander.expand(current)) {
                    if (!DISTANCE.containsKey(parent)) {
                        PARENTS.put(parent, current);
                        DISTANCE.put(parent, DISTANCE.get(current) + 1);
                        QUEUE.enqueue(parent);

                        if (searchProgressLogger != null) {
                            searchProgressLogger.onNeighborGeneration(parent);
                        }
                    } else if (DISTANCE.get(parent) > DISTANCE.get(current) + 1) {
                        DISTANCE.put(parent, DISTANCE.get(current) + 1);
                        PARENTS.put(parent, current);
                        
                        if (searchProgressLogger != null) {
                            searchProgressLogger.onNeighborImprovement(parent);
                        }
                    }
                }
            }
        }
    }

    /**
     * This class implements a concurrent {@link java.util.Map} wrapper. One 
     * reason to use this instead of a
     * {@link java.util.concurrent.ConcurrentHashMap} is that the latter does
     * not allow {@code null} <b>values</b>. However, we have to map the source
     * and the target nodes to {@code null}.
     * 
     * @param <K> the actual key type.
     * @param <V> the actual value type.
     */
    private static final class ConcurrentMapWrapper<K, V> {

        private final Map<K, V> map;
        
        ConcurrentMapWrapper(final Map<K, V> map) {
            this.map = map;
        }

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

    /**
     * This class implements a concurrent {@link java.util.Deque} wrapper. We 
     * need this to be able to implement {@code dequeue} atomically.
     * 
     * @param <N> the actual element type.
     */
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

        synchronized N getHead() {
            return queue.isEmpty() ? null : queue.getFirst();
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
        } catch (final InterruptedException ex) {}
    }
}
