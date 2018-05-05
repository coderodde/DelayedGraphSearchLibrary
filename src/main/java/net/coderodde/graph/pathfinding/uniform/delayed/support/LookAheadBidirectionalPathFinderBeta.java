package net.coderodde.graph.pathfinding.uniform.delayed.support;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.logging.Level;
import java.util.logging.Logger;
import net.coderodde.graph.pathfinding.uniform.delayed.AbstractDelayedGraphPathFinder;
import net.coderodde.graph.pathfinding.uniform.delayed.AbstractNodeExpander;
import net.coderodde.graph.pathfinding.uniform.delayed.ProgressLogger;

/**
 * 
 * 
 * @author Rodion "rodde" Efremov
 * @version 1.6 (May 5, 2018)
 * @param <N> the actual graph node type.
 */
public class LookAheadBidirectionalPathFinderBeta<N> 
extends AbstractDelayedGraphPathFinder<N> {

    /**
     * The default number of milliseconds to wait for the response.
     */
    private static final int DEFAULT_DOCUMENT_DOWNLOAD_TIMEOUT_MILLISECONDS = 
            5000;
    
    /**
     * The default number of download trials.
     */
    private static final int DEFAULT_NUMBER_OF_DOWNLOAD_TRIALS = 2;
    
    /**
     * The default number of forward downloader threads.
     */
    private static final int DEFAULT_NUMBER_OF_FORWARD_DOWNLOADER_THREADS = 16;
    
    /**
     * The default number of backward downloader threads.
     */
    private static final int DEFAULT_NUMBER_OF_BACKWARD_DOWNLOADER_THREADS = 16;
    
    /**
     * The minimum number of forward downloader threads.
     */
    private static final int MINIMUM_NUMBER_OF_FORWARD_DOWNLOADER_THREADS = 1;
    
    /**
     * The default number of backward downloader threads.
     */
    private static final int MINIMUM_NUMBER_OF_BACKWARD_DOWNLOADER_THREADS = 1;
    
    /**
     * The actual number of milliseconds to wait for the response.
     */
    private int documentDownloadTimeoutMilliseconds = 
            DEFAULT_DOCUMENT_DOWNLOAD_TIMEOUT_MILLISECONDS;
    
    /**
     * The actual number of download trials. If the web site does not response
     * that many times, the downloader thread exits.
     */
    private int numberOfDownloadTrials = DEFAULT_NUMBER_OF_DOWNLOAD_TRIALS;
    
    /**
     * The actual number of forward downloader threads.
     */
    private int numberOfForwardDownloaderThreads = 
            DEFAULT_NUMBER_OF_FORWARD_DOWNLOADER_THREADS;
    
    /**
     * The actual number of backward downloader threads.
     */
    private int numberOfBackwardDownloaderThreads = 
            DEFAULT_NUMBER_OF_BACKWARD_DOWNLOADER_THREADS;
    
    /**
     * The logger. If this field is set to {@code null}, no logging is done.
     */
    private Logger logger;
    
    /**
     * Returns the document download timeout in milliseconds. The value of zero
     * indicates that there is no timeout.
     * 
     * @return the document download timeout in milliseconds.
     */
    public int getDocumentDownloadTimeoutMilliseconds() {
        return documentDownloadTimeoutMilliseconds;
    }
    
    /**
     * Returns the number of download trials. If a downloader thread fails to 
     * download the document specified number of times, that thread exits.
     * 
     * @return the number of download trials.
     */
    public int getNumberOfDownloadTrials() {
        return numberOfDownloadTrials;
    }
    
    /**
     * Returns the number of forward downloader threads.
     * 
     * @return the number of forward downloader threads.
     */
    public int getNumberOfForwardDownloaderThreads() {
        return numberOfForwardDownloaderThreads;
    }
    
    /**
     * Returns the number of backward downloader threads.
     * 
     * @return the number of backward downloader threads. 
     */
    public int getNumberOfBackwardDownloaderThreads() {
        return numberOfBackwardDownloaderThreads;
    }
    
    /**
     * Returns the current logger.
     * 
     * @return the current logger.
     */
    public Logger getLogger() {
        return logger;
    }
    
    /**
     * Sets the document download timeout in milliseconds. If the argument is
     * non-positive, the downloader threads will not time out.
     * 
     * @param documentDownloadTimeoutMilliseconds the number of milliseconds to 
     *                                            wait for. Zero specifies that 
     *                                            there is no timeout and a 
     *                                            downloader thread may stuck 
     *                                            forever.
     * @return this pathfinder.
     */
    public LookAheadBidirectionalPathFinderBeta<N> 
        setDocumentDownloadTimeoutMilliseconds(
                int documentDownloadTimeoutMilliseconds) {
        if (documentDownloadTimeoutMilliseconds < 0) {
            documentDownloadTimeoutMilliseconds = 0;
            
            if (logger != null) {
                logger.log(
                        Level.WARNING, 
                        "The requested time out is negative: {0}. " +
                        "The threads will not time out.",
                        documentDownloadTimeoutMilliseconds);
            }
        } else if (documentDownloadTimeoutMilliseconds == 0) {
            if (logger != null) {
                logger.warning(
                        "Requesting the downloader threads to not time out.");
            }
        }
        
        this.documentDownloadTimeoutMilliseconds = 
                documentDownloadTimeoutMilliseconds;
        
        return this;
    }
        
    /**
     * Sets the number of download trials.
     * 
     * @param numberOfDownloadTrials the number of download trials. After that
     *                               many trials, a downloader thread exits.
     * @return this pathfinder.
     */
    public LookAheadBidirectionalPathFinderBeta<N>
        setNumberOfDownloadTrials(int numberOfDownloadTrials) {
        if (numberOfDownloadTrials < 1) {
            if (logger != null) {
                logger.log(
                        Level.WARNING, 
                        "The requested number of download trials is too " + 
                        "small: {0}. Setting to 1.", numberOfDownloadTrials);
                
                this.numberOfDownloadTrials = 1;
            }
        } else {
            this.numberOfDownloadTrials = numberOfDownloadTrials;
        }
        
        return this;
    }
        
    /**
     * Sets the number of forward downloader threads.
     * 
     * @param numberOfForwardDownloaderThreads the number of forward downloader
     *                                         threads.
     * @return this pathfinder.
     */
    public LookAheadBidirectionalPathFinderBeta<N> 
            setNumberOfForwardDownloaderThreads(
                    int numberOfForwardDownloaderThreads) {
        if (numberOfForwardDownloaderThreads <
                MINIMUM_NUMBER_OF_FORWARD_DOWNLOADER_THREADS) {
            if (logger != null) {
                logger.log(
                        Level.WARNING, 
                        "The requested number of forward downloader threads " +
                        "is too small: {0}. Will be increased to {1}.", 
                        new Object[]{
                            numberOfForwardDownloaderThreads, 
                            MINIMUM_NUMBER_OF_FORWARD_DOWNLOADER_THREADS
                        });
            }
            
            this.numberOfForwardDownloaderThreads = 
                    MINIMUM_NUMBER_OF_FORWARD_DOWNLOADER_THREADS;
        } else {
            this.numberOfForwardDownloaderThreads =
                    numberOfForwardDownloaderThreads;
        }
        
        return this;
    }
    
    /**
     * Sets the number of backward downloader threads.
     * 
     * @param numberOfBackwardDownloaderThreads the number of backward 
     *                                          downloader threads.
     * @return this pathfinder.
     */
    public LookAheadBidirectionalPathFinderBeta<N>
            setNumberOfBackwardDownloaderThreads(
                    int numberOfBackwardDownloaderThreads) {
        if (numberOfBackwardDownloaderThreads <
                MINIMUM_NUMBER_OF_BACKWARD_DOWNLOADER_THREADS) {
            
            if (logger != null) {
                logger.log(
                        Level.WARNING,
                        "The requested number of backward downloader threads " +
                        "is too small: {0}. Will be increased to {1}.",
                        new Object[]{
                            numberOfBackwardDownloaderThreads,
                            MINIMUM_NUMBER_OF_BACKWARD_DOWNLOADER_THREADS
                        });
            }
            
            this.numberOfBackwardDownloaderThreads = 
                    MINIMUM_NUMBER_OF_BACKWARD_DOWNLOADER_THREADS;
        } else {
            this.numberOfBackwardDownloaderThreads = 
                    numberOfBackwardDownloaderThreads;
        }
        
        return this;
    }
            
    @Override
    public List<N> 
        search(final N source, 
               final N target, 
               final AbstractNodeExpander<N> forwardSearchNodeExpander, 
               final AbstractNodeExpander<N> backwardSearchNodeExpander, 
               final ProgressLogger<N> forwardSearchProgressLogger, 
               final ProgressLogger<N> backwardSearchProgressLogger, 
               final ProgressLogger<N> sharedSearchProgressLogger) {
            Objects.requireNonNull(source, "The source node is null.");
            Objects.requireNonNull(target, "The target node is null.");
            Objects.requireNonNull(forwardSearchNodeExpander, 
                                   "The forward search node expander.");
            Objects.requireNonNull(backwardSearchProgressLogger,
                                   "The backward search node expander.");
            
            AbstractGraphNodeNextNodesProvider<N> forwardProvider  = null;
            AbstractGraphNodeNextNodesProvider<N> backwardProvider = null;
        return null;
    }
    
    /**
     * This abstract class specifies the API for all classes that provide a
     * collection of next nodes in the search.
     * 
     * @param <N> the actual graph node type.
     */
    private static abstract class AbstractGraphNodeNextNodesProvider<N> {
        
        /**
         * Gets the next nodes in the search process. In the forward search 
         * direction, this method returns a collection of child nodes of 
         * {@code node}. In the backward search direction, this method returns
         * a collection of parent nodes of {@code node}.
         * 
         * @return a collection of next nodes to consider.
         */
        abstract Collection<N> getNextNodes(N node);
    }
    
    
}
