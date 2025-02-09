package com.ververica.cdc.connectors.tdsql.source.reader.fetcher;

import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.SourceReaderBase;
import org.apache.flink.connector.base.source.reader.fetcher.SplitFetcher;
import org.apache.flink.connector.base.source.reader.fetcher.SplitFetcherManager;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;

import com.ververica.cdc.connectors.tdsql.source.assigner.splitter.TdSqlSplit;

import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

/**
 * Fetcher manager for TDSQL.
 *
 * @param <T> The output type for flink.
 */
public class TdSqlFetcherManager<T> extends SplitFetcherManager<T, TdSqlSplit> {
    /**
     * Creates a new SplitFetcherManager with multiple I/O threads.
     *
     * @param elementsQueue The queue that is used to hand over data from the I/O thread (the
     *     fetchers) to the reader (which emits the records). This must be the same queue instance
     *     that is also passed to the {@link SourceReaderBase}.
     * @param splitReaderFactory The factory for the split reader that connects to the source
     */
    public TdSqlFetcherManager(
            FutureCompletingBlockingQueue<RecordsWithSplitIds<T>> elementsQueue,
            Supplier<SplitReader<T, TdSqlSplit>> splitReaderFactory) {
        super(elementsQueue, splitReaderFactory);
    }

    @Override
    public void addSplits(List<TdSqlSplit> splitsToAdd) {
        for (TdSqlSplit split : splitsToAdd) {
            SplitFetcher<T, TdSqlSplit> fetcher = createSplitFetcher();
            fetcher.addSplits(Collections.singletonList(split));
            startFetcher(fetcher);
        }
    }
}
