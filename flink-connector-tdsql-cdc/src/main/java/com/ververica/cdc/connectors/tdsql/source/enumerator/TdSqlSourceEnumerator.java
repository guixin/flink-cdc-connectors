/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.connectors.tdsql.source.enumerator;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.util.FlinkRuntimeException;

import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;

import com.ververica.cdc.connectors.mysql.source.assigners.MySqlSplitAssigner;
import com.ververica.cdc.connectors.mysql.source.assigners.state.PendingSplitsState;
import com.ververica.cdc.connectors.mysql.source.config.MySqlSourceConfig;
import com.ververica.cdc.connectors.mysql.source.events.BinlogSplitMetaEvent;
import com.ververica.cdc.connectors.mysql.source.events.BinlogSplitMetaRequestEvent;
import com.ververica.cdc.connectors.mysql.source.events.FinishedSnapshotSplitsAckEvent;
import com.ververica.cdc.connectors.mysql.source.events.FinishedSnapshotSplitsReportEvent;
import com.ververica.cdc.connectors.mysql.source.events.FinishedSnapshotSplitsRequestEvent;
import com.ververica.cdc.connectors.mysql.source.offset.BinlogOffset;
import com.ververica.cdc.connectors.mysql.source.split.FinishedSnapshotSplitInfo;
import com.ververica.cdc.connectors.mysql.source.split.MySqlSplit;
import com.ververica.cdc.connectors.tdsql.bases.set.TdSqlSet;
import com.ververica.cdc.connectors.tdsql.source.assigner.splitter.TdSqlSplit;
import com.ververica.cdc.connectors.tdsql.source.assigner.state.TdSqlPendingSplitsState;
import com.ververica.cdc.connectors.tdsql.source.events.TdSqlSourceEvent;
import org.eclipse.jetty.util.ajax.JSON;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/** tdsql source enumerator. */
@Internal
public class TdSqlSourceEnumerator implements SplitEnumerator<TdSqlSplit, TdSqlPendingSplitsState> {
    private static final Logger LOG = LoggerFactory.getLogger(TdSqlSourceEnumerator.class);

    private final SplitEnumeratorContext<TdSqlSplit> context;
    private final Function<TdSqlSet, MySqlSourceConfig> sourceConfigFunction;
    private final Map<TdSqlSet, MySqlSplitAssigner> tdSqlAssigners;
    private final int currentParallelism;
    private final Map<Integer, List<TdSqlSet>> readerRef;
    private final Map<TdSqlSet, List<List<FinishedSnapshotSplitInfo>>> binlogSplitMeta;

    private final Set<Integer> readersAwaitingSplit;

    public TdSqlSourceEnumerator(
            SplitEnumeratorContext<TdSqlSplit> context,
            Function<TdSqlSet, MySqlSourceConfig> sourceConfigFunction,
            Map<TdSqlSet, MySqlSplitAssigner> tdSqlAssigners) {
        this.context = context;
        this.sourceConfigFunction = sourceConfigFunction;
        this.tdSqlAssigners = tdSqlAssigners;
        this.currentParallelism = context.currentParallelism();
        this.readerRef = new HashMap<>(context.currentParallelism());
        this.binlogSplitMeta = new HashMap<>();
        this.readersAwaitingSplit = new HashSet<>();
    }

    @Override
    public void start() {
        int size = tdSqlAssigners.size();
        int index = 0;
        for (TdSqlSet set : tdSqlAssigners.keySet()) {
            int subtaskId = index % currentParallelism;
            List<TdSqlSet> partitionSet = readerRef.getOrDefault(subtaskId, new ArrayList<>(size));
            partitionSet.add(set);
            readerRef.put(subtaskId, partitionSet);
            index++;
        }

        LOG.trace("dispatch rule: {}", JSON.toString(readerRef));
        tdSqlAssigners.values().forEach(MySqlSplitAssigner::open);

        context.callAsync(this::getRegisteredReader, this::syncWithReaders);
    }

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
        if (!context.registeredReaders().containsKey(subtaskId)) {
            // reader failed between sending the request and now. skip this request.
            return;
        }
        readersAwaitingSplit.add(subtaskId);
        prepareAssign();
    }

    @Override
    public void addSplitsBack(List<TdSqlSplit> splits, int subtaskId) {
        LOG.info("TdSql Source Enumerator adds splits back: {}", splits);

        splits.stream()
                .collect(
                        Collectors.groupingBy(
                                TdSqlSplit::setInfo,
                                Collectors.mapping(TdSqlSplit::mySqlSplit, Collectors.toList())))
                .forEach((k, v) -> tdSqlAssigners.get(k).addSplits(v));
        readersAwaitingSplit.add(subtaskId);
        prepareAssign();
    }

    private void prepareAssign() {
        Iterator<Integer> readers = readersAwaitingSplit.iterator();
        while (readers.hasNext()) {
            Integer subtaskId = readers.next();

            if (!context.registeredReaders().containsKey(subtaskId)) {
                readers.remove();
                continue;
            }
            if (assignSplits(subtaskId)) {
                readers.remove();
            }
        }
    }

    @Override
    public void addReader(int subtaskId) {}

    @Override
    public void handleSourceEvent(int subtaskId, SourceEvent sourceEvent) {
        LOG.info("receive subtask {} source event.", subtaskId);
        TdSqlSourceEvent tdSqlSourceEvent = (TdSqlSourceEvent) sourceEvent;
        SourceEvent mySqlSourceEvent = tdSqlSourceEvent.getMySqlEvent();
        TdSqlSet tdSqlSet = tdSqlSourceEvent.getSet();

        if (mySqlSourceEvent instanceof FinishedSnapshotSplitsReportEvent) {
            FinishedSnapshotSplitsReportEvent reportEvent =
                    (FinishedSnapshotSplitsReportEvent) mySqlSourceEvent;
            Map<String, BinlogOffset> finishedOffsets = reportEvent.getFinishedOffsets();
            LOG.info(
                    "finished split reader in set {} offset: {}",
                    tdSqlSet.getSetKey(),
                    JSON.toString(finishedOffsets));
            tdSqlAssigners.get(tdSqlSet).onFinishedSplits(finishedOffsets);

            FinishedSnapshotSplitsAckEvent ackEvent =
                    new FinishedSnapshotSplitsAckEvent(new ArrayList<>(finishedOffsets.keySet()));
            context.sendEventToSourceReader(subtaskId, new TdSqlSourceEvent(ackEvent, tdSqlSet));
        } else if (mySqlSourceEvent instanceof BinlogSplitMetaRequestEvent) {
            LOG.trace(
                    "handle BinlogSplitMetaRequestEvent... subtaskId {}, split id {}",
                    subtaskId,
                    ((BinlogSplitMetaRequestEvent) mySqlSourceEvent).getSplitId());
            sendBinlogMeta(subtaskId, (BinlogSplitMetaRequestEvent) mySqlSourceEvent, tdSqlSet);
        }
    }

    private Map<String, BinlogOffset> asMySqlBinlogOffset(
            TdSqlSet set, Map<String, BinlogOffset> finishedOffsets) {
        Map<String, BinlogOffset> removeSetInfoFinishedOffset =
                new HashMap<>(finishedOffsets.size());

        int setInfoOffset = set.getSetKey().length();
        for (String tdSqlSplitId : finishedOffsets.keySet()) {
            removeSetInfoFinishedOffset.put(
                    tdSqlSplitId.substring(setInfoOffset), finishedOffsets.get(tdSqlSplitId));
        }

        return removeSetInfoFinishedOffset;
    }

    private boolean assignSplits(int subtaskId) {
        List<TdSqlSet> sets = readerRef.get(subtaskId);

        for (TdSqlSet set : sets) {
            Optional<MySqlSplit> split = tdSqlAssigners.get(set).getNext();

            if (split.isPresent()) {
                TdSqlSplit tdSqlSplit = new TdSqlSplit(set, split.get());
                LOG.info("Assign split {} to subtask {}", tdSqlSplit, subtaskId);
                context.assignSplit(tdSqlSplit, subtaskId);
                return true;
            } else {
                LOG.info(
                        "Finished Assign split to subtask {} in set {}",
                        subtaskId,
                        set.getSetKey());
            }
        }
        return false;
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        tdSqlAssigners.values().forEach(s -> s.notifyCheckpointComplete(checkpointId));
        // binlog split may be available after checkpoint complete.
        // checkpoint mark snapshotAssigner change assign status.
        prepareAssign();
    }

    @Override
    public TdSqlPendingSplitsState snapshotState(long checkpointId) throws Exception {
        Map<TdSqlSet, PendingSplitsState> stateMap = new HashMap<>(tdSqlAssigners.size());

        for (Map.Entry<TdSqlSet, MySqlSplitAssigner> assignerEntry : tdSqlAssigners.entrySet()) {
            stateMap.put(
                    assignerEntry.getKey(), assignerEntry.getValue().snapshotState(checkpointId));
        }
        return new TdSqlPendingSplitsState(stateMap);
    }

    @Override
    public void close() throws IOException {
        tdSqlAssigners.values().forEach(MySqlSplitAssigner::close);
    }

    private int[] getRegisteredReader() {
        return this.context.registeredReaders().keySet().stream()
                .mapToInt(Integer::intValue)
                .toArray();
    }

    private void syncWithReaders(int[] subtaskIds, Throwable t) {
        if (t != null) {
            throw new FlinkRuntimeException("Failed to list obtain registered readers due to:", t);
        }
        // when the SourceEnumerator restores or the communication failed between
        // SourceEnumerator and SourceReader, it may missed some notification event.
        // tell all SourceReader(s) to report there finished but unacked splits.
        for (int subtaskId : subtaskIds) {
            List<TdSqlSet> partitions = readerRef.get(subtaskId);

            for (TdSqlSet set : partitions) {
                MySqlSplitAssigner assigner = tdSqlAssigners.get(set);
                if (assigner.waitingForFinishedSplits()) {
                    LOG.trace(
                            "set {} had waiting for finished split. trigger FinishedSnapshotSplitsRequestEvent",
                            set.getSetKey());
                    context.sendEventToSourceReader(
                            subtaskId, new FinishedSnapshotSplitsRequestEvent());
                }
            }
        }
    }

    private void sendBinlogMeta(
            int subTask, BinlogSplitMetaRequestEvent requestEvent, TdSqlSet tdSqlSet) {
        List<List<FinishedSnapshotSplitInfo>> metas = binlogSplitMeta.get(tdSqlSet);

        if (metas == null) {
            final List<FinishedSnapshotSplitInfo> finishedSnapshotSplitInfos =
                    tdSqlAssigners.get(tdSqlSet).getFinishedSplitInfos();
            if (finishedSnapshotSplitInfos.isEmpty()) {
                LOG.error(
                        "The assigner offer empty finished split information, this should not happen");
                throw new FlinkRuntimeException(
                        "The assigner offer empty finished split information, this should not happen");
            }
            metas =
                    Lists.partition(
                            finishedSnapshotSplitInfos,
                            sourceConfigFunction.apply(tdSqlSet).getSplitMetaGroupSize());
            binlogSplitMeta.put(tdSqlSet, metas);
        }
        final int requestMetaGroupId = requestEvent.getRequestMetaGroupId();
        if (metas.size() > requestMetaGroupId) {
            List<FinishedSnapshotSplitInfo> metaToSend = metas.get(requestMetaGroupId);
            BinlogSplitMetaEvent metadataEvent =
                    new BinlogSplitMetaEvent(
                            requestEvent.getSplitId(),
                            requestMetaGroupId,
                            metaToSend.stream()
                                    .map(FinishedSnapshotSplitInfo::serialize)
                                    .collect(Collectors.toList()));
            context.sendEventToSourceReader(subTask, new TdSqlSourceEvent(metadataEvent, tdSqlSet));
        } else {
            LOG.error(
                    "Received invalid request meta group id {}, the invalid meta group id range is [0, {}]",
                    requestMetaGroupId,
                    binlogSplitMeta.size() - 1);
        }
    }
}
