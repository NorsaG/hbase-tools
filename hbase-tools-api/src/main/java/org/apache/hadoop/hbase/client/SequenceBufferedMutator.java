
package org.apache.hadoop.hbase.client;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ipc.RpcControllerFactory;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * Custom mutator for write's optimization
 * It will merge operations in memory so fewer data's will be sent to HBase
 * <p/>
 * Should be reworked for HBase 2.x API usages.
 **/
@Deprecated
@InterfaceStability.Unstable
public class SequenceBufferedMutator implements BufferedMutator {
    private static final Log LOG = LogFactory.getLog(BufferedMutator.class);
    private static final int UNSET = -1;

    private final ExceptionListener listener;

    private final TableName tableName;
    private final Configuration conf;
    private static ConnectionConfiguration connectionConfiguration;

    private final ConcurrentLinkedQueue<Mutation> writeAsyncBuffer = new ConcurrentLinkedQueue<>();
    private final AtomicLong currentWriteBufferSize = new AtomicLong(0);

    private long writeBufferSize;
    private final int maxKeyValueSize;

    private boolean closed = false;
    private final ExecutorService pool;

    private final AsyncProcess ap;

    private static void initTableConfiguration(Configuration configuration) {
        if (connectionConfiguration == null)
            connectionConfiguration = new ConnectionConfiguration(configuration);
    }

    public static SequenceBufferedMutator createMutator(Connection connection, TableName tn) {
        initTableConfiguration(connection.getConfiguration());

        BufferedMutatorParams params = new BufferedMutatorParams(tn);
        params.pool(HTable.getDefaultExecutor(connection.getConfiguration()));
        params.writeBufferSize(connectionConfiguration.getWriteBufferSize());
        params.maxKeyValueSize(connectionConfiguration.getMaxKeyValueSize());
        return new SequenceBufferedMutator((ClusterConnection) connection, ((ClusterConnection) connection).getAsyncProcess().connection.getRpcRetryingCallerFactory(), ((ClusterConnection) connection).getAsyncProcess().rpcFactory, params);
    }

    public static SequenceBufferedMutator createMutator(Connection connection, BufferedMutatorParams params) {
        initTableConfiguration(connection.getConfiguration());

        if (params.getPool() == null) {
            params.pool(HTable.getDefaultExecutor(connection.getConfiguration()));
        }
        if (params.getWriteBufferSize() == BufferedMutatorParams.UNSET) {
            params.writeBufferSize(connectionConfiguration.getWriteBufferSize());
        }
        if (params.getMaxKeyValueSize() == BufferedMutatorParams.UNSET) {
            params.maxKeyValueSize(connectionConfiguration.getMaxKeyValueSize());
        }
        return new SequenceBufferedMutator((ClusterConnection) connection, ((ClusterConnection) connection).getAsyncProcess().connection.getRpcRetryingCallerFactory(), ((ClusterConnection) connection).getAsyncProcess().rpcFactory, params);
    }

    SequenceBufferedMutator(ClusterConnection conn, RpcRetryingCallerFactory rpcCallerFactory, RpcControllerFactory rpcFactory, BufferedMutatorParams params) {
        if (conn == null || conn.isClosed()) {
            throw new IllegalArgumentException("Connection is null or closed.");
        }

        this.tableName = params.getTableName();
        this.conf = conn.getConfiguration();
        this.pool = params.getPool();
        this.listener = params.getListener();

        initTableConfiguration(conn.getConfiguration());

        this.writeBufferSize = params.getWriteBufferSize() != UNSET ? params.getWriteBufferSize() : connectionConfiguration.getWriteBufferSize();
        this.maxKeyValueSize = params.getMaxKeyValueSize() != UNSET ? params.getMaxKeyValueSize() : connectionConfiguration.getMaxKeyValueSize();

        ap = new AsyncProcess(conn, conf, rpcCallerFactory, rpcFactory);
    }

    @Override
    public TableName getName() {
        return tableName;
    }

    @Override
    public Configuration getConfiguration() {
        return conf;
    }

    @Override
    public void mutate(Mutation m) throws InterruptedIOException, RetriesExhaustedWithDetailsException {
        mutate(Collections.singletonList(m));
    }

    @Override
    public void mutate(List<? extends Mutation> ms) throws InterruptedIOException, RetriesExhaustedWithDetailsException {
//        if (closed) {
//            throw new IllegalStateException("Cannot put when the BufferedMutator is closed.");
//        }
//
//        long toAddSize = 0;
//        for (Mutation m : ms) {
//            if (m instanceof Put) {
//                validatePut((Put) m);
//            }
//            toAddSize += m.heapSize();
//        }
//
//        if (ap.hasError()) {
//            currentWriteBufferSize.addAndGet(toAddSize);
//            writeAsyncBuffer.addAll(ms);
//            flushMergedCommits(true);
//        } else {
//            currentWriteBufferSize.addAndGet(toAddSize);
//            writeAsyncBuffer.addAll(ms);
//        }
//
//        while (writeAsyncBuffer.size() > 0) {
//            flushMergedCommits(false);
//        }
    }

    public void validatePut(final Put put) throws IllegalArgumentException {
        ConnectionUtils.validatePut(put, maxKeyValueSize);
    }

    @Override
    public synchronized void close() throws IOException {
        try {
            if (this.closed) {
                return;
            }
            flushMergedCommits(true);
            this.pool.shutdown();
            boolean terminated;
            int loopCnt = 0;
            do {
                terminated = this.pool.awaitTermination(60, TimeUnit.SECONDS);
                loopCnt += 1;
                if (loopCnt >= 10) {
                    LOG.warn("close() failed to terminate pool after 10 minutes. Abandoning pool.");
                    break;
                }
            } while (!terminated);
        } catch (InterruptedException e) {
            LOG.warn("waitForTermination interrupted");
        } finally {
            this.closed = true;
        }
    }

    @Override
    public synchronized void flush() throws InterruptedIOException, RetriesExhaustedWithDetailsException {
        flushMergedCommits(true);
    }

    private long calcBufferSize(Map<ByteBuffer, LinkedList<MutationInfo>> mutations) {
        long size = 0;
        for (LinkedList<MutationInfo> list : mutations.values()) {
            for (MutationInfo info : list) {
                for (Map.Entry<ByteBuffer, Map<ByteBuffer, Cell>> c : info.getValues().entrySet()) {
                    for (Cell cell : c.getValue().values()) {
                        KeyValue kv = (KeyValue) cell;
                        size += kv.heapSize();
                    }
                }
            }
        }
        return size;
    }

    /**
     * Merge logic.
     *
     * @param synchronous
     * @throws InterruptedIOException
     * @throws RetriesExhaustedWithDetailsException
     */
    private void flushMergedCommits(boolean synchronous) throws InterruptedIOException, RetriesExhaustedWithDetailsException {
//        long dequeueSize = 0;
//        List<Mutation> bufferList = null;
//        Map<ByteBuffer, LinkedList<MutationInfo>> mutations = new LinkedHashMap<>();
//        try {
//            Mutation m;
//            while ((writeBufferSize <= 0 || dequeueSize < (writeBufferSize * 0.8) || synchronous) && (m = writeAsyncBuffer.poll()) != null) {
//                ByteBuffer key = ByteBuffer.wrap(m.row);
//
//                //todo tmp solution - flag will signalize about required recalculation
//                boolean needCalc = false;
//
//                // add mutation if not exists
//                if (!mutations.containsKey(key)) {
//                    MutationInfo info = new MutationInfo(m);
//                    mutations.put(key, new LinkedList<>());
//                    mutations.get(key).add(info);
//                    // calculation flag
//                    needCalc = true;
//                } else {
//
//                    // in case of Delete add it to the tail
//                    // (in that case we don't need to check values)
//                    if (m.getClass().equals(Delete.class)) {
//                        MutationInfo info = new MutationInfo(m);
//                        mutations.get(key).add(info);
//                        needCalc = true;
//                    } else {
//                        // overwise analyze last mutation
//                        MutationInfo lastInfo = mutations.get(key).getLast();
//                        if (m.getClass().equals(Delete.class)) {
//                            // in case of Delete add it to the tail
//                            MutationInfo info = new MutationInfo(m);
//                            mutations.get(key).add(info);
//                        } else {
//                            // or merge mutations
//                            propagatePut(lastInfo, m);
//                        }
//                    }
//                }
//                if (needCalc) {
//                    dequeueSize += calcBufferSize(mutations);
//                }
//            }
//
//            if (!synchronous && mutations.size() == 0) {
//                return;
//            }
//
//            bufferList = buildMutationsBuffer(mutations);
//            if (!synchronous) {
//                ap.submit(tableName, bufferList, true, null, false);
//                if (ap.hasError()) {
//                    LOG.debug(tableName + ": One or more of the operations have failed - waiting for all operation in progress to finish (successfully or not)");
//                }
//            }
//            if (synchronous || ap.hasError()) {
//                while (!bufferList.isEmpty()) {
//                    ap.submit(tableName, bufferList, true, null, false);
//                }
//                RetriesExhaustedWithDetailsException error = ap.waitForMaximumCurrentTasks(null);
//                if (error != null) {
//                    if (listener == null) {
//                        throw error;
//                    } else {
//                        this.listener.onException(error, this);
//                    }
//                }
//            }
//        } finally {
//            if (bufferList != null) {
//                writeAsyncBuffer.addAll(bufferList);
//            }
//        }
    }

    private void propagatePut(MutationInfo info, Mutation m) {
        for (NavigableMap.Entry<byte[], List<Cell>> entry : m.getFamilyCellMap().entrySet()) {
            ByteBuffer family = ByteBuffer.wrap(entry.getKey());
            if (!info.getValues().containsKey(family)) {
                Map<ByteBuffer, Cell> rowVal = entry.getValue().stream().collect(Collectors.toMap((c) -> ByteBuffer.wrap(CellUtil.cloneQualifier(c)), cell -> cell));
                info.getValues().put(family, rowVal);
            } else {
                Map<ByteBuffer, Cell> rowVal = info.getValues().get(family);
                for (Cell cell : entry.getValue()) {
                    rowVal.put(ByteBuffer.wrap(CellUtil.cloneQualifier(cell)), cell);
                }
            }
        }
    }

    private List<Mutation> buildMutationsBuffer(Map<ByteBuffer, LinkedList<MutationInfo>> mutations) {
        List<Mutation> list = new LinkedList<>();
        for (LinkedList<MutationInfo> mutList : mutations.values()) {
            for (MutationInfo mi : mutList) {
                list.add(mi.toMutation());
            }
        }
        return list;
    }

    @Deprecated
    public void setWriteBufferSize(long writeBufferSize) throws RetriesExhaustedWithDetailsException, InterruptedIOException {
        this.writeBufferSize = writeBufferSize;
        if (currentWriteBufferSize.get() > writeBufferSize) {
            flush();
        }
    }

    @Override
    public long getWriteBufferSize() {
        return this.writeBufferSize;
    }

    @Deprecated
    public List<Row> getWriteBuffer() {
        return Arrays.asList(writeAsyncBuffer.toArray(new Row[0]));
    }

    private static class MutationInfo {

        private final Class<? extends Mutation> kind;
        private final ByteBuffer key;
        private final Map<ByteBuffer, Map<ByteBuffer, Cell>> values = new HashMap<>();

        public MutationInfo(Mutation mutation) {
            this.kind = mutation.getClass();
            this.key = ByteBuffer.wrap(mutation.getRow());
            for (NavigableMap.Entry<byte[], List<Cell>> family : mutation.getFamilyCellMap().entrySet()) {
                ByteBuffer familyVal = ByteBuffer.wrap(family.getKey());
                values.put(familyVal, new HashMap<>());
                for (Cell cell : family.getValue()) {
                    values.get(familyVal).put(ByteBuffer.wrap(CellUtil.cloneQualifier(cell)), cell);
                }
            }
        }

        public Map<ByteBuffer, Map<ByteBuffer, Cell>> getValues() {
            return values;
        }

        public Mutation toMutation() {
            Mutation m;
            if (kind.equals(Put.class)) {
                m = new Put(key.array());
            } else if (kind.equals(Delete.class)) {
                m = new Delete(key.array());
            } else {
                throw new RuntimeException("Not implemented");
            }

            if (!values.isEmpty()) {
                m.setFamilyCellMap(new TreeMap<>(Bytes.BYTES_COMPARATOR));
                for (Map.Entry<ByteBuffer, Map<ByteBuffer, Cell>> family : values.entrySet()) {
                    m.getFamilyCellMap().put(family.getKey().array(), new ArrayList<>(family.getValue().values()));
                }
            }
            return m;
        }
    }
}