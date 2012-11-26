package trident.cassandra;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import me.prettyprint.cassandra.model.ConfigurableConsistencyLevel;
import me.prettyprint.cassandra.serializers.BytesArraySerializer;
import me.prettyprint.cassandra.serializers.CompositeSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.CassandraHostConfigurator;
import me.prettyprint.cassandra.service.spring.HectorTemplate;
import me.prettyprint.cassandra.service.spring.HectorTemplateImpl;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.Row;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.MultigetSliceQuery;
import storm.trident.state.JSONNonTransactionalSerializer;
import storm.trident.state.JSONOpaqueSerializer;
import storm.trident.state.JSONTransactionalSerializer;
import storm.trident.state.OpaqueValue;
import storm.trident.state.Serializer;
import storm.trident.state.State;
import storm.trident.state.StateFactory;
import storm.trident.state.StateType;
import storm.trident.state.TransactionalValue;
import storm.trident.state.map.CachedMap;
import storm.trident.state.map.IBackingMap;
import storm.trident.state.map.MapState;
import storm.trident.state.map.NonTransactionalMap;
import storm.trident.state.map.OpaqueMap;
import storm.trident.state.map.SnapshottableMap;
import storm.trident.state.map.TransactionalMap;
import backtype.storm.tuple.Values;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * @author slukjanov
 */
public class CassandraState<T> implements IBackingMap<T> {

    private static final Map<StateType, Serializer> DEFAULT_SERIALZERS = Maps.newHashMap();

    static {
        DEFAULT_SERIALZERS.put(StateType.NON_TRANSACTIONAL, new JSONNonTransactionalSerializer());
        DEFAULT_SERIALZERS.put(StateType.TRANSACTIONAL, new JSONTransactionalSerializer());
        DEFAULT_SERIALZERS.put(StateType.OPAQUE, new JSONOpaqueSerializer());
    }

    public static class Options<T> implements Serializable {
        private static final long serialVersionUID = 1L;
        
        public int localCacheSize = 5000;
        public String globalKey = "$__GLOBAL_KEY__$";
        public Serializer<T> serializer = null;
        public String clusterName = "trident-state";
        public int replicationFactor = 1;
        public String keyspace = "test";
        public String columnFamily = "column_family";
        public String rowKey = "row_key";
        
        /** Use these indicies for determining the row key instead of the rowKey option */
        public int[] rowKeyIndex;
        /** Use these indicies for determining the column names instead of a composite of all columns */
        public int[] columnNameIndex;
        
        public String rowKeyDelimiter = "\u0001";
        
        private transient Integer maxKeyIndex;
    }

    public static StateFactory opaque(String hosts) {
        return opaque(hosts, new Options<OpaqueValue>());
    }

    public static StateFactory opaque(String hosts, Options<OpaqueValue> opts) {
        return new Factory(StateType.OPAQUE, hosts, opts);
    }

    public static StateFactory transactional(String hosts) {
        return transactional(hosts, new Options<TransactionalValue>());
    }

    public static StateFactory transactional(String hosts, Options<TransactionalValue> opts) {
        return new Factory(StateType.TRANSACTIONAL, hosts, opts);
    }

    public static StateFactory nonTransactional(String hosts) {
        return nonTransactional(hosts, new Options<Object>());
    }

    public static StateFactory nonTransactional(String hosts, Options<Object> opts) {
        return new Factory(StateType.NON_TRANSACTIONAL, hosts, opts);
    }

    protected static class Factory implements StateFactory {
        private static final long serialVersionUID = 1L;
        private StateType stateType;
        private Serializer serializer;
        private String hosts;
        private Options options;

        public Factory(StateType stateType, String hosts, Options options) {
            this.stateType = stateType;
            this.hosts = hosts;
            this.options = options;
            serializer = options.serializer;

            if (serializer == null) {
                serializer = DEFAULT_SERIALZERS.get(stateType);
            }

            if (serializer == null) {
                throw new RuntimeException("Serializer should be specified for type: " + stateType);
            }
        }

        @SuppressWarnings("unchecked")
        @Override
        public State makeState(Map conf, int partitionIndex, int numPartitions) {
            CassandraState state = new CassandraState(hosts, options, serializer);

            CachedMap cachedMap = new CachedMap(state, options.localCacheSize);

            MapState mapState;
            if (stateType == StateType.NON_TRANSACTIONAL) {
                mapState = NonTransactionalMap.build(cachedMap);
            } else if (stateType == StateType.OPAQUE) {
                mapState = OpaqueMap.build(cachedMap);
            } else if (stateType == StateType.TRANSACTIONAL) {
                mapState = TransactionalMap.build(cachedMap);
            } else {
                throw new RuntimeException("Unknown state type: " + stateType);
            }

            return new SnapshottableMap(mapState, new Values(options.globalKey));
        }
    }

    private HectorTemplate hectorTemplate;
    private Options<T> options;
    private Serializer<T> serializer;

    public CassandraState(String hosts, Options<T> options, Serializer<T> serializer) {
        hectorTemplate = new HectorTemplateImpl(
                HFactory.getOrCreateCluster(options.clusterName, new CassandraHostConfigurator(hosts)),
                options.keyspace, options.replicationFactor, "org.apache.cassandra.locator.SimpleStrategy",
                new ConfigurableConsistencyLevel()
        );

        this.options = options;
        this.serializer = serializer;
    }

    @Override
    public List<T> multiGet(List<List<Object>> keys) {
        Collection<Composite> columnNames = toColumnNames(keys);
        Collection<String> rowKeys = toRowKeys(keys);

        MultigetSliceQuery<String, Composite, byte[]> query = hectorTemplate.createMultigetSliceQuery(
                StringSerializer.get(),
                CompositeSerializer.get(),
                BytesArraySerializer.get())
                .setColumnFamily(options.columnFamily)
                .setKeys(rowKeys)
                .setColumnNames(columnNames.toArray(new Composite[columnNames.size()]));
        
        Map<List<Object>, byte[]> resultMap = Maps.newHashMap();

        // List<HColumn<Composite, byte[]>> result = query.execute().get().getColumns();
        
        for (Row<String, Composite, byte[]> row : query.execute().get()) {
            String rowKey = row.getKey();
            List<Object> origRow = fromRowKey(rowKey);
            
            for (HColumn<Composite, byte[]> column : row.getColumnSlice().getColumns()) {
                Composite columnName = column.getName();
                List<Object> columnKey = Lists.newArrayListWithExpectedSize(columnName.size());
                for (int i = 0; i < columnName.size(); i++) {
                    columnKey.add(columnName.get(i, StringSerializer.get()));
                }
                
                if (options.rowKeyIndex != null && options.columnNameIndex != null) {
                    List<Object> fullKey = createEmptyKey();
                    int idx = 0;
                    for (int colIdx : options.columnNameIndex) {
                        fullKey.set(colIdx, columnKey.get(idx++));
                    }
                    idx = 0;
                    for (int rowIdx : options.rowKeyIndex) {
                        fullKey.set(rowIdx, origRow.get(idx++));
                    }
                    
                    resultMap.put(fullKey, column.getValue());
                } else {
                    // Fall back to using the full column list as the key
                    resultMap.put(columnKey, column.getValue());
                }
            }
        }
        



        List<T> values = Lists.newArrayListWithExpectedSize(keys.size());
        for (List<Object> key : keys) {
            byte[] bytes = resultMap.get(key);
            if (bytes != null) {
                values.add(serializer.deserialize(bytes));
            } else {
                values.add(null);
            }
        }

        return values;
    }

    protected List<Object> createEmptyKey()
    {
        if (options.maxKeyIndex == null) {
            int max = -1;
            for (int r : options.rowKeyIndex) {
                if (r > max) max = r;
            }
            for (int c : options.columnNameIndex) {
                if (c > max) max = c;
            }
            options.maxKeyIndex = max;
        }
        List<Object> fullKey = Lists.newArrayListWithExpectedSize(options.maxKeyIndex+1);
        Object[] tmp = new Object[options.maxKeyIndex+1];
        Arrays.fill(tmp, "");
        fullKey.addAll(Arrays.asList(tmp));
        return fullKey;
    }

    @Override
    public void multiPut(List<List<Object>> keys, List<T> values) {
        Mutator<String> mutator = hectorTemplate.createMutator(StringSerializer.get());

        for (int i = 0; i < keys.size(); i++) {
            List<Object> key = keys.get(i);
            Composite columnName = toColumnName(key);
            String rowKey = toRowKey(key);
            byte[] bytes = serializer.serialize(values.get(i));
            HColumn<Composite, byte[]> column = HFactory.createColumn(columnName, bytes);
            mutator.insert(rowKey, options.columnFamily, column);
        }

        mutator.execute();
    }

    private Collection<Composite> toColumnNames(List<List<Object>> keys) {
        return Collections2.transform(keys, new Function<List<Object>, Composite>() {
            @Override
            public Composite apply(List<Object> key) {
                return toColumnName(key);
            }
        });
    }

    private Composite toColumnName(List<Object> key) {
        Composite columnName = new Composite();
        
        if (options.columnNameIndex == null) {
            // Use all objects in the key as the column name
            for (Object component : key) {
                columnName.addComponent((String) component, StringSerializer.get());
            }
        } else {
            for (int idx : options.columnNameIndex) {
                columnName.addComponent(String.valueOf(key.get(idx)), StringSerializer.get());
            }
        }

        return columnName;
    }

    private Collection<String> toRowKeys(List<List<Object>> keys) {
        return Collections2.transform(keys, new Function<List<Object>, String>() {
            @Override
            public String apply(List<Object> key) {
                return toRowKey(key);
            }
        });
    }

    private String toRowKey(List<Object> key) {
        if (options.rowKeyIndex == null) {
            return options.rowKey;
        } else {
            StringBuilder row = new StringBuilder();
            for (int idx : options.rowKeyIndex) {
                if (row.length() != 0) {
                    row.append(options.rowKeyDelimiter);
                }
                row.append((String)key.get(idx));
            }
            return row.toString();
        }
    }
    
    private List<Object> fromRowKey(String key)
    {
        return Arrays.asList((Object[])key.split(options.rowKeyDelimiter));
    }
}
