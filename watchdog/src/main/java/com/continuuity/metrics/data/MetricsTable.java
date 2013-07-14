/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.metrics.data;

import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.OperationException;
import com.continuuity.common.utils.ImmutablePair;
import com.continuuity.data.operation.executor.omid.memory.MemoryReadPointer;
import com.continuuity.data.table.OrderedVersionedColumnarTable;
import com.continuuity.data.table.Scanner;
import com.continuuity.metrics.MetricsConstants;
import com.continuuity.metrics.transport.MetricsRecord;
import com.continuuity.metrics.transport.TagMetric;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Table;
import com.google.common.collect.TreeBasedTable;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FuzzyRowFilter;
import org.apache.hadoop.hbase.util.Pair;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Table for storing metric.
 * <p>
 * Row key:
 * {@code context|metricName|tags|timebase|runId}
 * </p>
 * <p>
 * TODO: More doc.
 * </p>
 */
public final class MetricsTable {

  private static final int MAX_ROLL_TIME = 0xfffe;
  private static final byte[] FOUR_ZERO_BYTES = {0, 0, 0, 0};

  private final OrderedVersionedColumnarTable metricTable;
  private final MetricsEntityCodec entityCodec;
  private final boolean isFilterable;
  private final int resolution;
  private final int rollTimebaseInterval;
  private final ImmutablePair<byte[], byte[]> defaultTagFuzzyPair;

  // Cache for delta values.
  private final byte[][] deltaCache;

  /**
   * Creates a MetricTable. Same as calling
   * {@link #MetricsTable(EntityTable, com.continuuity.data.table.OrderedVersionedColumnarTable,
   * int, int, int, int, int)}
   * with
   * <p>
   * {@code contextDepth = }{@link com.continuuity.metrics.MetricsConstants#DEFAULT_CONTEXT_DEPTH}
   * </p>
   * <p>
   * {@code metricDepth = }{@link com.continuuity.metrics.MetricsConstants#DEFAULT_METRIC_DEPTH}
   * </p>
   * <p>
   * {@code tagDepth = }{@link com.continuuity.metrics.MetricsConstants#DEFAULT_TAG_DEPTH}
   * </p>
   */
  public MetricsTable(EntityTable entityTable, OrderedVersionedColumnarTable metricTable,
                      int resolution, int rollTime) {
    this(entityTable, metricTable,
         MetricsConstants.DEFAULT_CONTEXT_DEPTH,
         MetricsConstants.DEFAULT_METRIC_DEPTH,
         MetricsConstants.DEFAULT_TAG_DEPTH,
         resolution, rollTime);
  }

  /**
   * Creates a MetricTable.
   *
   * @param entityTable For lookup from entity name to uniqueID.
   * @param metricTable A OVC table for storing metric information.
   * @param contextDepth Maximum level in context
   * @param metricDepth Maximum level in metric
   * @param tagDepth Maximum level in tag
   * @param resolution Resolution in second of the table
   * @param rollTime Number of resolution for writing to a new row with a new timebase.
   *                 Meaning the differences between timebase of two consecutive rows divided by
   *                 resolution seconds. It essentially defines how many columns per row in the table.
   *                 This value should be < 65535.
   */
  public MetricsTable(EntityTable entityTable, OrderedVersionedColumnarTable metricTable,
                      int contextDepth, int metricDepth, int tagDepth,
                      int resolution, int rollTime) {

    this.metricTable = metricTable;
    this.entityCodec = new MetricsEntityCodec(entityTable, contextDepth, metricDepth, tagDepth);
    this.isFilterable = metricTable instanceof FilterableOVCTable;
    this.resolution = resolution;

    // Two bytes for column name, which is a delta timestamp
    Preconditions.checkArgument(rollTime <= MAX_ROLL_TIME, "Rolltime should be <= " + MAX_ROLL_TIME);
    this.rollTimebaseInterval = rollTime * resolution;
    this.deltaCache = createDeltaCache(rollTime);

    this.defaultTagFuzzyPair = createDefaultTagFuzzyPair();
  }

  /**
   * Saves a collection of {@link com.continuuity.metrics.transport.MetricsRecord}.
   */
  public void save(Iterable<MetricsRecord> records) throws OperationException {
    save(records.iterator());
  }

  public void save(Iterator<MetricsRecord> records) throws OperationException {
    // Simply collecting all rows/cols/values that need to be put to the underlying table.
    Table<byte[], byte[], byte[]> table = TreeBasedTable.create(Bytes.BYTES_COMPARATOR, Bytes.BYTES_COMPARATOR);

    while (records.hasNext()) {
      getUpdates(records.next(), table);
    }

    // Covert the table into the format needed by the put method.
    Map<byte[], Map<byte[], byte[]>> rowMap = table.rowMap();
    byte[][] rows = new byte[rowMap.size()][];
    byte[][][] columns = new byte[rowMap.size()][][];
    byte[][][] values = new byte[rowMap.size()][][];

    int rowIdx = 0;
    for (Map.Entry<byte[], Map<byte[], byte[]>> rowEntry : rowMap.entrySet()) {
      rows[rowIdx] = rowEntry.getKey();
      Map<byte[], byte[]> colMap = rowEntry.getValue();
      columns[rowIdx] = new byte[colMap.size()][];
      values[rowIdx] = new byte[colMap.size()][];

      int colIdx = 0;
      for (Map.Entry<byte[], byte[]> colEntry : colMap.entrySet()) {
        columns[rowIdx][colIdx] = colEntry.getKey();
        values[rowIdx][colIdx] = colEntry.getValue();
        colIdx++;
      }
      rowIdx++;
    }

    metricTable.put(rows, columns, System.currentTimeMillis(), values);
  }

  public MetricsScanner scan(MetricsScanQuery query) {
    int startTimeBase = getTimeBase(query.getStartTime());
    int endTimeBase = getTimeBase(query.getEndTime());

    byte[] startRow = getPaddedKey(query.getContextPrefix(), query.getRunId(),
                                   query.getMetricPrefix(), query.getTagPrefix(), startTimeBase, 0);
    byte[] endRow = getPaddedKey(query.getContextPrefix(), query.getRunId(),
                                 query.getMetricPrefix(), query.getTagPrefix(), endTimeBase + 1, 0xff);

    Scanner scanner;
    if (isFilterable) {
      scanner = ((FilterableOVCTable) metricTable).scan(startRow, endRow,
                                                        MemoryReadPointer.DIRTY_READ,
                                                        getFilter(query, startTimeBase, endTimeBase));
    } else {
      scanner = metricTable.scan(startRow, endRow, MemoryReadPointer.DIRTY_READ);
    }
    return new MetricsScanner(query, scanner, entityCodec, resolution);
  }

  /**
   * Setups all rows, columns and values for updating the metric table.
   */
  private void getUpdates(MetricsRecord record, Table<byte[], byte[], byte[]> table) {
    long timestamp = record.getTimestamp() / resolution * resolution;
    int timeBase = getTimeBase(timestamp);

    // Key for the no tag one
    byte[] rowKey = getKey(record.getContext(), record.getRunId(), record.getName(), null, timeBase);

    // delta is guaranteed to be 2 bytes.
    byte[] column = deltaCache[(int) (timestamp - timeBase)];

    table.put(rowKey, column, Bytes.toBytes(record.getValue()));

    // Save tags metrics
    for (TagMetric tag : record.getTags()) {
      rowKey = getKey(record.getContext(), record.getRunId(), record.getName(), tag.getTag(), timeBase);
      table.put(rowKey, column, Bytes.toBytes(tag.getValue()));
    }
  }

  /**
   * Creates the row key for the given context, metric, tag, and timebase.
   */
  private byte[] getKey(String context, String runId, String metric, String tag, int timeBase) {
    Preconditions.checkArgument(context != null, "Context cannot be null.");
    Preconditions.checkArgument(runId != null, "RunId cannot be null.");
    Preconditions.checkArgument(metric != null, "Metric cannot be null.");

    return concatBytes(entityCodec.encode(MetricsEntityType.CONTEXT, context),
                       entityCodec.encode(MetricsEntityType.METRIC, metric),
                       entityCodec.encode(MetricsEntityType.TAG, tag == null ? MetricsConstants.EMPTY_TAG : tag),
                       Bytes.toBytes(timeBase),
                       entityCodec.encode(MetricsEntityType.RUN, runId));
  }

  private byte[] getPaddedKey(String contextPrefix, String runId, String metricPrefix, String tagPrefix,
                              int timeBase, int padding) {

    Preconditions.checkArgument(metricPrefix != null, "Metric cannot be null.");

    // If there is no runId, just applies the padding
    return concatBytes(
      entityCodec.paddedEncode(MetricsEntityType.CONTEXT, contextPrefix, padding),
      entityCodec.paddedEncode(MetricsEntityType.METRIC, metricPrefix, padding),
      entityCodec.paddedEncode(MetricsEntityType.TAG,
                               tagPrefix == null ? MetricsConstants.EMPTY_TAG : tagPrefix, padding),
      Bytes.toBytes(timeBase),
      entityCodec.paddedEncode(MetricsEntityType.RUN, runId, padding));
  }

  private Filter getFilter(MetricsScanQuery query, long startTimeBase, long endTimeBase) {
    String tag = query.getTagPrefix();

    // Create fuzzy row filter
    ImmutablePair<byte[], byte[]> contextPair = entityCodec.paddedFuzzyEncode(MetricsEntityType.CONTEXT,
                                                                              query.getContextPrefix(), 0);
    ImmutablePair<byte[], byte[]> metricPair = entityCodec.paddedFuzzyEncode(MetricsEntityType.METRIC,
                                                                             query.getMetricPrefix(), 0);
    ImmutablePair<byte[], byte[]> tagPair = (tag == null) ? defaultTagFuzzyPair
                                                          : entityCodec.paddedFuzzyEncode(MetricsEntityType.TAG, tag, 0);
    ImmutablePair<byte[], byte[]> runIdPair = entityCodec.paddedFuzzyEncode(MetricsEntityType.RUN, query.getRunId(), 0);

    // For each timbase, construct a fuzzy filter pair
    List<Pair<byte[], byte[]>> fuzzyPairs = Lists.newLinkedList();
    for (long timeBase = startTimeBase; timeBase <= endTimeBase; timeBase += this.rollTimebaseInterval) {
      fuzzyPairs.add(Pair.newPair(concatBytes(contextPair.getFirst(), metricPair.getFirst(), tagPair.getFirst(),
                                              Bytes.toBytes((int) timeBase), runIdPair.getFirst()),
                                  concatBytes(contextPair.getSecond(), metricPair.getSecond(), tagPair.getSecond(),
                                              FOUR_ZERO_BYTES, runIdPair.getSecond())));
    }

    return new FuzzyRowFilter(fuzzyPairs);
  }

  /**
   * Returns timebase computed with the table setting for the given timestamp.
   */
  private int getTimeBase(long time) {
    // We are using 4 bytes timebase for row
    long timeBase = time / rollTimebaseInterval * rollTimebaseInterval;
    Preconditions.checkArgument(timeBase < 0x100000000L, "Timestamp is too large.");
    return (int) timeBase;
  }


  private byte[][] createDeltaCache(int rollTime) {
    byte[][] deltas = new byte[rollTime + 1][];

    for (int i = 0; i <= rollTime; i++) {
      deltas[i] = Bytes.toBytes((short) i);
    }
    return deltas;
  }

  private byte[] concatBytes(byte[]...array) {
    return com.google.common.primitives.Bytes.concat(array);
  }

  private ImmutablePair<byte[], byte[]> createDefaultTagFuzzyPair() {
    byte[] key = entityCodec.encode(MetricsEntityType.TAG, MetricsConstants.EMPTY_TAG);
    byte[] mask = new byte[key.length];
    Arrays.fill(mask, (byte) 0);
    return new ImmutablePair<byte[], byte[]>(key, mask);
  }
}

