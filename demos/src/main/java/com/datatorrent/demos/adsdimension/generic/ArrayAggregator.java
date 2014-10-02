/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.demos.adsdimension.generic;

import com.datatorrent.lib.statistics.DimensionsComputation;
import com.google.common.collect.Lists;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

class ArrayAggregate implements DimensionsComputation.AggregateEvent
{
  ArrayAggregate() {
  }

  public Object[] keys;
  public Object[] aggregates;
  private int aggregatorIndex = 0;
  private EventSchema eventSchema;
  long timestamp;

  public ArrayAggregate(EventSchema eventSchema) {
    this.eventSchema = eventSchema;
  }

  public ArrayAggregate(EventSchema eventSchema, int aggregatorIndex) {
    this.eventSchema = eventSchema;
    this.aggregatorIndex = aggregatorIndex;
  }

  public EventSchema getEventSchema() {
    return eventSchema;
  }

  @Override
  public int getAggregatorIndex()
  {
    return aggregatorIndex;
  }

  public long getTimestamp()
  {
    return timestamp;
  }

  public void setTimestamp(long timestamp)
  {
    this.timestamp = timestamp;
  }

  @Override
  public boolean equals(Object o)
  {

    if (this == o) {
      return true;
    }
    if (!(o instanceof ArrayAggregate)) {
      return false;
    }

    ArrayAggregate that = (ArrayAggregate) o;

    for(int i = 0; i < keys.length; i++)
    {
      Object thisValue = keys[i];
      Object thatValue = that.keys[i];
      if (thisValue != null ? ! thisValue.equals(thatValue) : thatValue != null) {
        return false;
      }
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    int result = Arrays.hashCode(keys);
    result = 31 * result + (int) (timestamp ^ (timestamp >>> 32));
    return result;
  }

  @Override
  public String toString()
  {
    return "MapAggregate{" +
        "timestamp = " + timestamp +
        ", keys=" + eventSchema.keys.toString() +
        ", values=" + aggregates +
        ", aggregatorIndex=" + aggregatorIndex +
        '}';
  }
}

class ArrayEvent {
  Object[] keys;
  Object[] values;
  long timestamp;
}

public class ArrayAggregator implements DimensionsComputation.Aggregator<ArrayEvent, ArrayAggregate>
{
  private static final long serialVersionUID = 7636266873750826291L;
  private EventSchema eventSchema;
  private String dimension;
  private TimeUnit time;
  private final List<String> keys = Lists.newArrayList();
  private final List<Integer> keyIndexes = Lists.newArrayList();
  public ArrayAggregator() {}

  public ArrayAggregator(EventSchema eventSchema)
  {
    this.eventSchema = eventSchema;
  }

  public void init(String dimension)
  {
    String[] attributes = dimension.split(":");
    for (String attribute : attributes) {
      String[] keyval = attribute.split("=", 2);
      String key = keyval[0];
      if (key.equals("time")) {
        time = TimeUnit.valueOf(keyval[1]);
        continue;
      }
      keys.add(key);
    }
    this.dimension = dimension;

    for(int i = 0; i < eventSchema.keysWithoutTimestamp.size(); i++)
      if (keys.contains(eventSchema.keysWithoutTimestamp.get(i)))
        keyIndexes.add(i);
  }

  @Override
  public ArrayAggregate getGroup(ArrayEvent src, int aggregatorIndex)
  {
    ArrayAggregate aggr = new ArrayAggregate();
    aggr.keys = new Object[src.keys.length];
    for(int i : keyIndexes)
    {
      aggr.keys[i] = src.keys[i];
    }

    aggr.aggregates = new Object[src.values.length];

    // put converted timestamp as per unit specified in aggregation.
    aggr.timestamp = TimeUnit.MILLISECONDS.convert(time.convert(src.timestamp, TimeUnit.MILLISECONDS), time);

    return aggr;
  }

  @Override
  public int computeHashCode(ArrayEvent object)
  {
    int hashCode = 31;
    for(int i : keyIndexes)
    {
      hashCode = hashCode * 31 + object.keys[i].hashCode();
    }
    long ltime = time.convert(object.timestamp, TimeUnit.MILLISECONDS);
    hashCode = hashCode * 31 + (int)ltime;

    return hashCode;
  }

  @Override public boolean equals(ArrayEvent o1, ArrayEvent o2)
  {
    if (o1 == o2)
      return true;

    if (o1 == null || o2 == null)
      return false;

    long t1 = time.convert(o1.timestamp, TimeUnit.MILLISECONDS);
    long t2 = time.convert(o2.timestamp, TimeUnit.MILLISECONDS);
    if (t1 != t2)
      return false;

    for(int i : keyIndexes)
    {
        Object i1 = o1.keys[i];
        Object i2 = o2.keys[i];

        if (i1 == i2)
          continue;

        if (i1 == null || i2 == null)
          return false;

        if (i1.equals(i2))
          continue;

        return false;
    }
    return true;
  }


  @Override
  public void aggregate(ArrayAggregate dest, ArrayEvent src)
  {
    for(int i = 0; i < eventSchema.aggregateKeys.size(); i++) {
      Class type = eventSchema.getAggregateType(i);
      dest.aggregates[i] = apply(eventSchema.aggregateKeys.get(i), dest.aggregates[i], src.values[i]);
    }
  }


  @Override
  public void aggregate(ArrayAggregate dest, ArrayAggregate src)
  {
    for(int i = 0; i < eventSchema.aggregateKeys.size(); i++) {
      Class type = eventSchema.getAggregateType(i);
      dest.aggregates[i] = apply(eventSchema.aggregateKeys.get(i), dest.aggregates[i], src.aggregates[i]);
    }
  }

  /* Apply operator between multiple objects */
  private Object apply(String metric, Object o, Object o1)
  {
    //TODO define a class for each type of aggregation and
    // avoid if/else.
    if (eventSchema.aggregates.get(metric).equals("sum"))
    {
      if (eventSchema.fields.get(metric).equals(Integer.class)) {
        int val1 = (o != null) ? ((Number)o).intValue() : 0;
        int val2 = (o1 != null) ? ((Number)o1).intValue() : 0;
        return new Integer(val1 + val2);
      } else if (eventSchema.fields.get(metric).equals(Long.class)) {
        long val1 = (o != null) ? ((Number)o).longValue() : 0;
        long val2 = (o1 != null) ? ((Number)o1).longValue() : 0;
        return new Long(val1 + val2);
      } else if (eventSchema.fields.get(metric).equals(Double.class)) {
        double val1 = (o != null) ? ((Number)o).doubleValue() : 0;
        double val2 = (o1 != null) ? ((Number)o1).doubleValue() : 0;
        return new Double(val1 + val2);
      }
    }
    return null;
  }

  public EventSchema getEventSchema()
  {
    return eventSchema;
  }

  public String getDimension()
  {
    return dimension;
  }

  public TimeUnit getTime()
  {
    return time;
  }

  public List<String> getKeys()
  {
    return keys;
  }
}
