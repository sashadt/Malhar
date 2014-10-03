package com.datatorrent.demos.adsdimension.generic;

import com.google.common.collect.Lists;

import java.util.List;

public class DimensionsGenerator
{
  private EventSchema eventSchema;

  public DimensionsGenerator(EventSchema eventSchema)
  {
    this.eventSchema = eventSchema;
  }


  public GenericAggregator[] generateAggregators()
  {
    if (eventSchema.keys.size() <= 0 ) return null;

    List<String> keys = Lists.newArrayList();

    for(String key : eventSchema.keys)
    {
      if (key.equals(eventSchema.getTimestamp()))
        continue;
      keys.add(key);
    }
    int numKeys = keys.size();
    int numDimensions = 1 << numKeys;
    GenericAggregator[] aggregators = new GenericAggregator[numDimensions];

    for(int i = 0; i < numDimensions; i++)
    {
      StringBuilder builder = new StringBuilder("time=MINUTES");
      aggregators[i] = new GenericAggregator(eventSchema);
      for(int k = 0; k < numKeys; k++)
      {
        if ((i & (1 << k)) != 0) {
          builder.append(':');
          builder.append(keys.get(k));
        }
      }
      aggregators[i].init(builder.toString());
    }

    return aggregators;
  }
}
