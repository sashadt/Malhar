/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.lib.dedup;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;

import com.datatorrent.lib.bucket.Bucketable;
import com.datatorrent.lib.bucket.Event;
import com.datatorrent.lib.bucket.HdfsBucketStore;
import com.datatorrent.lib.bucket.NonOperationalBucketStore;

/**
 * {@link Deduper} that uses hdfs to store buckets.
 *
 * @since 0.9.5
 */
public abstract class DeduperWithHdfsStore<INPUT extends Bucketable & Event, OUTPUT> extends Deduper<INPUT, OUTPUT>
{
  @Override
  public void setup(Context.OperatorContext context)
  {
    boolean stateless = context.getValue(Context.OperatorContext.STATELESS);
    if (stateless) {
      bucketManager.setBucketStore(new NonOperationalBucketStore<INPUT>());
    }
    else {
      ((HdfsBucketStore<INPUT>) bucketManager.getBucketStore()).setConfiguration(context.getId(), context.getValue(DAG.APPLICATION_PATH), partitionKeys, partitionMask);
    }
    super.setup(context);
  }
}
