/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
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
package com.datatorrent.lib.util;

import java.util.HashMap;
import org.apache.commons.lang.mutable.MutableInt;

/**
 * Count unique occurances of keys within a window<p>
 * <br>
 * <b>Benchmarks</b>: Blast as many tuples as possible in inline mode<br>
 * Operator processes > 110 million tuples/sec. Only one tuple per unique key is emitted on end of window, so this operator is not bound by outbound throughput<br>
 *
 * @since 0.3.2
 */
public class BaseUniqueKeyValueCounter<K,V> extends BaseKeyValueOperator<K,V>
{
  /**
   * Reference counts each tuple
   * @param key tuple key
   * @param val tuple value
   */
  public void processTuple(K key, V val)
  {
    HashMap<K,V> tuple = new HashMap<K,V>(1);
    tuple.put(key,val);
    MutableInt i = map.get(tuple);
    if (i == null) {
      i = new MutableInt(0);
      map.put(cloneTuple(tuple), i);
    }
    i.increment();
  }

  /**
   * Bucket counting mechanism.
   * Since we clear the bucket at the beginning of the window, we make this object transient.
   */
  protected HashMap<HashMap<K,V>, MutableInt> map = new HashMap<HashMap<K,V>, MutableInt>();
}
