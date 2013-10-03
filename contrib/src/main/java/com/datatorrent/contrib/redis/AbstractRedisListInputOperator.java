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
package com.datatorrent.contrib.redis;

import com.datatorrent.api.annotation.ShipContainingJars;
import com.datatorrent.api.ActivationListener;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.Context.OperatorContext;
import com.lambdaworks.redis.RedisAsyncConnection;
import com.lambdaworks.redis.RedisClient;
import com.lambdaworks.redis.RedisConnection;
import com.lambdaworks.redis.KeyValue;
import com.lambdaworks.redis.codec.RedisCodec;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import javax.validation.constraints.NotNull;


interface RedisReadStrategy<K,V> {
  V read(RedisAsyncConnection<K,V> conn, K[] keys, int timeoutMS);
}

class RedisBLPOP<K,V> implements RedisReadStrategy<K,V> {
  public V read(RedisAsyncConnection<K,V> conn, K[] keys, int timeoutMS)
  {
    V message = null;
    try {
      message = conn.blpop(timeoutMS, keys).get().value;
    } catch (InterruptedException e) {
      //logger.error(e.printStackTrace());
    } catch (ExecutionException e) {
      // TODO Auto-generated catch block
      //logger.error(e.printStackTrace());
    }
    return message;
  }
}

class RedisBRPOP<K,V> implements RedisReadStrategy<K,V> {
  public V read(RedisAsyncConnection<K,V> conn, K[] keys, int timeoutMS)
  {
    V message = null;
    try {
      message = conn.brpop(timeoutMS, keys).get().value;
    } catch (InterruptedException e) {
      //logger.error(e.printStackTrace());
    } catch (ExecutionException e) {
      // TODO Auto-generated catch block
      //logger.error(e.printStackTrace());
    }
    return message;
  }
}



/**
 * Redis input operator base.
 *
 * @since 0.3.5
 */
@ShipContainingJars(classes = {RedisClient.class})
public abstract class AbstractRedisListInputOperator<K,V> extends BaseOperator implements InputOperator, ActivationListener<OperatorContext>
{

  protected transient RedisClient redisClient;
  protected transient RedisAsyncConnection<K,V> redisConnection;
  private String host = "localhost";
  private int port = 6379;
  private int dbIndex = 0;
  private int timeoutMS = 0;
  @NotNull
  private K[] redisKeys = null;
  private static final int DEFAULT_BLAST_SIZE = 1000;
  private static final int DEFAULT_BUFFER_SIZE = 1024*1024;
  private int emitBatchSize = DEFAULT_BLAST_SIZE;
  private int bufferSize = DEFAULT_BUFFER_SIZE;
  transient private RedisReadStrategy<K,V> redisReadStrategy = new RedisBLPOP<K,V>();
  
  
  private volatile boolean running = false;
  transient ArrayBlockingQueue<V> holdingBuffer = new ArrayBlockingQueue<V>(bufferSize);
  
  public RedisClient getRedisClient()
  {
    return redisClient;
  }

  public RedisAsyncConnection<K, V> getRedisConnection()
  {
    return redisConnection;
  }

  public String getHost()
  {
    return host;
  }

  public int getPort()
  {
    return port;
  }

  public int getDbIndex()
  {
    return dbIndex;
  }

  public int getTimeoutMS()
  {
    return timeoutMS;
  }

  public K[] getRedisKeys()
  {
    return redisKeys;
  }

  public RedisReadStrategy<K, V> getRedisReadStrategy()
  {
    return redisReadStrategy;
  }



  //Get codec to use 
  public abstract RedisCodec<K,V> getCodec();
  
  public void setRedisReadStrategy(RedisReadStrategy<K,V> rrs)
  {
    this.redisReadStrategy = rrs;
  }

  public void setHost(String host)
  {
    this.host = host;
  }

  public void setPort(int port)
  {
    this.port = port;
  }

  public void setDbIndex(int index)
  {
    this.dbIndex = index;
  }

  public void setTimeoutMS(int timeout)
  {
    this.timeoutMS = timeout;
  }
  public void setRedisKeys(K...redisKeys)
  {
    this.redisKeys = redisKeys;
  }
  
//  @Min(1)
  public void setTupleBlast(int i)
  {
    this.emitBatchSize = i;
  }
  public void setBufferSize(int size) {
    this.bufferSize = size;
  }
  @Override
  public void setup(OperatorContext ctx)
  {
    redisClient = new RedisClient(host, port);
    redisConnection = redisClient.connectAsync(this.getCodec());
    redisConnection.select(dbIndex);
    redisConnection.setTimeout(timeoutMS, TimeUnit.MILLISECONDS);
    super.setup(ctx);
  }

  @Override
  public void teardown()
  {
    redisConnection.close();
    redisClient.shutdown();
  }
  
/**
 * start a thread receiving data,
 * and add into holdingBuffer
 * @param ctx
 */
@Override
public void activate(OperatorContext ctx)
  {
    new Thread()
    {
      @Override
      public void run()
      {
        running = true;
        while (running) {
          try {
            V message =  redisReadStrategy.read(redisConnection, redisKeys, timeoutMS);
            if (message != null) {
              holdingBuffer.add(message);
            }
          }
          catch (Exception e) {
//        logger.debug(e.toString());
            break;
          }
        }
      }
    }.start();
  }

  
  @Override
  public void deactivate()
  {
    running = false;
  }

  public abstract void emitTuple(V message);

  @Override
  public void emitTuples()
  {
    int ntuples = emitBatchSize;
    if (ntuples > holdingBuffer.size()) {
      ntuples = holdingBuffer.size();
    }
    for (int i = ntuples; i-- > 0;) {
      emitTuple(holdingBuffer.poll());
    }
  }
}
