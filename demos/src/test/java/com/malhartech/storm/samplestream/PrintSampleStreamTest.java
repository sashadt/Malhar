/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.storm.samplestream;

import com.malhartech.stram.StramLocalCluster;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author Zhongjian Wang <zhongjian@malhar-inc.com>
 */
public class PrintSampleStreamTest
{
  public PrintSampleStreamTest()
  {
  }

  @Test
  public void testSomeMethod() throws Exception
  {
    PrintSampleStream topology = new PrintSampleStream();
    final StramLocalCluster lc = new StramLocalCluster(topology.getApplication(new Configuration(false)));

    new Thread("LocalClusterController")
    {
      @Override
      public void run()
      {
        try {
          Thread.sleep(10000);
        }
        catch (InterruptedException ex) {
        }

        lc.shutdown();
      }
    }.start();

    lc.run();
  }
}