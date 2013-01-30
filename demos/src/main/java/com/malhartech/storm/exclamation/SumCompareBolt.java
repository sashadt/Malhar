/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.storm.exclamation;

import com.malhartech.api.BaseOperator;
import com.malhartech.api.DefaultInputPort;
import com.malhartech.api.DefaultOutputPort;
import java.util.LinkedList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The result of rate should be closed to the pi/4 value which is approximately 0.785398
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class SumCompareBolt extends BaseOperator
{
  public int inArea = 0;
  public int outArea = 0;
  private transient double rate = 0;
  final public int base = 30000 * 30000;
  private static final Logger logger = LoggerFactory.getLogger(SumCompareBolt.class);
  public transient LinkedList<Integer> list1 = new LinkedList<Integer>();
  public transient LinkedList<Integer> list2 = new LinkedList<Integer>();
  public final transient DefaultInputPort<Integer> input1 = new DefaultInputPort<Integer>(this)
  {
    @Override
    public void process(Integer num)
    {
      list1.add(num);
    }
  };
  public final transient DefaultInputPort<Integer> input2 = new DefaultInputPort<Integer>(this)
  {
    @Override
    public void process(Integer num)
    {
      list2.add(num);
    }
  };
  public final transient DefaultOutputPort<Integer> output = new DefaultOutputPort<Integer>(this);

  @Override
  public void beginWindow(long windowId)
  {
  }

  @Override
  public void endWindow()
  {
    int min = list1.size() > list2.size() ? list2.size() : list1.size();
    int num1, num2;
    double rate = 0;
    for (int i = 0; i < min; i++) {
      num1 = list1.poll();
      num2 = list2.poll();
      if (num1 + num2 <= base) {
        ++inArea;
      }
      else {
        ++outArea;
      }
      rate = (double)inArea / (inArea + outArea);
    }
    logger.debug("all:" + (inArea + outArea) + " in:" + inArea + " out:" + outArea + " rate:" + rate);
    list1.clear();
    list2.clear();
  }
}
