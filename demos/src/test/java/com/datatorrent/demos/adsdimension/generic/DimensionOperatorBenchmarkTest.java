package com.datatorrent.demos.adsdimension.generic;

import com.datatorrent.api.LocalMode;
import com.datatorrent.demos.adsdimension.Application;
import com.datatorrent.demos.adsdimension.HDSApplicationTest;
import com.datatorrent.lib.util.ObjectMapperString;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.apache.hadoop.conf.Configuration;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

public class DimensionOperatorBenchmarkTest
{
  private static final Logger LOG = LoggerFactory.getLogger(DimensionOperatorBenchmarkTest.class);

  @Test
  public void test() throws Exception
  {
    DimensionOperatorBenchmark app = new DimensionOperatorBenchmark();
    LocalMode lma = LocalMode.newInstance();
    app.populateDAG(lma.getDAG(), new Configuration(false));
    LocalMode.Controller lc = lma.getController();
    lc.setHeartbeatMonitoringEnabled(false);

    ObjectMapper propertyObjectMapper = new ObjectMapper();
    propertyObjectMapper.configure(JsonGenerator.Feature.WRITE_NUMBERS_AS_STRINGS, true);
    propertyObjectMapper.configure(SerializationConfig.Feature.FAIL_ON_EMPTY_BEANS, false);

    // Test that DAG can be serialized to JSON by by Gateway
    ObjectMapperString str = new ObjectMapperString(
            propertyObjectMapper.writeValueAsString(
                    lma.getDAG().getOperatorMeta("DimensionsComputation").getOperator()));
    LOG.debug("DimensionsComputation representation: {}", str);


    lc.run(5000);


  }


}
