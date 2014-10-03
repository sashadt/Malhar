package com.datatorrent.demos.adsdimension.generic;

import com.google.common.collect.Maps;
import junit.framework.Assert;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class GenericAggregateSerializerTest
{

  public static final String TEST_SCHEMA_JSON = "{\n" +
            "  \"fields\": {\"pubId\":\"java.lang.Integer\", \"adId\":\"java.lang.Integer\", \"adUnit\":\"java.lang.Integer\", \"clicks\":\"java.lang.Long\", \"timestamp\":\"java.lang.Long\"},\n" +
            "  \"dimensions\": [\"time=MINUTES\", \"time=MINUTES:adUnit\", \"time=MINUTES:adId\", \"time=MINUTES:pubId\", \"time=MINUTES:adId:adUnit\", \"time=MINUTES:pubId:adUnit\", \"time=MINUTES:pubId:adId\", \"time=MINUTES:pubId:adId:adUnit\"],\n" +
            "  \"aggregates\": { \"clicks\": \"sum\"},\n" +
            "  \"timestamp\": \"timestamp\"\n" +
            "}";

  /**
   * Return a EventDescrition object, to be used by operator to
   * perform aggregation, serialization and deserialization.
   * @return
   */
  public static EventSchema getEventSchema() {
    EventSchema eventSchema;
    try {
      eventSchema = EventSchema.createFromJSON(TEST_SCHEMA_JSON);
    } catch (Exception e) {
      throw new IllegalArgumentException("Failed to parse JSON input: " + TEST_SCHEMA_JSON, e);
    }

    return eventSchema;
  }
  private static final Logger LOG = LoggerFactory.getLogger(GenericAggregateSerializerTest.class);


  @Test
  public void test()
  {
    EventSchema eventSchema = getEventSchema();
    GenericAggregateSerializer ser = new GenericAggregateSerializer(eventSchema);

    LOG.debug("eventSchema: {}", eventSchema );

    LOG.debug("keySize: {}  valLen: {} ", eventSchema.getKeyLen(), eventSchema.getValLen() );

    /* prepare a object */
    Map<String, Object> eventMap = Maps.newHashMap();
    eventMap.put("timestamp", System.currentTimeMillis());
    eventMap.put("pubId", 1);
    eventMap.put("adUnit", 2);
    eventMap.put("adId", 3);
    eventMap.put("clicks", 10L);

    GenericAggregate event = new GenericAggregate(eventSchema.convertMapToGenericEvent(eventMap));

    /* serialize and deserialize object */
    byte[] keyBytes = ser.getKey(event);
    byte[] valBytes = ser.getValue(event);

    GenericAggregate o = ser.fromBytes(keyBytes, valBytes);

    org.junit.Assert.assertNotSame("deserialized", event, o);

    Assert.assertEquals(o, event);
    Assert.assertEquals("pubId", eventSchema.getKey(o, "pubId"), eventMap.get("pubId"));
    Assert.assertEquals("pubId", eventSchema.getKey(o, "adUnit"), eventMap.get("adUnit"));
    Assert.assertEquals("pubId", eventSchema.getKey(o, "adId"), eventMap.get("adId"));
    Assert.assertEquals("pubId", eventSchema.getKey(o, "clicks"), eventMap.get("clicks"));

    Assert.assertEquals("timestamp type ", o.get("timestamp").getClass(), Long.class);
    Assert.assertEquals("pubId type ", o.get("pubId").getClass(), Integer.class);
    Assert.assertEquals("adId type ", o.get("adId").getClass(), Integer.class);
    Assert.assertEquals("adUnit type ", o.get("adUnit").getClass(), Integer.class);
    Assert.assertEquals("click type ", o.get("clicks").getClass(), Long.class);
  }

  /* Test with missing fields, serialized with default values */
  @Test
  public void test1()
  {
    EventSchema eventSchema = getEventSchema();
    GenericAggregateSerializer ser = new GenericAggregateSerializer(eventSchema);

    LOG.debug("keySize: {}  valLen: {} ", eventSchema.getKeyLen(), eventSchema.getValLen() );

    /* prepare a object */
    GenericAggregate event = new GenericAggregate();
    event.fields.put("timestamp", System.currentTimeMillis());
    event.fields.put("pubId", 1);
    event.fields.put("adUnit", 2);
    event.fields.put("clicks", new Long(10));

    /* serialize and deserialize object */
    byte[] keyBytes = ser.getKey(event);
    byte[] valBytes = ser.getValue(event);

    GenericAggregate o = ser.fromBytes(keyBytes, valBytes);

    //Assert.assertEquals(o, event);
    Assert.assertEquals("pubId", o.get("pubId"), event.get("pubId"));
    Assert.assertEquals("pubId", o.get("adUnit"), event.get("adUnit"));
    Assert.assertEquals("pubId", event.get("adId"), null);
    Assert.assertEquals("pubId", o.get("adId"), 0);
    Assert.assertEquals("pubId", o.get("clicks"), event.get("clicks"));

    Assert.assertEquals("timestamp type ", o.get("timestamp").getClass(), Long.class);
    Assert.assertEquals("pubId type ", o.get("pubId").getClass(), Integer.class);
    Assert.assertEquals("adId type ", o.get("adId").getClass(), Integer.class);
    Assert.assertEquals("adUnit type ", o.get("adUnit").getClass(), Integer.class);
    Assert.assertEquals("click type ", o.get("clicks").getClass(), Long.class);
  }

}
