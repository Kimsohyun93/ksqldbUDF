package keti.bada.ksql.udfdemo;


import io.confluent.ksql.function.udaf.Udaf;
import io.confluent.ksql.function.udaf.UdafDescription;
import io.confluent.ksql.function.udaf.UdafFactory;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Schema;

import java.util.HashMap;
import java.util.Map;

@UdafDescription(name = "stats",
        author = "example user",
        version = "1.3.5",
        description = "Maintains statistical values.")
public class StatsUdaf {

  private static final String WSTART = "WSTART";
  private static final String AVG = "AVG";

  public static final Schema PARAM_SCHEMA = SchemaBuilder.struct().optional()
          .field(WSTART, Schema.OPTIONAL_STRING_SCHEMA)
          .field(AVG, Schema.OPTIONAL_FLOAT64_SCHEMA)
          .build();

  public static final String PARAM_SCHEMA_DESCRIPTOR = "STRUCT<" +
          "WSTART STRING" +
          "AVG DOUBLE" +
          ">";

  public static final Schema AGGREGATE_SCHEMA = SchemaBuilder.struct().optional()
          .field(WSTART, Schema.OPTIONAL_STRING_SCHEMA)
          .field(AVG, Schema.OPTIONAL_FLOAT64_SCHEMA)
          .build();
  public static final String AGGREGATE_SCHEMA_DESCRIPTOR = "STRUCT<" +
          "WSTART STRING" +
          "AVG DOUBLE" +
          ">";

  public static final Schema RETURN_SCHEMA = SchemaBuilder.struct().optional()
          .field(WSTART, Schema.OPTIONAL_STRING_SCHEMA)
          .build();

  public static final String RETURN_SCHEMA_DESCRIPTOR = "STRUCT<" +
          "WSTART STRING" +
          ">";


  private StatsUdaf() {
  }

  @UdafFactory(description = "Computes the min, max, count, and difference between min/max.",
          paramSchema = PARAM_SCHEMA_DESCRIPTOR,
          aggregateSchema = AGGREGATE_SCHEMA_DESCRIPTOR,
          returnSchema = RETURN_SCHEMA_DESCRIPTOR)
  public static Udaf<Struct, Struct, Struct> createUdaf() {
    return new StatsUdafImpl();
  }

  private static class StatsUdafImpl implements Udaf<Struct, Struct, Struct> {

    @Override
    public Struct initialize() {
      System.out.println("INITIALIZE Stats Data");
      final Struct data = new Struct(AGGREGATE_SCHEMA);
      data.put(WSTART, "1900-01-01 00:00:00 +0900");
      data.put(AVG, 0.0);
      System.out.println(data);
      return data;
    }

    @Override
    public Struct aggregate(Struct newValue, Struct aggregateValue) {
      Double avg = newValue.getFloat64("AVG");
      Double wstart = newValue.getFloat64("WSTART");

      aggregateValue.put(WSTART, wstart);
      aggregateValue.put(AVG, avg);

      return aggregateValue;
    }

    @Override
    public Struct map(Struct intermediate) {
      Struct result = new Struct(RETURN_SCHEMA);
      result.put("WSTART", intermediate.getString(WSTART));


      return result;
    }

    @Override
    public Struct merge(Struct aggOne, Struct aggTwo) {
      return aggOne;
    }

  }
}