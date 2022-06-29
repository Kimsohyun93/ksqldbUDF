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
          .field("MIN", Schema.OPTIONAL_INT64_SCHEMA)
          .field("MAX", Schema.OPTIONAL_INT64_SCHEMA)
          .field("COUNT", Schema.OPTIONAL_INT64_SCHEMA)
          .build();

  public static final String AGGREGATE_SCHEMA_DESCRIPTOR = "STRUCT<" +
          "MIN BIGINT," +
          "MAX BIGINT," +
          "COUNT BIGINT" +
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
  public static Udaf<Struct, Map<String, Double>, Struct> createUdaf() {
    return new StatsUdafImpl();
  }

  private static class StatsUdafImpl implements Udaf<Struct, Map<String, Double>, Struct> {

    @Override
    public Map<String, Double> initialize() {
      System.out.println("INITIALIZE Stats Data");
      final Map<String, Double> data = new HashMap<>();
      data.put("1900-01-01 00:00:00 +0900", 0.0);
      System.out.println(data);
      return data;
    }

    @Override
    public Map<String, Double> aggregate(Struct newValue, Map<String, Double> aggregateValue) {
      Double AVG = newValue.getFloat64("AVG");
      Double WSTART = newValue.getFloat64("WSTART");


      return aggregateValue;
    }

    @Override
    public Struct map(Map<String, Double> intermediate) {
      Struct result = new Struct(RETURN_SCHEMA);


      return result;
    }

    @Override
    public Map<String, Double> merge(Map<String, Double> aggOne, Map<String, Double> aggTwo) {
      return aggOne;
    }
//
//    private Long getMin(Struct aggregateValue) {
//      Long result = aggregateValue.getInt64("MIN");
//
//      if (result != null) {
//        return result;
//      } else {
//        return Long.MAX_VALUE;
//      }
//    }
//
//    private Long getMax(Struct aggregateValue) {
//      Long result = aggregateValue.getInt64("MAX");
//
//      if (result != null) {
//        return result;
//      } else {
//        return Long.MIN_VALUE;
//      }
//    }
//
//    private Long getCount(Struct aggregateValue) {
//      Long result = aggregateValue.getInt64("COUNT");
//
//      if (result != null) {
//        return result;
//      } else {
//        return 0L;
//      }
//    }
  }
}