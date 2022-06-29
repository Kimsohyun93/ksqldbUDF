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

  public static final Schema PARAM_SCHEMA = SchemaBuilder.struct().optional()
          .field("AVG", Schema.OPTIONAL_FLOAT64_SCHEMA)
          .build();

  public static final String PARAM_SCHEMA_DESCRIPTOR = "STRUCT<" +
          "AVG DOUBLE" +
          ">";

  public static final Schema AGGREGATE_SCHEMA = SchemaBuilder.struct().optional()
          .field("MIN", Schema.OPTIONAL_FLOAT64_SCHEMA)
          .field("MAX", Schema.OPTIONAL_FLOAT64_SCHEMA)
          .field("COUNT", Schema.OPTIONAL_FLOAT64_SCHEMA)
          .build();

  public static final String AGGREGATE_SCHEMA_DESCRIPTOR = "STRUCT<" +
          "MIN DOUBLE," +
          "MAX DOUBLE," +
          "COUNT DOUBLE" +
          ">";

  public static final Schema RETURN_SCHEMA = SchemaBuilder.struct().optional()
          .field("MIN", Schema.OPTIONAL_FLOAT64_SCHEMA)
          .field("MAX", Schema.OPTIONAL_FLOAT64_SCHEMA)
          .field("COUNT", Schema.OPTIONAL_FLOAT64_SCHEMA)
          .field("DIFFERENTIAL", Schema.OPTIONAL_FLOAT64_SCHEMA)
          .build();

  public static final String RETURN_SCHEMA_DESCRIPTOR = "STRUCT<" +
          "MIN DOUBLE," +
          "MAX DOUBLE," +
          "COUNT DOUBLE," +
          "DIFFERENTIAL DOUBLE" +
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
      return new HashMap<String, Double>();
    }

    @Override
    public Map<String, Double> aggregate(Struct newValue, Map<String, Double> aggregateValue) {
      Double c = newValue.getFloat64("C");

      return aggregateValue;
    }

    @Override
    public Struct map(Map<String, Double> intermediate) {
      Struct result = new Struct(RETURN_SCHEMA);

      Double min = intermediate.get("MIN");
      Double max = intermediate.get("MAX");


      return result;
    }

    @Override
    public Map<String, Double> merge(Map<String, Double> aggOne, Map<String, Double> aggTwo) {
      return aggOne;
    }

  }
}