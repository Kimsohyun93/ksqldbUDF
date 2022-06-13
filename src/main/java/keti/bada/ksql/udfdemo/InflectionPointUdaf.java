package keti.bada.ksql.udfdemo;

import io.confluent.ksql.function.udaf.Udaf;
import io.confluent.ksql.function.udaf.UdafDescription;
import io.confluent.ksql.function.udaf.UdafFactory;

import java.util.HashMap;
import java.util.Map;

@UdafDescription(
        name = "inflection_points",
        description = "Example UDAF that computes some summary stats for a stream of doubles",
        version = "0.1.0-SNAPSHOT",
        author = "Shekhar"
)
public final class InflectionPointUdaf {
  private InflectionPointUdaf() {
  }

  @UdafFactory(description = "compute the slope and find the inflection point")
  public static Udaf<Double, Map<String, Double>, Map<String, Double>> createUdaf() {
    return new Udaf<Double, Map<String, Double>, Map<String, Double>>() {
      @Override
      public Map<String, Double> initialize() {

        System.out.println("INITIALIZE Stats Data");

        final Map<String, Double> stats = new HashMap<>();
        stats.put("mean", 0.0);
        stats.put("sample_size", 0.0);
        stats.put("sum", 0.0);

        System.out.println(stats);

        return stats;
      }


      @Override
      public Map<String, Double> aggregate(
              final Double newValue,
              final Map<String, Double> aggregateValue
      ) {
        System.out.println("AGGREGATE FUNCTION NEW VALUE");
        System.out.println(newValue);
        System.out.println("AGGREGATE FUNCTION AGGREGATE VALUE");
        System.out.println(aggregateValue);

        final Double sampleSize = 1.0 + aggregateValue
                .getOrDefault("sample_size", 0.0);

        final Double sum = newValue + aggregateValue
                .getOrDefault("sum", 0.0);

        // calculate the new aggregate
        aggregateValue.put("mean", sum / sampleSize);
        aggregateValue.put("sample_size", sampleSize);
        aggregateValue.put("sum", sum);
        return aggregateValue;
      }


      @Override
      public Map<String, Double> merge(
              final Map<String, Double> aggOne,
              final Map<String, Double> aggTwo
      ) {
        System.out.println("MERGE FUNCTION AGG ONE");
        System.out.println(aggOne);
        System.out.println("MERGE FUNCTION AGG TWO");
        System.out.println(aggTwo);
        final Double sampleSize =
                aggOne.getOrDefault("sample_size", 0.0) + aggTwo.getOrDefault("sample_size", 0.0);
        final Double sum =
                aggOne.getOrDefault("sum", 0.0) + aggTwo.getOrDefault("sum", 0.0);

        // calculate the new aggregate
        final Map<String, Double> newAggregate = new HashMap<>();
        newAggregate.put("mean", sum / sampleSize);
        newAggregate.put("sample_size", sampleSize);
        newAggregate.put("sum", sum);
        return newAggregate;
      }

      @Override
      public Map<String, Double> map(final Map<String, Double> agg) {
        return agg;
      }
    };
  }
}
