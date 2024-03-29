package keti.bada.ksql.udfdemo;

import io.confluent.ksql.function.udaf.Udaf;
import io.confluent.ksql.function.udaf.UdafDescription;
import io.confluent.ksql.function.udaf.UdafFactory;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import java.util.*;

import static io.confluent.ksql.schema.ksql.types.SqlTypes.DOUBLE;
import static io.confluent.ksql.schema.ksql.types.SqlTypes.struct;


@UdafDescription(
        name = "inflection_points",
        description = "compute inflection points",
//        aggregateSchema="STRUCT<WSTART varchar, AVG double>",
        version = "0.1.0-SNAPSHOT",
        author = "shkim"
)
public final class InflectionPointUdaf {

  private static final String WSTART = "WSTART";
  private static final String AVG = "AVG";

  public static final Schema PARAM_SCHEMA = SchemaBuilder.struct().optional()
          .field(WSTART, Schema.OPTIONAL_STRING_SCHEMA)
          .field(AVG, Schema.OPTIONAL_FLOAT64_SCHEMA)
          .build();

  public static final String PARAM_SCHEMA_DESCRIPTOR = "STRUCT<" +
          "WSTART STRING," +
          "AVG DOUBLE" +
          ">";

  public static final Schema RETURN_SCHEMA = SchemaBuilder.struct().optional()
          .field(WSTART, Schema.OPTIONAL_STRING_SCHEMA)
          .build();

  public static final String RETURN_SCHEMA_DESCRIPTOR = "STRUCT<" +
          "WSTART STRING" +
          ">";

  private InflectionPointUdaf() {
  }

  @UdafFactory(description = "compute the slope and find the inflection points",
  paramSchema = PARAM_SCHEMA_DESCRIPTOR,
  returnSchema = RETURN_SCHEMA_DESCRIPTOR)
  public static Udaf<Struct, Map<String, Double>, Struct> createUdaf() {

    return new Udaf<Struct, Map<String, Double>, Struct>() {
      @Override
      public Map<String, Double> initialize() {

        System.out.println("INITIALIZE Stats Data");
        final Map<String, Double> data = new HashMap<>();
        data.put("1900-01-01 00:00:00 +0900", 0.0);
        System.out.println(data);
        return data;
      }


      @Override
      public Map<String, Double> aggregate(
              final Struct newValue,
              final Map<String, Double> aggregateValue
      ) {
        System.out.println("AGGREGATE FUNCTION NEW VALUE");
        System.out.println(newValue);

        final String startData = newValue.getString(WSTART);
        final Double avgData = newValue.getFloat64(AVG);

        aggregateValue.put(startData,avgData);

//        System.out.println("AGGREGATE FUNCTION AGGREGATE VALUE");
//        System.out.println(aggregateValue);

        return aggregateValue;
      }


      @Override
      public Map<String, Double> merge(
              final Map<String, Double> aggOne,
              final Map<String, Double> aggTwo
      ) {
        System.out.println("========== MERGE FUNCTION");
//
//        // 키로 정렬
//        String[] mapkeyOne = (String[]) aggOne.keySet().toArray();
//        Arrays.sort(mapkeyOne);
//        String[] mapkeyTwo = (String[]) aggTwo.keySet().toArray();
//        Arrays.sort(mapkeyTwo);
//
//        Map<String, Double> newAggregate = new HashMap<>();
//        if (mapkeyOne[0].compareTo(mapkeyTwo[0]) < 0){
//            // 사전적으로 one 이 앞에 있을 때
//          newAggregate = aggOne;
//          for(String key : aggTwo.keySet() ){
//            newAggregate.put(key, aggTwo.get(key));
//          }
//        }else {
//          newAggregate = aggTwo;
//          for(String key : aggOne.keySet() ){
//            newAggregate.put(key, aggOne.get(key));
//          }
//        }
//        return newAggregate;
        return aggOne;
      }

      @Override
      public Struct map(final Map<String, Double> agg) {

        Struct result = new Struct(RETURN_SCHEMA);
        // 키로 정렬
        Map<String, Double> sortedMap = new TreeMap<>(agg);
        int index=0;
        System.out.println("========== AGG KEYSET");
        System.out.println(sortedMap.keySet());

        String previous_key = "";
        Double previous_value = 0.0;
//        Map<String, Double> result = new HashMap<>(); // return
        Double previous_result = 0.0; // 내 이전 값의 차
        sortedMap.remove("1900-01-01 00:00:00 +0900");
        // inflection point 계산
        String wstartString = "";
        for ( Map.Entry<String, Double> elem : sortedMap.entrySet()){
          if(index == sortedMap.size() -1){
            break;
          }
          if (index == 0){
            previous_key = elem.getKey();
            previous_value = elem.getValue();
            index ++;
            continue;
          }
          Double present_result = elem.getValue() - previous_value;
          if(previous_result / present_result < 0){ // 부호 다름
            if(result.getString(WSTART) != null){
              wstartString = result.getString(WSTART);
              wstartString += previous_key + " ";
            }else{
              wstartString = previous_key + " ";
            }

            result.put(WSTART, wstartString);
          }
          previous_result = present_result;
          previous_key = elem.getKey();
          previous_value = elem.getValue();
          index ++;
        }
        System.out.println(result);
        return result;
      }
    };
  }
}
