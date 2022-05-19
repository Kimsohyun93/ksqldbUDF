package keti.bada.ksql.udfdemo;


import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import io.confluent.ksql.function.udf.UdfParameter;

@UdfDescription(name = "multiply", description = "multiplies 2 numbers")
public class Multiply {

  @Udf(description = "multiply two non-nullable INTs.")
  public long multiply(@UdfParameter(value = "v1", description = "first value to multiply") int v1, @UdfParameter(value = "v2", description = "second value to multiply") int v2) {
    return v1 * v2;
  }

  @Udf(description = "multiply two nullable BIGINTs. If either param is null, null is returned.")
  public Long multiply(@UdfParameter(value = "v1", description = "first value to multiply") Long v1, @UdfParameter(value = "v2", description = "second value to multiply") Long v2) {
    return v1 == null || v2 == null ? null : v1 * v2;
  }

  @Udf(description = "multiply two non-nullable DOUBLEs.")
  public double multiply(@UdfParameter(value = "v1", description = "first value to multiply") double v1, @UdfParameter(value = "v2", description = "second value to multiply") double v2) {
    return v1 * v2;
  }
}