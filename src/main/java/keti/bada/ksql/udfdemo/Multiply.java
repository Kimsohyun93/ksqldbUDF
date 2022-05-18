package keti.bada.ksql.udfdemo;

import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import io.confluent.ksql.function.udf.UdfParameter;

@UdfDescription(name = "multiply", description = "multiplies 2 numbers")
public class Multiply {

  @Udf(description = "multiply two non-nullable INTs.")
  public long multiply(@UdfParameter int v1, @UdfParameter int v2) {
    return v1 * v2;
  }

  @Udf(description = "multiply two non-nullable BIGINTs.")
  public long multiply(@UdfParameter long v1, @UdfParameter long v2) {
    return v1 * v2;
  }

  @Udf(description = "multiply two nullable BIGINTs. If either param is null, null is returned.")
  public Long multiply(@UdfParameter Long v1, @UdfParameter Long v2) {
    return v1 == null || v2 == null ? null : v1 * v2;
  }

  @Udf(description = "multiply two non-nullable DOUBLEs.")
  public double multiply(@UdfParameter double v1, @UdfParameter double v2) {
    return v1 * v2;
  }
}