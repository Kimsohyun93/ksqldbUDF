package keti.bada.ksql.udfdemo;

import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import io.confluent.ksql.function.udf.UdfParameter;

import java.util.Map;

@UdfDescription(name = "formula",
        author = "example user",
        version = "1.0.2",
        description = "A custom formula for important business logic.")
public class Formula {

  @Udf(description = "The standard version of the formula with integer parameters.")
  public long formula(@UdfParameter int v1, @UdfParameter int v2) {
    return (v1 * v2);
  }

  @Udf(description = "A special variant of the formula, handling double parameters.")
  public long formula(@UdfParameter double v1, @UdfParameter double v2) {
    return ((int) (Math.ceil(v1) * Math.ceil(v2)));
  }

}