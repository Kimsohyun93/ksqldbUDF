package keti.bada.ksql.udfdemo;

import com.vividsolutions.jts.geom.Coordinate;
import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import io.confluent.ksql.function.udf.UdfParameter;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.util.GeometricShapeFactory;


@UdfDescription(name = "geo_circle", description = "The two inputs are (lat, lng) pairs and diameter. Enter the diameter in meters. Return geometric polygon to the string. Polygon has a default of 32 points, which can be changed to optional parameters.")
public class GeoCircle {

  @Udf(description = "Create GeoJson Polygon with two inputs (lat,lng) pairs and diameter. All Input types are Double.")
  public String geo_circle(@UdfParameter(value = "lat", description = "the latitude of the center of the circle") double lat,
                           @UdfParameter(value = "lng", description = "the longitude of the center of the circle") double lng,
                           @UdfParameter(value = "diameter", description = "diameter of the circle (M)") double diameter) {
    GeometricShapeFactory shapeFactory = new GeometricShapeFactory();
    shapeFactory.setNumPoints(32);
    shapeFactory.setCentre(new Coordinate(lat, lng));

    // Length in meters of 1° of latitude = always 111.32 km
    shapeFactory.setWidth(diameter/111320d);
    // Length in meters of 1° of longitude = 40075 km * cos( latitude ) / 360
    shapeFactory.setHeight(diameter / (40075000 * Math.cos(Math.toRadians(lat)) / 360));

    return shapeFactory.createCircle().toString();
  }

  @Udf(description = "Create GeoJson Polygon with two inputs (lat,lng) pairs and diameter as Double, num_points as int")
  public String geo_circle(@UdfParameter(value = "lat", description = "the latitude of the center of the circle") double lat,
                           @UdfParameter(value = "lng", description = "the longitude of the center of the circle") double lng,
                           @UdfParameter(value = "diameter", description = "the diameter of the circle (M)") double diameter,
                           @UdfParameter(value = "num_points", description = "the number of points ") int num_points) {
    GeometricShapeFactory shapeFactory = new GeometricShapeFactory();
    shapeFactory.setNumPoints(num_points);
    shapeFactory.setCentre(new Coordinate(lat, lng));

    // Length in meters of 1° of latitude = always 111.32 km
    shapeFactory.setWidth(diameter/111320d);
    // Length in meters of 1° of longitude = 40075 km * cos( latitude ) / 360
    shapeFactory.setHeight(diameter / (40075000 * Math.cos(Math.toRadians(lat)) / 360));

    return shapeFactory.createCircle().toString();
  }
}
