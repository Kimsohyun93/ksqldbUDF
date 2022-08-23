package keti.bada.ksql.udfdemo;

import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import io.confluent.ksql.function.udf.UdfParameter;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.util.GeometricShapeFactory;


@UdfDescription(name = "geo_circle", description = "The two inputs are (lat, lng) pairs and radius. Enter the radius in meters. Return GeoJson Polygon. Polygon has a default of 32 points, which can be changed to optional parameters.")
public class geoCircle {

  @Udf(description = "Create GeoJson Polygon with two inputs (lat,lng) pairs and radius. All Input types are Double.")
  public Polygon geocircle(@UdfParameter(value = "lat", description = "the latitude of the center of the circle") double lat,
                           @UdfParameter(value = "lng", description = "the longitude of the center of the circle") double lng,
                           @UdfParameter(value = "radius", description = "radius of the circle") double radius) {
    GeometricShapeFactory shapeFactory = new GeometricShapeFactory();
    shapeFactory.setNumPoints(32);
    shapeFactory.setCentre(new Coordinate(lat, lng));

    // Length in meters of 1° of latitude = always 111.32 km
    shapeFactory.setWidth(radius/111320d);
    // Length in meters of 1° of longitude = 40075 km * cos( latitude ) / 360
    shapeFactory.setHeight(radius / (40075000 * Math.cos(Math.toRadians(lat)) / 360));

    return shapeFactory.createCircle();
  }

  @Udf(description = "Create GeoJson Polygon with two inputs (lat,lng) pairs and radius. All Input types are Double.")
  public Polygon geocircle(@UdfParameter(value = "lat", description = "the latitude of the center of the circle") double lat,
                           @UdfParameter(value = "lng", description = "the longitude of the center of the circle") double lng,
                           @UdfParameter(value = "radius", description = "the radius of the circle") double radius,
                           @UdfParameter(value = "num_points", description = "the number of points ") int num_points) {
    GeometricShapeFactory shapeFactory = new GeometricShapeFactory();
    shapeFactory.setNumPoints(num_points);
    shapeFactory.setCentre(new Coordinate(lat, lng));

    // Length in meters of 1° of latitude = always 111.32 km
    shapeFactory.setWidth(radius/111320d);
    // Length in meters of 1° of longitude = 40075 km * cos( latitude ) / 360
    shapeFactory.setHeight(radius / (40075000 * Math.cos(Math.toRadians(lat)) / 360));

    return shapeFactory.createCircle();
  }

}