/*
 * Copyright 2020 Global Biodiversity Information Facility (GBIF)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gbif.geocode.api.model;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Tests for {@link LocationAvroConverter}.
 */
public class LocationAvroConverterTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  /**
   * Loads locations from the test JSON resource file.
   */
  private List<Location> loadLocations() throws Exception {
    InputStream in = getClass().getResourceAsStream("/locations.json");
    assertNotNull(in, "locations.json resource not found");
    JsonNode root = MAPPER.readTree(in);
    JsonNode locationsNode = root.get("locations");
    List<Location> locations = new ArrayList<>();
    for (JsonNode node : locationsNode) {
      Location location = new Location();
      location.setId(node.get("id").asText());
      location.setType(node.get("type").asText());
      location.setSource(node.get("source").asText());
      location.setTitle(node.get("title").asText());
      location.setIsoCountryCode2Digit(node.get("isoCountryCode2Digit").asText());
      location.setDistance(node.get("distance").asDouble());
      locations.add(location);
    }
    return locations;
  }

  @Test
  public void testEncodeAndDecode() throws Exception {
    List<Location> locations = loadLocations();
    assertEquals(10, locations.size());

    for (Location original : locations) {
      LocationAvro encoded = LocationAvroConverter.encode(original);
      assertNotNull(encoded);
      assertEquals(original.getId(), encoded.getId());
      assertEquals(original.getType(), encoded.getType());
      assertEquals(original.getSource(), encoded.getSource());
      assertEquals(original.getTitle(), encoded.getTitle());
      assertEquals(original.getIsoCountryCode2Digit(), encoded.getIsoCountryCode2Digit());
      assertEquals(original.getDistance(), encoded.getDistance());
      assertEquals(original.getDistanceMeters(), encoded.getDistanceMeters());

      Location decoded = LocationAvroConverter.decode(encoded);
      assertNotNull(decoded);
      assertEquals(original.getId(), decoded.getId());
      assertEquals(original.getType(), decoded.getType());
      assertEquals(original.getSource(), decoded.getSource());
      assertEquals(original.getTitle(), decoded.getTitle());
      assertEquals(original.getIsoCountryCode2Digit(), decoded.getIsoCountryCode2Digit());
      assertEquals(original.getDistance(), decoded.getDistance());
      assertEquals(original.getDistanceMeters(), decoded.getDistanceMeters());
    }
  }

  @Test
  public void testEncodeNullReturnsNull() {
    assertNull(LocationAvroConverter.encode(null));
  }

  @Test
  public void testDecodeNullReturnsNull() {
    assertNull(LocationAvroConverter.decode(null));
  }

  @Test
  public void testFirstLocation() throws Exception {
    List<Location> locations = loadLocations();
    Location first = locations.get(0);

    assertEquals("ECU", first.getId());
    assertEquals("GADM0", first.getType());
    assertEquals("http://gadm.org/", first.getSource());
    assertEquals("EC", first.getIsoCountryCode2Digit());
    assertEquals(0.0, first.getDistance());
    assertEquals("Ecuador", first.getTitle());

    LocationAvro encoded = LocationAvroConverter.encode(first);
    assertEquals("ECU", encoded.getId());
    assertEquals("GADM0", encoded.getType());

    Location decoded = LocationAvroConverter.decode(encoded);
    assertEquals(first.getId(), decoded.getId());
    assertEquals(first.getType(), decoded.getType());
    assertEquals(first.getSource(), decoded.getSource());
    assertEquals(first.getIsoCountryCode2Digit(), decoded.getIsoCountryCode2Digit());
    assertEquals(first.getDistance(), decoded.getDistance());
    assertEquals(first.getTitle(), decoded.getTitle());
  }
}
