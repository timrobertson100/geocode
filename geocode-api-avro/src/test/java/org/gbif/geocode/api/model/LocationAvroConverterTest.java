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

import java.io.File;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
      location.setDistanceMeters(node.get("distanceMeters").asDouble());
      locations.add(location);
    }
    return locations;
  }

  /**
   * Round-trip encode→decode must reproduce the original {@link Location} exactly for all test
   * locations, including the marine-regions entry whose id carries a known prefix.
   */
  @Test
  public void testEncodeAndDecode() throws Exception {
    List<Location> locations = loadLocations();
    assertEquals(10, locations.size());

    for (Location original : locations) {
      LocationAvro encoded = LocationAvroConverter.encode(original);
      assertNotNull(encoded);

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

  /**
   * Verifies that the GADM source URL is encoded to {@link LocationSource#GADM} and that a
   * plain GADM id is stored unchanged (no prefix to strip).
   */
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
    assertEquals(LocationType.GADM0, encoded.getType());
    assertEquals(LocationSource.GADM, encoded.getSource());
    assertNull(encoded.getIdPrefix());

    Location decoded = LocationAvroConverter.decode(encoded);
    assertEquals(first.getId(), decoded.getId());
    assertEquals(first.getType(), decoded.getType());
    assertEquals(first.getSource(), decoded.getSource());
    assertEquals(first.getIsoCountryCode2Digit(), decoded.getIsoCountryCode2Digit());
    assertEquals(first.getDistance(), decoded.getDistance());
    assertEquals(first.getTitle(), decoded.getTitle());
  }

  /**
   * Verifies that the Marine Regions source is encoded to {@link LocationSource#MARINE_REGIONS},
   * that the MRGID prefix is stripped from the id and stored in {@link LocationIdPrefix#MRGID},
   * and that decoding restores the full original id.
   */
  @Test
  public void testMarineRegionsLocation() throws Exception {
    Location marine = loadLocations().stream()
        .filter(l -> l.getId().startsWith("http://marineregions.org/mrgid/"))
        .findFirst()
        .orElseThrow(() -> new AssertionError("No marine-regions location in test data"));

    LocationAvro encoded = LocationAvroConverter.encode(marine);
    assertEquals("8431", encoded.getId());
    assertEquals(LocationSource.MARINE_REGIONS, encoded.getSource());
    assertEquals(LocationIdPrefix.MRGID, encoded.getIdPrefix());

    Location decoded = LocationAvroConverter.decode(encoded);
    assertEquals("http://marineregions.org/mrgid/8431", decoded.getId());
    assertEquals("https://www.marineregions.org/", decoded.getSource());
  }

  @Test
  public void testEncodeNullReturnsNull() {
    assertNull(LocationAvroConverter.encode((Location)null));
  }

  @Test
  public void testDecodeNullReturnsNull() {
    assertNull(LocationAvroConverter.decode(null));
  }

  /**
   * Verifies that null distance and distanceMeters in a Location are preserved through
   * encode and decode.
   */
  @Test
  public void testNullDistances() throws Exception {
    List<Location> locations = loadLocations();
    Location base = locations.get(0);

    Location loc = new Location();
    loc.setId(base.getId());
    loc.setType(base.getType());
    loc.setSource(base.getSource());
    loc.setTitle(base.getTitle());
    loc.setIsoCountryCode2Digit(base.getIsoCountryCode2Digit());
    loc.setDistance(null);
    loc.setDistanceMeters(null);

    LocationAvro encoded = LocationAvroConverter.encode(loc);
    assertNull(encoded.getDistance());
    assertNull(encoded.getDistanceMeters());

    Location decoded = LocationAvroConverter.decode(encoded);
    assertNull(decoded.getDistance());
    assertNull(decoded.getDistanceMeters());
  }

  /**
   * Utility: generates 10,000 locations (cycling through the test data with slightly modified
   * ids and titles), writes them as plain JSON and as Avro to temp files, then prints the
   * resulting file sizes so the space saving can be seen at a glance.
   */
  @Test
  public void reportFileSizes() throws Exception {
    List<Location> baseLocations = loadLocations();
    int total = 10_000;

    List<Location> locations = new ArrayList<>(total);
    for (int i = 0; i < total; i++) {
      Location base = baseLocations.get(i % baseLocations.size());
      Location loc = new Location();
      loc.setId(base.getId() + "_" + i);
      loc.setType(base.getType());
      loc.setSource(base.getSource());
      loc.setTitle(base.getTitle() + " " + i);
      loc.setIsoCountryCode2Digit(base.getIsoCountryCode2Digit());
      loc.setDistance(base.getDistance());
      loc.setDistanceMeters(base.getDistanceMeters());
      locations.add(loc);
    }

    // Write JSON
    File jsonFile = File.createTempFile("locations-", ".json");
    jsonFile.deleteOnExit();
    MAPPER.writeValue(jsonFile, locations);

    // Write Avro
    File avroFile = File.createTempFile("locations-", ".avro");
    avroFile.deleteOnExit();
    DatumWriter<LocationAvro> datumWriter = new SpecificDatumWriter<>(LocationAvro.class);
    try (DataFileWriter<LocationAvro> dataFileWriter = new DataFileWriter<>(datumWriter)) {
      dataFileWriter.create(LocationAvro.getClassSchema(), avroFile);
      for (Location loc : locations) {
        dataFileWriter.append(LocationAvroConverter.encode(loc));
      }
    }

    System.out.printf("%,d locations → JSON: %,d bytes | Avro: %,d bytes%n",
        total, jsonFile.length(), avroFile.length());
  }

  /**
   * Decodes an Avro byte[] container into a list of {@link LocationAvro} records.
   */
  private List<LocationAvro> decodeAvroBytes(byte[] avroBytes) throws Exception {
    List<LocationAvro> decoded = new ArrayList<>();
    try (DataFileReader<LocationAvro> reader = new DataFileReader<>(
        new SeekableByteArrayInput(avroBytes),
        new SpecificDatumReader<>(LocationAvro.class))) {
      reader.forEach(decoded::add);
    }
    return decoded;
  }

  /**
   * Asserts that each decoded {@link LocationAvro} round-trips back to the corresponding
   * original {@link Location}.
   */
  private void assertLocationsRoundTrip(List<Location> expected, List<LocationAvro> actual) {
    assertEquals(expected.size(), actual.size());
    for (int i = 0; i < expected.size(); i++) {
      Location original = expected.get(i);
      Location roundTripped = LocationAvroConverter.decode(actual.get(i));
      assertEquals(original.getId(), roundTripped.getId());
      assertEquals(original.getType(), roundTripped.getType());
      assertEquals(original.getSource(), roundTripped.getSource());
      assertEquals(original.getTitle(), roundTripped.getTitle());
    }
  }

  /**
   * Verifies that {@link LocationAvroConverter#encode(List)} produces a non-empty Avro byte[]
   * whose decoded records round-trip back to the original Locations.
   */
  @Test
  public void testEncodeListReturnsByteArray() throws Exception {
    List<Location> locations = loadLocations();

    byte[] avroBytes = LocationAvroConverter.encode(locations);
    assertNotNull(avroBytes);
    assertTrue(avroBytes.length > 0);

    assertLocationsRoundTrip(locations, decodeAvroBytes(avroBytes));
  }

  /**
   * Verifies that {@link LocationAvroConverter#fromJson(byte[])} converts a GeoCode response
   * ({"locations":[...]}) into Avro bytes whose decoded records match the originals.
   */
  @Test
  public void testFromJsonGeocodeResponseFormat() throws Exception {
    List<Location> locations = loadLocations();
    byte[] jsonBytes;
    try (InputStream in = getClass().getResourceAsStream("/locations.json")) {
      assertNotNull(in, "locations.json resource not found");
      jsonBytes = in.readAllBytes();
    }

    byte[] avroBytes = LocationAvroConverter.fromJson(jsonBytes);
    assertNotNull(avroBytes);
    assertTrue(avroBytes.length > 0);

    assertLocationsRoundTrip(locations, decodeAvroBytes(avroBytes));
  }

  /**
   * Verifies that {@link LocationAvroConverter#fromJson(byte[])} converts the
   * {@link ObjectMapper} JSON encoding of a {@code List<Location>} (plain array) into Avro bytes
   * whose decoded records match the originals.
   */
  @Test
  public void testFromJsonReturnsByteArray() throws Exception {
    List<Location> locations = loadLocations();
    byte[] jsonBytes = MAPPER.writeValueAsBytes(locations);

    byte[] avroBytes = LocationAvroConverter.fromJson(jsonBytes);
    assertNotNull(avroBytes);
    assertTrue(avroBytes.length > 0);

    assertLocationsRoundTrip(locations, decodeAvroBytes(avroBytes));
  }
}
