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

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Provides conversion between {@link Location} and {@link LocationAvro}.
 *
 * <p>Optimisations applied during encoding:
 * <ul>
 *   <li>The {@code type} string is stored as a compact {@link LocationType} enum.</li>
 *   <li>The {@code source} URL is stored as a compact {@link LocationSource} enum.</li>
 *   <li>Well-known ID prefixes are stripped from {@code id} and recorded in {@code id_prefix},
 *       saving repeated prefix bytes per record.</li>
 * </ul>
 * The type mapping uses {@link LocationType#valueOf(String)} directly, so the type string must
 * exactly match a symbol in the {@link LocationType} enum.  The source and prefix mappings are
 * easy to extend: add a new entry to {@link #SOURCE_MAP} or
 * {@link #ID_PREFIX_URL_MAP} and add the corresponding symbol to the Avro schema.
 */
public class LocationAvroConverter {

  private LocationAvroConverter() {
    // utility class
  }

  // ---------------------------------------------------------------------------
  // Source URL <-> enum mappings
  // To add a new source: add an entry here AND a symbol to LocationSource in Location.avsc
  // ---------------------------------------------------------------------------

  private static final Map<String, LocationSource> SOURCE_MAP = new LinkedHashMap<>();
  private static final Map<LocationSource, String> SOURCE_URL_MAP = new LinkedHashMap<>();

  static {
    addSource("http://gadm.org/",                      LocationSource.GADM);
    addSource("http://www.tdwg.org/standards/109",     LocationSource.WGSRPD);
    addSource("https://github.com/gbif/continents",    LocationSource.CONTINENT);
    addSource("https://www.marineregions.org/",        LocationSource.MARINE_REGIONS);
  }

  private static void addSource(String url, LocationSource source) {
    SOURCE_MAP.put(url, source);
    SOURCE_URL_MAP.put(source, url);
  }

  // ---------------------------------------------------------------------------
  // ID prefix enum -> prefix URL mappings
  // To add a new prefix: add an entry here AND a symbol to LocationIdPrefix in Location.avsc
  // ---------------------------------------------------------------------------

  private static final Map<LocationIdPrefix, String> ID_PREFIX_URL_MAP = new LinkedHashMap<>();

  static {
    ID_PREFIX_URL_MAP.put(LocationIdPrefix.MRGID, "http://marineregions.org/mrgid/");
  }

  /**
   * Encodes a {@link Location} to a {@link LocationAvro}.
   *
   * <p>The {@code source} string is converted to a {@link LocationSource} enum and any known
   * prefix is stripped from {@code id} and stored in {@code id_prefix}.
   *
   * @param location the location to encode
   * @return the encoded LocationAvro
   * @throws IllegalArgumentException if the source URL or type string is not a known value
   */
  public static LocationAvro encode(Location location) {
    if (location == null) {
      return null;
    }

    LocationSource sourceEnum = SOURCE_MAP.get(location.getSource());
    if (sourceEnum == null) {
      throw new IllegalArgumentException("Unknown source URL: " + location.getSource());
    }

    LocationType typeEnum = LocationType.valueOf(location.getType());

    String id = location.getId();
    LocationIdPrefix idPrefix = null;
    // Iteration order is insertion order (LinkedHashMap). If prefixes can overlap,
    // list longer/more-specific prefixes first so the most precise match wins.
    for (Map.Entry<LocationIdPrefix, String> entry : ID_PREFIX_URL_MAP.entrySet()) {
      if (id != null && id.startsWith(entry.getValue())) {
        idPrefix = entry.getKey();
        id = id.substring(entry.getValue().length());
        break;
      }
    }

    return LocationAvro.newBuilder()
      .setId(id)
      .setIdPrefix(idPrefix)
      .setType(typeEnum)
      .setSource(sourceEnum)
      .setTitle(location.getTitle())
      .setIsoCountryCode2Digit(location.getIsoCountryCode2Digit())
      .setDistance(location.getDistance())
      .setDistanceMeters(location.getDistanceMeters())
      .build();
  }

  /**
   * Decodes a {@link LocationAvro} to a {@link Location}.
   *
   * <p>The {@link LocationSource} enum is converted back to its source URL string and any
   * stripped {@code id_prefix} is prepended to {@code id}.
   *
   * @param locationAvro the LocationAvro to decode
   * @return the decoded Location
   */
  public static Location decode(LocationAvro locationAvro) {
    if (locationAvro == null) {
      return null;
    }

    String id = locationAvro.getId();
    if (locationAvro.getIdPrefix() != null) {
      id = ID_PREFIX_URL_MAP.get(locationAvro.getIdPrefix()) + id;
    }

    Location location = new Location();
    location.setId(id);
    location.setType(locationAvro.getType().toString());
    location.setSource(SOURCE_URL_MAP.get(locationAvro.getSource()));
    location.setTitle(locationAvro.getTitle());
    location.setIsoCountryCode2Digit(locationAvro.getIsoCountryCode2Digit());
    location.setDistance(locationAvro.getDistance());
    location.setDistanceMeters(locationAvro.getDistanceMeters());
    return location;
  }
}
