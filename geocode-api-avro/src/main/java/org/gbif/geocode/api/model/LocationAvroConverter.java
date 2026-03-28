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

/**
 * Provides conversion between {@link Location} and {@link LocationAvro}.
 */
public class LocationAvroConverter {

  private LocationAvroConverter() {
    // utility class
  }

  /**
   * Encodes a {@link Location} to a {@link LocationAvro}.
   *
   * @param location the location to encode
   * @return the encoded LocationAvro
   */
  public static LocationAvro encode(Location location) {
    if (location == null) {
      return null;
    }
    return LocationAvro.newBuilder()
      .setId(location.getId())
      .setType(location.getType())
      .setSource(location.getSource())
      .setTitle(location.getTitle())
      .setIsoCountryCode2Digit(location.getIsoCountryCode2Digit())
      .setDistance(location.getDistance())
      .setDistanceMeters(location.getDistanceMeters())
      .build();
  }

  /**
   * Decodes a {@link LocationAvro} to a {@link Location}.
   *
   * @param locationAvro the LocationAvro to decode
   * @return the decoded Location
   */
  public static Location decode(LocationAvro locationAvro) {
    if (locationAvro == null) {
      return null;
    }
    Location location = new Location();
    location.setId(locationAvro.getId());
    location.setType(locationAvro.getType());
    location.setSource(locationAvro.getSource());
    location.setTitle(locationAvro.getTitle());
    location.setIsoCountryCode2Digit(locationAvro.getIsoCountryCode2Digit());
    location.setDistance(locationAvro.getDistance());
    location.setDistanceMeters(locationAvro.getDistanceMeters());
    return location;
  }
}
