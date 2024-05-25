import React, { useState, useMemo, useCallback, useRef } from "react";
import {
  GoogleMap,
  useLoadScript,
  Marker,
  InfoWindow,
  Circle,
} from "@react-google-maps/api";
import "../styles/globals.css";
import Places from "./places";

const mapContainerStyle = {
  width: "80vw",
  height: "180vh",
};

const libraries = ["places"];
const MapPrediction = () => {
  const [officePosition, setOfficePosition] = useState(null);
  const [locations, setLocations] = useState([]);
  const center = useMemo(() => ({ lat: 40.679375, lng: -74.003151 }), []);
  const options = useMemo(
    () => ({
      mapId: "55474bd884220168",
      disableDefaultUI: true,
      clickableIcons: false,
    }),
    []
  );

  const { isLoaded, loadError } = useLoadScript({
    googleMapsApiKey: "AIzaSyAOzdWMGEEIt-yYbLgI5xUZqDQz4LLqpYE", // Use environment variable for API key
    libraries,
  });

  const mapRef = useRef(null);

  const onMapLoad = useCallback((map) => {
    mapRef.current = map;
  }, []);

  // Define marker icons
  const defaultIcon = null; // Google Maps default marker icon
  const specialIcon =
    "https://developers.google.com/maps/documentation/javascript/examples/full/images/beachflag.png";
  const handleMouseEnter = (index) => {
    setLocations(
      locations.map((loc, i) => ({
        ...loc,
        isActive: i === index,
      }))
    );
  };

  const handleMouseLeave = () => {
    setLocations(
      locations.map((loc) => ({
        ...loc,
        isActive: false,
      }))
    );
  };

  if (loadError) return <div></div>;
  if (!isLoaded) return <div></div>;
  return (
    <div className="container">
      <div className="controls">
        <h1>Destination?</h1>
        <Places
          setOffice={(position) => {
            setOfficePosition(position);
            mapRef.current?.panTo(position);
          }}
          setLocations={setLocations}
        />
        <ul>
          {locations.map((location, index) => (
            <li
              className="list-item"
              key={index}
              onMouseEnter={() => handleMouseEnter(index)}
              onMouseLeave={handleMouseLeave}
            >
              Point {index + 1}: <br />
              <span className="distance">
                {location.distance.toFixed(2)} km away
                <br />
                Probablity: {location.prob}
              </span>
            </li>
          ))}
        </ul>
      </div>
      <div className="map">
        <GoogleMap
          mapContainerStyle={mapContainerStyle}
          zoom={15}
          center={center}
          mapTypeId={"satellite"}
          onLoad={onMapLoad}
        >
          {officePosition && (
            <>
              <Marker
                position={officePosition}
                icon="https://developers.google.com/maps/documentation/javascript/examples/full/images/beachflag.png"
              />
              <Circle
                center={officePosition}
                radius={500}
                options={closeOptions}
              />
              <Circle
                center={officePosition}
                radius={1000}
                options={middleOptions}
              />
              <Circle
                center={officePosition}
                radius={1500}
                options={farOptions}
              />
            </>
          )}
          {locations.map((loc, index) => (
            <Marker
              key={index}
              position={{ lat: loc.lat, lng: loc.lng }}
              icon={loc.isActive ? specialIcon : defaultIcon}
            />
          ))}
        </GoogleMap>
      </div>
    </div>
  );
};
const defaultOptions = {
  strokeOpacity: 0.5,
  strokeWeight: 2,
  clickable: false,
  draggable: false,
  editable: false,
  visible: true,
};
const closeOptions = {
  ...defaultOptions,
  zIndex: 3,
  fillOpacity: 0.05,
  strokeColor: "#4A7B2F", // Darker shade of green
  fillColor: "#4A7B2F",
};
const middleOptions = {
  ...defaultOptions,
  zIndex: 2,
  fillOpacity: 0.05,
  strokeColor: "#C79100", // Darker shade of yellow/orange
  fillColor: "#C79100",
};
const farOptions = {
  ...defaultOptions,
  zIndex: 1,
  fillOpacity: 0.05,
  strokeColor: "#BF360C", // Darker shade of red
  fillColor: "#BF360C",
};
export default MapPrediction;
