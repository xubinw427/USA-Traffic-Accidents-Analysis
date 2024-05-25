import React, { useState } from "react";
import usePlacesAutocomplete, {
  getGeocode,
  getLatLng,
} from "use-places-autocomplete";
import {
  Combobox,
  ComboboxInput,
  ComboboxPopover,
  ComboboxList,
  ComboboxOption,
} from "@reach/combobox";
import "@reach/combobox/styles.css";

function getDistanceFromLatLonInKm(lat1, lon1, lat2, lon2) {
  var R = 6371; // Radius of the earth in kilometers
  var dLat = deg2rad(lat2 - lat1);
  var dLon = deg2rad(lon2 - lon1);
  var a =
    Math.sin(dLat / 2) * Math.sin(dLat / 2) +
    Math.cos(deg2rad(lat1)) *
      Math.cos(deg2rad(lat2)) *
      Math.sin(dLon / 2) *
      Math.sin(dLon / 2);
  var c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
  var distance = R * c; // Distance in kilometers
  return distance;
}

function deg2rad(deg) {
  return deg * (Math.PI / 180);
}
export default function Places({ setOffice, setLocations }) {
  const [humidity, setHumidity] = useState("");
  const [temperature, setTemperature] = useState("");
  const [condition, setCondition] = useState("");
  const [selectedLat, setSelectedLat] = useState(null);
  const [selectedLng, setSelectedLng] = useState(null);
  const [date, setDate] = useState("");
  const [selectionMade, setSelectionMade] = useState(false);
  const {
    ready,
    value,
    setValue,
    suggestions: { status, data },
    clearSuggestions,
  } = usePlacesAutocomplete({ requestOptions: {}, debounce: 300 });

  const handleInput = (e) => {
    setValue(e.target.value);
    setSelectionMade(false);
  };
  const handleClearInput = () => {
    setValue("");
    clearSuggestions();
  };
  const handlePredictionRequest = async () => {
    // Check if latitude or longitude is not selected
    if (selectedLat == null || selectedLng == null) {
      console.error("No location selected");
      return;
    }

    // Construct the payload with user inputs
    const payload = {
      lat: selectedLat,
      lng: selectedLng,
      humidity,
      temperature,
      condition,
      date,
    };

    try {
      // Make the POST request with the payload
      const response = await fetch("http://localhost:5000/api/location", {
      // const response = await fetch("http://52.9.248.230/api/location", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify(payload),
      });

      if (!response.ok) {
        throw new Error(`HTTP error! status: ${response.status}`);
      }
      const locations = await response.json();
      console.log("find locations: ", locations)
      if (locations.length === 0) {
        window.alert("No accident records found near the given location. Please enter another location.");
        return;
      }
  
      // console.log(typeof locations); 
      // console.log(locations);
      const parsed_location = await JSON.parse(locations);
      // console.log(typeof parsed_location);
      // console.log(parsed_location);
      const locationsObject = parsed_location.reduce((acc, loc, index) => {
        acc[index] = loc;
        return acc;
      }, {});
      // console.log(typeof locationsObject);
      // console.log(locationsObject);
      // console.log(locationsObject);
      const locationsWithDistance = Object.entries(locationsObject).map(
        (location) => {
          // Calculate the distance from the selected location
          // console.log(location);
          const distance = getDistanceFromLatLonInKm(
            selectedLat,
            selectedLng,
            location[1].lat,
            location[1].lng
          );

          // Return a new object for each location including the calculated distance
          return { ...location[1], distance, isActive: false };
        }
      );

      // Update your state with the new locations array
      setLocations(locationsWithDistance);
    } catch (error) {
      console.error("Failed to fetch predictions:", error);
    }
  };

  const handleSelect = async (address) => {
    setValue(address, false);
    clearSuggestions();
    setSelectionMade(true);
    try {
      const results = await getGeocode({ address });
      const { lat, lng } = await getLatLng(results[0]);
      setOffice({ lat, lng });
      setSelectedLat(lat);
      setSelectedLng(lng);
    } catch (error) {
      console.error("Error: ", error);
      setSelectionMade(false);
    }
  };

  return (
    <Combobox onSelect={handleSelect}>
      <div style={{ position: "relative" }}>
        <ComboboxInput
          value={value}
          onChange={handleInput}
          disabled={!ready}
          className="combobox-input"
          placeholder="Enter a place name or postal code"
          style={{ width: "100%", paddingRight: "30px" }} // Ensure input takes full width of the container
        />
        {value && (
          <button
            onClick={handleClearInput}
            style={{
              position: "absolute",
              right: "10px",
              top: "50%",
              transform: "translateY(-50%)",
              cursor: "pointer",
              background: "transparent",
              border: "none",
              color: "#999",
              fontWeight: "bold",
            }}
            aria-label="Clear text"
          >
            X
          </button>
        )}
      </div>
      <div>
        <label htmlFor="humidity">Humidity (%): </label>
        <input
          type="number"
          id="humidity"
          value={humidity}
          onChange={(e) => setHumidity(e.target.value)}
          placeholder="Enter Humidity"
        />
      </div>
      <div>
        <label htmlFor="temperature">Temperature (°C): </label>
        <input
          type="number"
          id="temperature"
          value={temperature}
          onChange={(e) => setTemperature(e.target.value)}
          placeholder="Enter Temperature"
        />
      </div>

      <div>
        <label htmlFor="condition">Weather Condition: </label>
        <select
          id="condition"
          value={condition}
          onChange={(e) => setCondition(e.target.value)}
        >
          <option value="">Select a condition</option>
          <option value="Cloudy">Cloudy</option>
          <option value="Sunny">Sunny</option>
          <option value="Drizzle">Drizzle</option>
          <option value="Fair">Fair</option>
          <option value="Rain">Rain</option>
          <option value="Snow">Snow</option>
          <option value="T-Storm">T-Storm</option>
          <option value="Tornado">Tornado</option>
          <option value="Thunder ">Thunder </option>
          <option value="Fog">Fog</option>
          <option value="Shower">Shower</option>
          <option value="Dust">Dust</option>
          <option value="Haze">Haze</option>
          <option value="Windy">Windy</option>
        </select>
      </div>

      <div>
        <label htmlFor="date">Date: </label>
        <input
          type="datetime-local"
          value={date}
          onChange={(e) => setDate(e.target.value)}
          min="2020-01-01T00:00"
          max="2030-12-31T23:59"
        />
      </div>
      <button onClick={() => handlePredictionRequest()}>Predict</button>
      <ComboboxPopover>
        {status === "OK" ? (
          <ComboboxList>
            {data.map(({ place_id, description }) => (
              <ComboboxOption key={place_id} value={description} />
            ))}
          </ComboboxList>
        ) : (
          !selectionMade &&
          value && (
            <div style={{ padding: "0.5rem" }}>
              No locations found. Try another postal code.
            </div>
          )
        )}
      </ComboboxPopover>
    </Combobox>
  );
}
