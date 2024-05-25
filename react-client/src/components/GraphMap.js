import React, { useState, useEffect } from "react";
import Leaflet from "./Leaflet/index";

const GraphMap = () => {
  const [markers, setMarkers] = useState([]);

  useEffect(() => {
    async function fetchData() {
      try {
        const response = await fetch("http://localhost:5000/graphMap/state", {
          // const response = await fetch('http://52.9.248.230/graphMap/state', {
          method: "GET",
          headers: {
            "Content-Type": "application/json",
          },
        });

        if (!response.ok) {
          throw new Error(`HTTP error! status: ${response.status}`);
        }

        const responseData = await response.json();
        setMarkers(responseData.data);
        console.log(response);
      } catch (error) {
        console.error("Error during data fetch:", error);
      }
    }

    fetchData();
  }, []);
  return (
    <div>
      {/* <Typography variant="subtitle1" align="center" style={{ margin: '20px' }}>
        <b>Each state's number represents the Average Delay Time in minutes. Darker colors indicate higher values, while lighter colors indicate lower values.</b>
      </Typography> */}
      <Leaflet zoom={5} markers={markers} lat={40} lng={-100} />
    </div>
  );
};

export default GraphMap;
