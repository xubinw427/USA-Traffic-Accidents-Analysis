import React, { useState, useRef } from "react";
import { MapContainer, TileLayer, Marker, Popup } from "react-leaflet";
import L from "leaflet";
import markerIconUrl from '../../static/marker.png';
import "leaflet/dist/leaflet.css";
import osm from "./osm-providers";

const markerIcon = new L.Icon({
  iconUrl: markerIconUrl,
  iconSize: [40, 40],
  iconAnchor: [20, 40], 
  popupAnchor: [0, -40], 
  shadowUrl: null, 
  shadowSize: null,
  shadowAnchor: null, 
});

const MarkersMap = ({ zoom, markers, lat, lng }) => {
  const [center, setCenter] = useState({ lat: lat, lng: lng });
  const ZOOM_LEVEL = zoom;
  const mapRef = useRef();

  return (
    <>
      <div className="row">
        <div className="col text-center">
          <div className="col">
            <MapContainer center={center} zoom={ZOOM_LEVEL} ref={mapRef}>
              <TileLayer
                url={osm.maptiler.url}
                attribution={osm.maptiler.attribution}
              />

              {markers.map((marker) => (
                <Marker
                  key={marker.id}
                  position={[marker.lat, marker.lng]}
                  icon={markerIcon}
                  eventHandlers={{
                    mouseover: (e) => {
                      e.target.openPopup();
                    },
                    mouseout: (e) => {
                      e.target.closePopup();
                    }
                  }}
                >
                  <Popup>
                    <b>
                      State: {marker.state}, Traffic num from 2016-2022:  {marker.accuracy}
                    </b>
                  </Popup>
                </Marker>
              ))}
            </MapContainer>
          </div>
        </div>
      </div>
    </>
  );
};

export default MarkersMap;
