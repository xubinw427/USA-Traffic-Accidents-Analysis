import React, { Suspense } from "react";
// import { Switch, Route, useRouteMatch } from "react-router-dom";
import "./index.css";

const MarkersMap = React.lazy(() => import("./markers"));

const Leaflet = ({zoom, markers, lat, lng}) => {
  // let { path } = useRouteMatch();
  return (
    <Suspense fallback={<div></div>}>
      <MarkersMap zoom={zoom} markers={markers} lat={lat} lng={lng} />
    </Suspense>
  );
};

export default Leaflet;