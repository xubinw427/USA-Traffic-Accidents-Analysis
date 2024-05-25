import React from "react";
// import LineChart from './components/LineChart';
// import PieChart from './components/PieChart';
import { BrowserRouter, Route, Routes } from "react-router-dom";

import Navbar from "./components/Navbar";

import DataIntro from "./components/DataIntro";
import DataAnalysis from "./components/DataAnalysis";
import MapPrediction from "./components/MapPrediction";
import HomePage from "./components/HomePage";

function App() {
  return (
    <div>
      <BrowserRouter>
        <Navbar />

        <Routes>
          <Route path="/" element={<HomePage />} />
          <Route path="/DataIntro" element={<DataIntro />} />
          <Route path="/DataAnalysis" element={<DataAnalysis />} />
          <Route path="/MapPrediction" element={<MapPrediction />} />
        </Routes>
      </BrowserRouter>
    </div>
  );
}

export default App;
