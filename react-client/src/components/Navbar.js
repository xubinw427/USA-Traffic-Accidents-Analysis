import React from "react";
import { Link } from "react-router-dom";
import AppBar from "@mui/material/AppBar";
import Box from "@mui/material/Box";
import Toolbar from "@mui/material/Toolbar";
import IconButton from "@mui/material/IconButton";
import Button from "@mui/material/Button";

import './css/Navbar.css'; 

function Navbar() {
  return (
    <>
    <nav>
      <ul>
        <li>
          <a href="/">Home Page</a>
        </li>
        <li>
          <a href="/DataIntro">Data Source</a>
        </li>
        <li>
          <a href="/DataAnalysis">Data Analysis</a>
        </li>
        <li>
          <a href="/MapPrediction">Map Prediction</a>
        </li>
      </ul>
    </nav>
    </>
  );
}

export default Navbar;
