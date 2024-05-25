import React from "react";
import { Link } from "react-router-dom";
import './css/HomePage.css';

function HomePage() {
  return (
    <div className="home-page">
      <div className="homePagecontainer">
        <Link to="/DataIntro" className="block">
          Explore Raw Data Sources
        </Link>
        <Link to="/DataAnalysis" className="block">
          View Data Analysis Outcomes
        </Link>
        <Link to="/MapPrediction" className="block">
          Start Prediction for Accidents
        </Link>
      </div>
    </div>
  );
}

export default HomePage;
