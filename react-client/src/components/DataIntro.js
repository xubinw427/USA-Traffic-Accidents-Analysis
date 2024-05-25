import React from 'react'
import './css/DataIntro.css'; 

function DataIntro() {

  const DataTable1 = () => {
    const attributes = [
      { attribute: "Severity", description: "Shows the severity of the accident, a number between 1 and 4, where 1 indicates the least impact on traffic" },
      { attribute: "Start_Time", description: "Shows start time of the accident in local time zone." },
      { attribute: "End_Time", description: "Shows end time of the accident in local time zone. End time here refers to when the impact of accident on traffic flow was dismissed." },
      { attribute: "Start_Lat", description: "Shows latitude in GPS coordinate of the start point." },
      { attribute: "Start_Lng", description: "Shows longitude in GPS coordinate of the start point." },
      { attribute: "City", description: "Shows the city in address field." },
      { attribute: "State", description: "Shows the state in address field." },
      { attribute: "Zipcode", description: "Shows the zipcode in address field." },
      { attribute: "Temperature(F)", description: "Shows the temperature (in Fahrenheit)." },
      { attribute: "Wind_Chill(F)", description: "Shows the wind chill (in Fahrenheit)." },
      { attribute: "Humidity(%)", description: "Shows the humidity (in percentage)." },
      { attribute: "Visibility(mi)", description: "Shows visibility (in miles)." },
    ];
  
    return (
      <table>
        <thead>
          <tr>
            <th>Attribute</th>
            <th>Description</th>
          </tr>
        </thead>
        <tbody>
          {attributes.map((item, index) => (
            <tr key={index}>
              <td>{item.attribute}</td>
              <td>{item.description}</td>
            </tr>
          ))}
        </tbody>
      </table>
    );
  };


  
  return (
    <div className='TrainingModelsContainer'>
      <h1>
        US Accidents (2016 - 2023)
      </h1>
      <h1>Introduction</h1>
      <div style={{paddingTop:'0px', paddingBottom:'0px'}}>Our dataset &nbsp;            
        <a href="https://www.kaggle.com/datasets/sobhanmoosavi/us-accidents" target="_blank" rel="noopener noreferrer">
          (Link)
        </a>&nbsp;
          encompasses car accident records across 49 states in the USA, gathered from February 2016 to March 2023. 
        The collection of data was facilitated through several APIs that stream traffic-related incidents. These APIs source 
        their information from a variety of contributors, such as departments of transportation at both the state and national levels, 
        law enforcement bodies, traffic surveillance cameras, and sensors embedded within the road infrastructure. Currently, the dataset 
        holds about 7.7 million records of traffic accidents.

      </div>
      <h2>Below are the content of our dataset and the tables of some attributes</h2>
      <DataTable1 />
    </div>
  )
}

export default DataIntro