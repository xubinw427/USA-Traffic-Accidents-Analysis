import React, { useState, useEffect } from 'react';
import './css/DataAnalysis.css'; 
import LineChart from './LineChart';
import BarChart from './BarChart';
import GraphMap from './GraphMap';

import Box from '@mui/material/Box';
import Button from '@mui/material/Button';
import List from '@mui/material/List';
import ListItem from '@mui/material/ListItem';
import ListItemIcon from '@mui/material/ListItemIcon';
import ListItemText from '@mui/material/ListItemText';
import TimelineIcon from '@mui/icons-material/Timeline';
import LandscapeIcon from '@mui/icons-material/Landscape';
import WeatherIcon from '@mui/icons-material/WbSunny'; 


const Sidebar = ({ onItemSelected }) => {
  return (
    <Box
      sx={{
        width: 280,
        // bgcolor: '#222D32', 
        color: 'white', 
        '& .MuiListItemIcon-root': {
          color: 'white', 
        },
        '& .MuiListItemText-primary': { 
          color: 'white', 
        }
      }}
    >
      <List>
        <ListItem button onClick={() => onItemSelected('time')}>
          <ListItemIcon>
            <TimelineIcon />
          </ListItemIcon>
          <ListItemText primary="Analysis By Time" />
        </ListItem>
        <ListItem button onClick={() => onItemSelected('geography')}>
          <ListItemIcon>
            <LandscapeIcon />
          </ListItemIcon>
          <ListItemText primary="Analysis By Geography" />
        </ListItem>
        <ListItem button onClick={() => onItemSelected('weather')}>
          <ListItemIcon>
            <WeatherIcon />
          </ListItemIcon>
          <ListItemText primary="Analysis By Weather" />
        </ListItem>
      </List>
    </Box>
  );
};

const SecondaryNavbar = ({ filters, onFilterSelected, selectedFilter }) => {
  if (!filters) {
    return null;
  }


  function switchFilter(filter) {
    switch (filter) {
      case 'Year':
        return 'Year';
      case 'Month':
        return 'Month';
      case 'Week':
        return 'Week';
      case 'Hour':
        return 'Hour';
      case 'US State':
        return 'US State';
      case 'City':
        return 'City';
      case 'Temperature':
        return 'Temperature';
      case 'Wind_Chill':
        return 'Wind Chill';
      case 'Humidity':
        return 'Humidity';
      case 'Visibility':
        return 'Visibility';
      case 'Road':
        return 'Road conditions';
      default:
        return "default"
    }
  }

  return (
    <div className="secondary-navbar" >
      {filters.map((filter) => (
        <Button
          key={filter}
          onClick={() => onFilterSelected(filter)}
          variant="outlined"
          style={{
            margin: '0 8px', 
            color: selectedFilter === filter ? 'darkslategray' : 'white', 
            borderColor: 'white', 
            backgroundColor: selectedFilter === filter ? 'white' : 'transparent' 
          }}
        >
          {switchFilter(filter)}
        </Button>
      ))}
    </div>
  );
};

const Content = ({ selectedItem, secondaryFilter}) => { 
  console.log(secondaryFilter)
  const renderChart = () => {
    switch(secondaryFilter) {
      case "Year": 
        return <LineChart props="Year"/>;
      case "Month": 
        return <LineChart props="Month"/>;
      case "Week": 
        return <LineChart props="Week"/>;
      case "Hour": 
        return <LineChart props="Hour"/>;
      case "US State": 
        return <GraphMap />;
      case "Temperature": 
        return <BarChart props="Temperature"/>;
      case "Wind_Chill": 
        return <BarChart props="WindChill"/>;
      case "Humidity": 
        return <BarChart props="Humidity"/>;  
      case "Visibility": 
        return <BarChart props="Visibility"/>;
      case "Road": 
        return <BarChart props="Road"/>;;
      default:
        return <div>Please Click the Top Tag</div>
    }
  }

  return (
    <div className="content">
      {renderChart()}
    </div>
  );
};

const DataAnalysis = () => {
  const [selectedItem, setSelectedItem] = useState('time');
  const [secondaryFilter, setSecondaryFilter] = useState('');

  const allFiltersMap = {
    'time': ['Year', 'Month', 'Week', 'Hour'],
    'geography': ['US State'],
    'weather': ['Temperature', 'Wind_Chill', 'Humidity', 'Visibility', 'Road']
  };

  useEffect(() => {
    const filters = allFiltersMap[selectedItem];
    if (filters && filters.length > 0) {
      setSecondaryFilter(filters[0]); // 
    }
  }, [selectedItem]); // 

  const allFilters = allFiltersMap[selectedItem];

  return (

    <div className="app-container">
      <Sidebar onItemSelected={(item) => {
        setSelectedItem(item);
        if (item !== selectedItem) {
          const firstFilter = allFiltersMap[item] && allFiltersMap[item][0];
          setSecondaryFilter(firstFilter || ''); 
        }
      }} />
      <div className="content-container">
        <SecondaryNavbar filters={allFilters} onFilterSelected={(filter) => setSecondaryFilter(filter)} />
        <Content
          selectedItem={selectedItem}
          secondaryFilter={secondaryFilter}
          allFilters={allFilters}
        />
      </div>
    </div>
  );
};

export default DataAnalysis;