import React, { useEffect, useState } from "react";
import { Line } from "react-chartjs-2";
import "chart.js/auto";

function LineChart({ props }) {
  const [data, setData] = useState({});

  useEffect(() => {
    fetch(`http://localhost:5000/lineChartData/${props}`)
      // fetch(`http://52.9.248.230/lineChartData/${props}`)
      .then((response) => {
        console.log("Response received: ", response);
        return response.json();
      })
      .then((data) => setData(data))
      .catch((error) => console.error("Error fetching data: ", error));
  }, [props]);

  const options = {
    scales: {
      x: {
        title: {
          display: true,
          text: props,
          font: {
            size: 15,
          },
        },
      },
      y: {
        title: {
          display: true,
          text: "Number of Accidents",
          font: {
            size: 15,
          },
        },
      },
    },
    plugins: {
      legend: {
        display: true,
        position: "top",
      },
    },
  };

  const startYear = 2016;
  const endYear = 2022;
  const colorMap = {
    2016: "rgb(75, 192, 192)",
    2017: "red",
    2018: "green",
    2019: "brown",
    2020: "orange",
    2021: "blue",
    2022: "purple",
  };

  let datasets = [];
  for (let year = startYear; year <= endYear; year++) {
    datasets.push({
      label: `${props} in ${year}`,
      data: data[`attribute_${year}`],
      fill: false,
      borderColor: colorMap[year] || "gray",
      tension: 0.1,
    });
  }

  const inputs = {
    labels: data["label"],
    datasets: datasets,
    // datasets: [
    //   {
    //     label: props + " in 2016 ",
    //     data: data['attribute2'],
    //     fill: false,
    //     borderColor: 'rgb(75, 192, 192)',
    //     tension: 0.1
    //   },
    //   {
    //     label: props + " in 2017 ",
    //     data: data['attribute3'],
    //     fill: false,
    //     borderColor: 'red',
    //     tension: 0.1
    //   },
    //   {
    //     label: props + " in 2018 ",
    //     data: data['attribute4'],
    //     fill: false,
    //     borderColor: 'green',
    //     tension: 0.1
    //   }
    // ],
  };

  const inputs_year = {
    labels: data["label"],
    datasets: [
      {
        label: props,
        data: data["attribute"],
        fill: false,
        borderColor: "rgb(75, 192, 192)",
        tension: 0.1,
      },
    ],
  };

  const input = props === "Year" ? inputs_year : inputs;

  return <div>{inputs && <Line data={input} options={options} />}</div>;
}

export default LineChart;
