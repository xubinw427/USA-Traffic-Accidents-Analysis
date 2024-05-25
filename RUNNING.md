## Traffic-Accidents-Analysis-and-Prediction (for running locally)

### Prerequisites:

Please note that our project requires Python version 3.9 or higher to run.

```bash
# Clone repository locally
# HTTPS
git clone https://github.sfu.ca/zqa14/Traffic-Accidents-Analysis-and-Prediction.git
# (or) SSH
git clone git@github.sfu.ca:zqa14/Traffic-Accidents-Analysis-and-Prediction.git
# CD into folder using terminal
cd Traffic-Accidents-Analysis-and-Prediction
# install environment
pip install -r requirements.txt --no-binary :all:
```

Open a terminal as backend (please ensure that port 5000 is not in use):

# Starting from the FlightDelay_Analysis folder

# Open flask-server folder

cd ./flask-server

# (for Windows) enter the venv vitural environment

source venv/Scripts/activate

# (for MacOs) enter the venv vitural environment

source venv/bin/activate

# start the backend server on port "http://127.0.0.1:5000"

python server.py
