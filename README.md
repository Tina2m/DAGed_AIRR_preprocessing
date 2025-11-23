Below are the steps for running the website locally on Windows (PowerShell). The interface will launch at:
http://localhost:8000/ui/
________________________________________
1) Install prerequisites
Run PowerShell as Administrator.
# Install Git (if not installed)
winget install -e --id Git.Git

# Install Docker Desktop for Windows
winget install -e --id Docker.DockerDesktop
Notes:
•	After installing Docker Desktop, please sign out and back in, or reboot.
•	When Docker starts for the first time, allow it to enable WSL 2 if prompted.
________________________________________
2) Clone the repository
Choose a directory where you want to place the source code:
cd $env:USERPROFILE
git clone https://github.com/Tina2m/presto-click
cd presto-click
(Optional) Pre-pull the Immcantation/pRESTO suite to avoid delays during first run:
docker pull immcantation/suite:4.6.0
docker run --rm immcantation/suite:4.6.0 bash -lc "FilterSeq.py --version"
________________________________________
3) Build the Docker image
From the repository root:
docker build -t immunostream:latest .
________________________________________
4) Run the container
The backend stores session data under /data and serves the UI under /ui.
We’ll bind-mount a local data folder and expose port 8000.
# Create local data directory
mkdir data -ErrorAction SilentlyContinue

# Run the server
docker run --name immunostream `
  -p 8000:8000 `
  -v ${PWD}\data:/data `
  -v ${PWD}\ui:/ui `
  immunostream:latest
Now open your browser and go to:
👉 http://localhost:8000/ui/
You should see the landing page.
 
