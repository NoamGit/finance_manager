# Getting started

The app is build with a single `docker-compose` file so you need to install docker beforehand. You will also need to set
the `.env` file with the proper secrets.

**python** - you need to run the `poetry install` to build you python environment. There is a single env common to all flows.

## Scrapers 
### Using Israeli bank scarpers
**Otsar-Hahayal scraper** - For Otsar-Hahayal you must disable FibiGuard (see personal settings after login, ) or approve the chromium browser as a legit device on the app. 

## Prefect orchestration

### GCS - Setting storage for your flows in google
use this manual to create a 5GB free object storagein gcp
https://cloud.google.com/docs/authentication/getting-started

configure your `GOOGLE_APPLICATION_CREDENTIALS` in your .env file

### Configure a storage block 
Add manual

### Running/Testing with prefect
1. Set execution workspace to cloud with `prefect cloud workspace set --workspace "noamboxgmailcom/finance-bi-prod"` (you can get the string from the prefect-cloud UI)
1. go to `cd ~/projects_storage/finance_manager/flows/credit_scrapers`
2. activate environment with `poetry shell`
4. `TODO` Set python path with `export PYTHONPATH=$PYTHONPATH:/src`
5. `TODO` Set puppeteer path with `export PUPPETEER_EXECUTABLE_PATH='/snap/bin/chromium'`
6. start an agent with `prefect agent start `standard-queue` `

5. [optional] set run parameters and test run with `python isracard_flow.py`
6. You should see a run in prefect cloud

### Deploy prefect flow
1. Activate local-agent with `prefect agent start --work-queue "ubuntu-local-agent"`
2. Build the flow with `prefect deployment build flows/bank_scrapers/otsar_hahayal_flow.py:scrape_otsar_hahayal -n otsar-hahayal-scrape -q ubuntu-local-agent -sb gcs/flow-storage -o flows/deployments/otsar_hahyal.yaml`
3. After reviewing and modifying (if needed) the .yaml file apply your deployment with `prefect deployment apply flows/deployments/otsar_hahyal.yaml`
4. `TODO` Set puppeteer path with `export PUPPETEER_EXECUTABLE_PATH='/snap/bin/chromium'`
4. Make sure the agent you've specified is up with `prefect agent start -q 'ubuntu-local-agent'`
5. Run your deployment from UI

## Financial-manager services
### mysql-backup
https://github.com/databacker/mysql-backup

#### superset

`chmod a+x superset-entrypoint.sh`

### Q & A

Docker is hanging and not stopping?
> `sudo service docker stop` https://stackoverflow.com/questions/39130263/docker-proxy-using-port-when-no-containers-are-running

#### Permission denied (in deployment)
When facing `errno 13 permission denied`

use `sudo chmod -R 755 <path>` to allow full recursive access to the folder