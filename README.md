# Hippo Claim Revert Data Project

A lightweight data pipeline for analyzing pharmacy claims and reverts.

---
## Requirements

- Python 3.10+ (recommended: 3.12 with `pyenv` or virtual environment)
- Install dependencies:



## 1. Setup

### Clone the repository

```bash
git clone https://github.com/lucasrbarbosa-bos/hippo-data-project.git
cd hippo-data-project
```

## Create a virtual environment & install requirements
```
python -m venv venv
```

### Activate On Linux/Mac
```
source venv/bin/activate
```
### Activate On Windows (PowerShell)

```
.\venv\Scripts\Activate.ps1
```
```
pip install -r requirements.txt
```

## 2. Usage
Run the full pipeline in one go:

```bash
python main.py \
  --pharmacies-dir data/pharmacies \
  --claims-dir data/claims \
  --reverts-dir data/reverts \
  --outdir outputs
```

Run in stages (parse → validate → transform):

```
python main.py \
  --pharmacies-dir data/pharmacies \
  --claims-dir data/claims \
  --reverts-dir data/reverts \
  --staging-dir staging \
  --outdir outputs \
  --no-stage false
```
## Arguments

- `--pharmacies-dir PATH → folder with pharmacy data`
- `--claims-dir PATH → folder with claims data`
- `--reverts-dir PATH → folder with reverts data`
- `--staging-dir PATH → optional staging folder (default: staging)`
- `--outdir PATH → output folder (default: outputs)`
- `--no-stage true|false → run everything at once (true) or stage the files first (false)`

#### The --no-stage Flag

- `--no-stage true (default)`:
Runs the pipeline directly on the files in data/pharmacies, data/claims, and data/reverts.

- `--no-stage false:`
Before running the rest of the pipeline, all input files are first copied into a staging/ folder.
The pipeline then processes events from the staged files, ensuring only events that exist in the pharmacy dataset are included.
This is useful due to a clear reproducible staging step before processing.



## 3. Outputs
After running, results are written to the output folder:

- metrics_by_npi_ndc.json
- top2_chains_by_ndc.json
- most_common_quantities_by_ndc.json

