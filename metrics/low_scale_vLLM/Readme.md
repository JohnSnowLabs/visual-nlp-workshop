# Key-Value and Table Extraction from Cloud Providers Output

## Overview

This repository contains Jupyter notebooks that demonstrate how to use key-value and table extraction services provided by various cloud providers. Specifically, we use Amazon Textract, Google Document AI, and Azure Form Recognizer to extract information from images. Additionally, there is a notebook for extracting key-value and table information as JSON files from in-house labeled ground truth.

## File Structure

- `credentials.json`: Configuration file containing the necessary credentials for accessing the cloud services.
- `src/`: Source files and notebooks path.
  - `cloud_providers/`: Cloud providers files path.
    - `Amazon_KV_Table_Extraction.ipynb`: Notebook for extracting key-value pairs and tables using Amazon Textract.
    - `Azure_KV_Table_Extraction.ipynb`: Notebook for extracting key-value pairs and tables using Azure Form Recognizer.
    - `Google_KV_Table_Extraction.ipynb`: Notebook for extracting key-value pairs and tables using Google Document AI.
    - `Cloud_Provider_Result_To_Evaluation_Format.ipynb`: Notebook to adapt ouput of cloud providers for the evaluation notebook.
  - `create_gt/`: Key value groundtruth generation files path.
    - `Generic_Report_GT_KV_Table_Extraction.ipynb`: Notebook for extracting key-value pairs and tables from in-house labeled ground truth.
  - `Evaluate_KV_Results.ipynb`: Notebook for evaluating key-value results.

- `data/`: datasets to reproduce metrics.


   - `FUNSD_GT.zip`: FUNSD Ground-Truth
   - `GenericReports01_GT.zip`: Generic Report Ground-Truth
   - `vLLM_v1_GenericReports01_fixoutput.zip`: Result of vLLM_v1 in Generic_Report01
   - `vLLM_v1_funsd_fixoutput.zip`: Result of vLLM_v1 in FUNSD
   - `vLLM_v2_GenericReports01_fixoutput.zip`: Result of vLLM_v in Generic_Report01
   - `vLLM_v2_funsd_fixoutput.zip`: Result of vLLM_v2 in FUNSD
   - `vLLM_v3_GenericReports01_fixoutput.zip`: Result of vLLM_v3 in Generic_Report01
   - `vLLM_v3_funsd_fixoutput.zip`: Result of vLLM_v3 in FUNSD

## Prerequisites

- Make sure images are available for processing.
- Add your credentials to the `credentials.json` file, before attempting to run any of the notebooks.
- Python 3.7+
- Jupyter Notebook
- Cloud service accounts with appropriate permissions for Amazon Textract, Google Document AI, and Azure Form Recognizer

## Running the Notebooks

1. **Amazon Textract:**
    - Open `Amazon_KV_Table_Extraction.ipynb` in Jupyter Notebook.
    - Follow the instructions within the notebook to extract key-value pairs and tables from images.
    - For Amazon Textract we can extract Table.csv and Key-Value json file.

2. **Azure Form Recognizer:**
    - Open `Azure_KV_Table_Extraction.ipynb` in Jupyter Notebook.
    - Follow the instructions within the notebook to extract key-value pairs and tables from images.

3. **Google Document AI:**
    - Open `Google_KV_Table_Extraction.ipynb` in Jupyter Notebook.
    - Follow the instructions within the notebook to extract key-value pairs and tables from images.

4. **Cloud_Provider_Result_To_Evaluation_Format:**
    - Open `Cloud_Provider_Result_To_Evaluation_Format.ipynb` in Jupyter Notebook.
    - Follow the instructions within the notebook to adapt results of cloud provider services to be evaluate.

5. **In-house Ground Truth:**
    - Open `Generic_Report_GT_KV_Table_Extraction.ipynb` in Jupyter Notebook.
    - Follow the instructions within the notebook to extract key-value pairs and tables from in-house labeled ground truth.

6. **Key-value Evaluation:**
    - Open `Evaluate_KV_Results.ipynb` in Jupyter Notebook.
    - Follow the instructions within the notebook to evaluate key-value results.


## References

- [Amazon Textract](https://aws.amazon.com/textract/)
- [Google Document AI](https://cloud.google.com/document-ai)
- [Azure Form Recognizer](https://azure.microsoft.com/en-us/services/form-recognizer/)
