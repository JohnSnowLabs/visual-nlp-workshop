# Spark OCR Workshop
Public examples of using John Snow Labs' OCR for Apache Spark.

## Installing and Quickstart Instructions

You need `Secret key` and `License key`.
Please contact us at info@johnsnowlabs.com for get it.

`Secret key` is a key for download or install python package and jar from https://pypi.johnsnowlabs.com/.
`Secret key` is specific for each release.

`License key` is a key for run Spark OCR. 

### Setting up the license key

#### Setting up in notebook

Need set value for `license` variable:

```python
license = "license key"
```

#### Using Environment variable

Linux, Mac OS:

```bash
export JSL_OCR_LICENSE=license_key
```
Windows:

```cmd
set JSL_OCR_LICENSE=license_key
```

### Run notebooks on Google Colab

* Go to the https://colab.research.google.com/
* Open notebook:
  * File -> Open notebook.
  * Switch to `Github` tab.
  * Enter url: https://github.com/JohnSnowLabs/spark-ocr-workshop.
  * Select `SparkOcrSimpleExample.ipynb` notebook.
* Set `secret` and `license` variables to valid values in first cell.
* Run all cells: Runtime -> Run all.
* Restart runtime: Runtime -> Restart runtime (Need restart first time after installing new packages).
* Run all cellls again.

### Run notebooks locally using jupyter

* Install jupyter notebooks. More details: https://jupyter.org/install :
```bash
python3 -m pip install --user jupyter
```
* Clone Spark OCR workshop repo:
```bash
git clone https://github.com/JohnSnowLabs/spark-ocr-workshop
cd spark-ocr-workshop
```
* Run jupyter:
```bash
jupyter-notebook
```
* Open jupyter in browser
* Open `jupyter/SparkOcrSimpleExample.ipynb` notebook.
* Set `secret` and `license` variables to valid values in first cell.
* Run all cells: Cell -> Run all.
* Restart runtime: Kernel -> Restart (Need restart first time after installing new packages).
* Run all cellls again.

### Run at java project

It is possible to call functionality from Java project.
`./java/ folder` contains sample of Java project that could be built with Maven.


