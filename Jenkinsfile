@Library('jenkinslib')_

def DBTOKEN = "DATABRICKS_TOKEN"
def DBURL = "https://dbc-6ca13d9d-74bb.cloud.databricks.com"
//def CLUSTERID = "0428-112519-vaxgi8gx"
def SCRIPTPATH = "./.ci"
def NOTEBOOKPATH = "./databricks/python"
def WORKSPACEPATH = "/Shared/Spark OCR/tests"
def OUTFILEPATH = "."
def TESTRESULTPATH = "./reports/junit"
def IGNORE = "3. Compare CPU and GPU image processing with Spark OCR.ipynb"

def SPARK_NLP_VERSION = "3.4.2"
def SPARK_NLP_HEALTHCARE_VERSION = "3.4.2"
def SPARK_OCR_VERSION = "3.12.0"

def PYPI_REPO_HEALTHCARE_SECRET = sparknlp_helpers.spark_nlp_healthcare_secret(SPARK_NLP_HEALTHCARE_VERSION)
def PYPI_REPO_OCR_SECRET = sparknlp_helpers.spark_ocr_secret(SPARK_OCR_VERSION)


pipeline {
    agent {
        dockerfile {
                filename '.ci/Dockerfile.build'
        }
    }
    environment {
        DATABRICKS_CONFIG_FILE = ".databricks.cfg"
        GITHUB_CREDS = credentials('55e7e818-4ccf-4d23-b54c-fd97c21081ba')
    }
    parameters {
        choice(
            name:'databricks_runtime',
            choices:'6.4.x-esr-scala2.11\n7.3.x-cpu-ml-scala2.12\n7.3.x-hls-scala2.12\n10.2.x-gpu-ml-scala2.12\n10.5.x-aarch64-scala2.12\n7.3.x-gpu-ml-scala2.12\n10.2.x-aarch64-photon-scala2.12\n10.4.x-cpu-ml-scala2.12\n9.1.x-aarch64-scala2.12\n10.1.x-photon-scala2.12\n9.1.x-photon-scala2.12\n10.4.x-scala2.12\n10.2.x-photon-scala2.12\n10.4.x-photon-scala2.12\n11.0.x-photon-scala2.12\n10.3.x-photon-scala2.12\n10.5.x-photon-scala2.12\n10.1.x-gpu-ml-scala2.12\n9.1.x-scala2.12\n11.0.x-scala2.12\n10.3.x-cpu-ml-scala2.12\n10.3.x-aarch64-photon-scala2.12\n11.0.x-gpu-ml-scala2.12\n10.5.x-aarch64-photon-scala2.12\n10.1.x-cpu-ml-scala2.12\n10.4.x-aarch64-photon-scala2.12\n10.5.x-gpu-ml-scala2.12\napache-spark-2.4.x-esr-scala2.11\n10.1.x-scala2.12\n9.1.x-cpu-ml-scala2.12\n11.0.x-cpu-ml-scala2.12\n10.2.x-aarch64-scala2.12\n10.2.x-scala2.12\n10.2.x-cpu-ml-scala2.12\n11.0.x-aarch64-photon-scala2.12\n10.4.x-aarch64-scala2.12\n11.0.x-aarch64-scala2.12\n10.1.x-aarch64-scala2.12\n9.1.x-gpu-ml-scala2.12\napache-spark-2.4.x-scala2.11\n10.5.x-scala2.12\n7.3.x-scala2.12\n10.3.x-scala2.12\n10.3.x-aarch64-scala2.12\n10.5.x-cpu-ml-scala2.12\n10.3.x-gpu-ml-scala2.12\n10.4.x-gpu-ml-scala2.12',
            description:'define spark version',
            defaultValue: '7.3.x-scala2.12'
        )
    }
    stages {
        stage('Setup') {
            steps {
                script {
                    withCredentials([string(credentialsId: DBTOKEN, variable: 'TOKEN')]) {
                        sh('echo "${TOKEN}" > secret.txt')
                        sh("databricks configure --token-file secret.txt --host ${DBURL}")
                    }
                }
            }
        }
        stage('Copy notebooks to Databricks') {
            steps {
                script {
                    sh("databricks  workspace import_dir -o '${NOTEBOOKPATH}' '${WORKSPACEPATH}'")
                }
            }
        }
        stage('Create Cluster') {
            steps {
                script {
                    withCredentials([string(credentialsId:'TEST_SPARK_OCR_LICENSE',variable:'SPARK_OCR_LICENSE'),[
                        $class: 'AmazonWebServicesCredentialsBinding',
                        credentialsId: 'a4362e3b-808e-45e0-b7d2-1c62b0572df4',
                        accessKeyVariable: 'AWS_ACCESS_KEY_ID',
                        secretKeyVariable: 'AWS_SECRET_ACCESS_KEY']]) {
                        def jsonCluster = '''
                        {
                            "num_workers": 1,
                            "cluster_name": "Spark Ocr Notebook Test",
                            "spark_version": "${databricks_runtime}",
                            "spark_conf": {
                              "spark.sql.legacy.allowUntypedScalaUDF": "true"
                            },
                            "aws_attributes": {
                              "first_on_demand": 1,
                              "availability": "SPOT_WITH_FALLBACK",
                              "zone_id": "us-west-2a",
                              "spot_bid_price_percent": 100,
                              "ebs_volume_count": 0
                            },
                            "node_type_id": "i3.xlarge",
                            "driver_node_type_id": "i3.xlarge",
                            "spark_env_vars": {
                              "JSL_OCR_LICENSE": "${SPARK_OCR_LICENSE}",
                              "AWS_ACCESS_KEY_ID": "${AWS_ACCESS_KEY_ID}",
                              "AWS_SECRET_ACCESS_KEY": "${AWS_SECRET_ACCESS_KEY}"
                            },
                            "autotermination_minutes": 20,
                        }
                        '''
                        def clusterRespString = sh(returnStdout: true, script: "databricks clusters create --json ${jsonCluster}")
                        def CLUSTERID = (readJSON text: clusterRespString)['cluster_id']
                        }
                    }
                }
            }
        }
        stage('Install deps to Cluster') {
            steps {
                script {
                    sh("databricks libraries uninstall --cluster-id ${CLUSTERID} --all")
                    sh("databricks libraries install --cluster-id ${CLUSTERID} --jar  s3://pypi.johnsnowlabs.com/${PYPI_REPO_OCR_SECRET}/jars/spark-ocr-assembly-${SPARK_OCR_VERSION}-spark30.jar")
                    sh("databricks libraries install --cluster-id ${CLUSTERID} --jar  s3://pypi.johnsnowlabs.com/${PYPI_REPO_HEALTHCARE_SECRET}/spark-nlp-jsl-${SPARK_NLP_HEALTHCARE_VERSION}.jar")
                    sh("databricks libraries install --cluster-id ${CLUSTERID} --maven-coordinates com.johnsnowlabs.nlp:spark-nlp_2.12:${SPARK_NLP_VERSION}")
                    sh("databricks libraries install --cluster-id ${CLUSTERID} --whl s3://pypi.johnsnowlabs.com/${PYPI_REPO_OCR_SECRET}/spark-ocr/spark_ocr-${SPARK_OCR_VERSION}+spark30-py3-none-any.whl")
                    sh("databricks libraries install --cluster-id ${CLUSTERID} --whl s3://pypi.johnsnowlabs.com/${PYPI_REPO_HEALTHCARE_SECRET}/spark-nlp-jsl/spark_nlp_jsl-${SPARK_NLP_VERSION}-py3-none-any.whl")
                    sh("databricks libraries install --cluster-id ${CLUSTERID} --pypi-package spark-nlp==${SPARK_NLP_VERSION}")
                }
            }
        }
        stage('Start cluster') {
            steps {
                script {
                    def respString = sh script: "databricks clusters get --cluster-id ${CLUSTERID}", returnStdout: true
                    def respJson = readJSON text: respString
                    if (respJson['state'] == 'RUNNING') {
                        sh("databricks clusters restart --cluster-id ${CLUSTERID}")
                    } else {
                        sh("databricks clusters start --cluster-id ${CLUSTERID}")
                    }
                    timeout(10) {
                        waitUntil {
                           script {
                             def respStringWait = sh script: "databricks clusters get --cluster-id ${CLUSTERID}", returnStdout: true
                             def respJsonWait = readJSON text: respStringWait
                             return (respJsonWait['state'] == 'RUNNING');
                           }
                        }
                    }
                }
            }
        }
        stage('Run Notebook Tests') {
            steps {
                script {
                    withCredentials([string(credentialsId: DBTOKEN, variable: 'TOKEN')]) {
                        sh """python3 $SCRIPTPATH/executenotebook.py --workspace=$DBURL\
                                        --token=$TOKEN\
                                        --clusterid=$CLUSTERID\
                                        --localpath=${NOTEBOOKPATH}\
                                        --workspacepath='${WORKSPACEPATH}'\
                                        --outfilepath='${OUTFILEPATH}'\
                                        --ignore='${IGNORE}'
                           """
                        sh """sed -i -e 's #ENV# ${OUTFILEPATH} g' ${SCRIPTPATH}/evaluatenotebookruns.py
                              python3 -m pytest -s --junit-xml=${TESTRESULTPATH}/TEST-notebookout.xml ${SCRIPTPATH}/evaluatenotebookruns.py
                           """
                    }
                }
            }
        }
    }
    post {
        always {
            sh "find ${OUTFILEPATH} -name '*.json' -exec rm {} +"
            junit allowEmptyResults: true, testResults: "**/reports/junit/*.xml"
        }
    }
}
