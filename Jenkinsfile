@Library('jenkinslib')_

def DBTOKEN = "DATABRICKS_TOKEN"
def DBURL = "https://dbc-6ca13d9d-74bb.cloud.databricks.com"
def CLUSTERID = "0428-112519-vaxgi8gx"
def SCRIPTPATH = "./.ci"
def NOTEBOOKPATH = "./databricks/python"
def WORKSPACEPATH = "/Shared/Spark OCR/tests"
def OUTFILEPATH = "."
def TESTRESULTPATH = "./reports/junit"
def IGNORE = "3. Compare CPU and GPU image processing with Spark OCR.ipynb"

pipeline {
    agent {
        dockerfile {
                filename '.ci/Dockerfile.build'
        }
    }
    environment {
        DATABRICKS_CONFIG_FILE = ".databricks.cfg"
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
