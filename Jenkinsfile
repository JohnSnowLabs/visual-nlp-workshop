@Library('jenkinslib')_

def DBTOKEN = "DATABRICKS_TOKEN"
def DBURL = "https://dbc-6ca13d9d-74bb.cloud.databricks.com/"
def CLUSTERID = "0428-112519-vaxgi8gx"

pipeline {
    agent {
        dockerfile {
                filename '.ci/Dockerfile.build'
        }
    }
    stages {
        stage('Setup') {
            steps {
                script {
                    withCredentials([string(credentialsId: DBTOKEN, variable: 'TOKEN')]) {
                        sh(script: '''#!/bin/bash
                            echo "$TOKEN" > secret.txt
                            databricks configure --token-file secret.txt --host $DBURL''')
                    }
                }
            }
        }
        stage('Copy notebooks to Databricks') {
            steps {
                script {
                    sh('databricks  workspace import_dir -o  "./databricks/python" "/Shared/Spark OCR/tests/"')
                }
            }
        }
    }
}
