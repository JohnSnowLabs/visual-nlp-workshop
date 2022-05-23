
def DBTOKEN = "DATABRICKS_TOKEN"

pipeline {
    agent {
        dockerfile {
                filename '.ci/Dockerfile.build'
        }
    }

    stages {
        stage('Setup') {
            withCredentials([string(credentialsId: DBTOKEN, variable: 'TOKEN')]) {
                sh """#!/bin/bash
                    # Configure Databricks CLI for deployment
                    echo "${DBURL}
                    $TOKEN" | databricks configure --token

                    # Configure Databricks Connect for testing
                    echo "${DBURL}
                    $TOKEN
                    ${CLUSTERID}
                    0
                    15001" | databricks-connect configure
                   """
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
