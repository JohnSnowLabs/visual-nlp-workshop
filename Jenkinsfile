
pipeline {
    agent {
        dockerfile {
                filename '.ci/Dockerfile.build'
        }
    }

    stages {
        stage('Copy notebooks to Databricks') {
            steps {
                script {
                    sh('databricks  workspace import_dir -o  "./databricks/python" "/Shared/Spark OCR/tests/"')
                }
            }
        }
    }
}
