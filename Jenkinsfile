#!groovy
@Library('gini-shared-library@master') _


pipeline {
    agent {
        label 'base'
    }

    environment {
            PROJECT_NAME = getProjectName()
        }

    options {
        gitLabConnection('git.i.gini.net')
    }

    stages {
        stage('Build and test') {
            steps {
                sh """./gradlew -g=/efs/${PROJECT_NAME} \
                        -I /home/jenkins/gradle/init.gradle \
                        -Pbranch=${env.GIT_BRANCH} \
                        -PbuildNumber=${env.BUILD_NUMBER} \
                        clean build"""
            }
        }
        stage('upload archives') {
            steps {
                sh """./gradlew -g=/efs/${PROJECT_NAME} \
                    -I /home/jenkins/gradle/init.gradle \
                    -Pbranch=${env.GIT_BRANCH} \
                    -PbuildNumber=${env.BUILD_NUMBER} \
                    uploadArchives"""

            }
        }
    }
}
