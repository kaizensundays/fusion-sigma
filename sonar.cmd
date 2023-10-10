
set JAVA_HOME=%JAVA_11_HOME%

mvn verify sonar:sonar %SONAR_OPTS% -Dsonar.projectKey=kaizensundays_fusion-sigma -Dsonar.branch.name=dev -P sonar
