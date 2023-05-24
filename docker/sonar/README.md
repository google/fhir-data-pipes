# Run Static Code Analysis Using SonarQube

Include sonar and jacoco related properties to the pom.xml where you want to run the analysis:

Like e.g. pipeline's pom.xml has jacoco dependency and plugin in its build, so when you run `mvn clean install`,
it is going to generate report along with jacoco.exe in all the dependents target folders e.g. controller's target
folder. For sonar to use the coverage, you can add this to controller's pom.xml properties:

```
    <sonar.java.coveragePlugin>jacoco</sonar.java.coveragePlugin>
    <sonar.dynamicAnalysis>reuseReports</sonar.dynamicAnalysis>
    <sonar.jacoco.reportPath>${project.basedir}/target/jacoco.exec</sonar.jacoco.reportPath>
    <sonar.language>java</sonar.language>
    <sonar.host.url>http://localhost:9000</sonar.host.url>
    <sonar.projectName>pipelines-controller</sonar.projectName>
    <sonar.login>admin</sonar.login>
    <sonar.password>admin</sonar.password>
    <sonar.java.binaries>target/classes</sonar.java.binaries>
```

Bring up SonarQube docker container. Like if you are at root, run the following:

```
    docker-compose -f docker/sonar/sonarqube-compose.yaml up
```

Build the package:

```
    mvn clean package
```

Then run the maven sonar command where you added sonar properties, lets say controller:

```
    mvn sonar:sonar
```

Once the above command runs successfully, you should be able to see the analysis report on browser:

```
    http://localhost:9000
```

For any subsequent change, just run clean package and sonar and refresh the browser to verify your fix:

```
    mvn clean package
    mvn sonar:sonar
```