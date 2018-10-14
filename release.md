### Release a new version
mvn clean -DskipTests -Darguments=-DskipTests release:prepare -DignoreSnapshots=true
mvn clean -DskipTests -Darguments=-DskipTests release:perform -DignoreSnapshots=true

