### Release a new version
mvn clean -DskipTests -Darguments=-DskipTests release:prepare
mvn clean -DskipTests -Darguments=-DskipTests release:perform

