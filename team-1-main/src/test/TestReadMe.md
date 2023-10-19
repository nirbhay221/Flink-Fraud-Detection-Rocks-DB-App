### 1. Add theset dependencies into the pom.xml

<!-- https://mvnrepository.com/artifact/io.prometheus/simpleclient -->
		<dependency>
			<groupId>io.prometheus</groupId>
			<artifactId>simpleclient</artifactId>
			<version>0.16.0</version>
		</dependency>

		<!-- https://mvnrepository.com/artifact/io.prometheus/simpleclient_httpserver -->
		<dependency>
			<groupId>io.prometheus</groupId>
			<artifactId>simpleclient_httpserver</artifactId>
			<version>0.16.0</version>
		</dependency>

		<!-- https://mvnrepository.com/artifact/io.prometheus/simpleclient_hotspot -->
		<dependency>
			<groupId>io.prometheus</groupId>
			<artifactId>simpleclient_hotspot</artifactId>
			<version>0.16.0</version>
		</dependency>

### 2. go the directory where the pom.xml file is and run "mvn test" to test on all the tests
