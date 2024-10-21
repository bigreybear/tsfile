mvn [clean] package -pl seart -am -Pdependency-in-lib
Always with these options to skip checks:
-Dmaven.test.skip=true -Drat.skip=true -Dspotless.check.skip=true -Dcheckstyle.skip=true
Package with following plugins could be easily portable.

    <build>
        <plugins>
            <!-- Maven Dependency Plugin -->
            <plugin>
                <artifactId>maven-dependency-plugin</artifactId>
                <version>3.4.0</version>
                <executions>
                    <execution>
                        <id>copy-dependencies</id>
                        <phase>package</phase>
                        <goals>
                            <goal>copy-dependencies</goal>
                        </goals>
                        <configuration>
                            <outputDirectory>${project.build.directory}/lib</outputDirectory>
                        </configuration>
                    </execution>
                </executions>
            </plugin>

            <!-- Maven Jar Plugin -->
            <plugin>
                <artifactId>maven-jar-plugin</artifactId>
                <version>3.3.0</version>
                <configuration>
                    <archive>
                        <manifest>
                            <addClasspath>true</addClasspath>
                            <classpathPrefix>lib/</classpathPrefix>
                            <mainClass>com.example.Main</mainClass>
                        </manifest>
                    </archive>
                </configuration>
            </plugin>
        </plugins>
    </build>