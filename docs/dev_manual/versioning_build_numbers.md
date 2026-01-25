# Versioning + Build Metadata (nbdatatools Pattern)

Goal: keep a single project version, stamp builds with SCM-based metadata, and expose it via a CLI-friendly resource file.

## How this repo does it
- Single source of truth: `datatools-mvn-defaults/pom.xml` defines `<revision>0.1.15-SNAPSHOT</revision>`; all module `pom.xml` files use `${revision}`. See `project-requirements.md` for the policy.
- Build metadata: `datatools-mvn-defaults/pom.xml` configures `buildnumber-maven-plugin` to set:
  - `build.number` from SCM revision
  - `build.timestamp` via `create-timestamp`
- Runtime visibility: `datatools-commands/src/main/resources/version.properties` is Maven-filtered in `datatools-commands/pom.xml` and is loaded by `datatools-commands/src/main/java/io/nosqlbench/command/version/CMD_version.java`.
- Release tags/scripts:
  - `scripts/tag-release-build`, `scripts/tag-snapshot`, `scripts/trigger-release-build`
  - `scripts/get-release-version.sh` validates that the Git tag matches the Maven revision
  - `scripts/bump-minor-version` updates `${revision}` to the next `-SNAPSHOT`

## Portable recipe (copy into another project)
1. Centralize versioning:
   - In a root or defaults `pom.xml`, define:
     ```xml
     <properties>
       <revision>0.1.0-SNAPSHOT</revision>
     </properties>
     ```
   - Use `${revision}` for all `<version>` declarations.

2. Generate build metadata at build time:
   - Add to `pluginManagement` (or shared parent) so all modules can use it:
     ```xml
     <plugin>
       <groupId>org.codehaus.mojo</groupId>
       <artifactId>buildnumber-maven-plugin</artifactId>
       <version>3.2.1</version>
       <executions>
         <execution>
           <id>create-timestamp</id>
           <phase>validate</phase>
           <goals><goal>create-timestamp</goal></goals>
           <configuration>
             <timestampFormat>yyyy-MM-dd HH:mm:ss</timestampFormat>
             <timestampPropertyName>build.timestamp</timestampPropertyName>
           </configuration>
         </execution>
         <execution>
           <id>create-buildnumber</id>
           <phase>validate</phase>
           <goals><goal>create</goal></goals>
           <configuration>
             <doCheck>false</doCheck>
             <doUpdate>false</doUpdate>
             <revisionOnScmFailure>unknown</revisionOnScmFailure>
             <buildNumberPropertyName>build.number</buildNumberPropertyName>
           </configuration>
         </execution>
       </executions>
     </plugin>
     ```
   - Ensure `<scm>` is configured in the parent POM so SCM revision is available.

3. Expose version/build info to runtime:
   - Create `src/main/resources/version.properties`:
     ```properties
     version=${project.version}
     groupId=${project.groupId}
     artifactId=${project.artifactId}
     buildNumber=${build.number}
     buildTimestamp=${build.timestamp}
     ```
   - Enable resource filtering for that file:
     ```xml
     <resources>
       <resource>
         <directory>src/main/resources</directory>
         <filtering>true</filtering>
         <includes>
           <include>version.properties</include>
         </includes>
       </resource>
     </resources>
     ```

4. Provide a runtime accessor:
   - Add a small CLI/endpoint that reads `version.properties` from the classpath and prints fields.
   - Handle the "no SCM / dev build" case by defaulting `buildNumber` and `buildTimestamp`.

5. Release discipline (optional but recommended):
   - Add lightweight scripts to:
     - bump `${revision}` to the next `-SNAPSHOT`
     - tag releases (`X.Y.Z`) and snapshots (`X.Y.Z-snapshot`)
     - validate that a tag matches the Maven revision before release.

## Notes
- This pattern keeps Maven CI-friendly (`${revision}`) versions while still surfacing build numbers in the shipped artifact.
- If you publish a fat jar, you can also use the same properties to stamp filenames or symlinks.
