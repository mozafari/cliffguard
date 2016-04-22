# Installation

Table of Contents:
- [Requirements](#requirements)
- [Installation](#installation)
- [Importing into Eclipse](#importing-into-eclipse)
- [Developer Guide](#developer-guide)

## Requirements

- One of Oracle JDK 1.8+ or OpenJDK 8+
- Maven 3.2.0+
- Vertica 6+ or Microsoft SQL Server 2008+ (only necessary for using the designer tool)
- SWIG 3.0.0+ and GLPK 4.56+ (only for developers)

## Installation

Attention: Before you install cliffguard, make sure $JAVA_HOME is set.

```bash
$ git clone https://github.com/mozafari/cliffguard.git
$ cd cliffguard
$ mvn clean package
```

After this step, the binary file (a JAR file) will be created in

```
./target/CliffGuard.jar
```

At this point, CliffGuard has been successfully compiled.
Refer to the README documentation to learn how you can use various tools that are included in the CliffGuard package.

##  Importing into Eclipse

### Requirements

- Eclipse (Luna or Mars version are recommended)
- m2eclipse plugin for Eclipse
- maven-build-helper plugin for Eclipse

To install m2eclipse plugin for Eclipse:

1. Select Window (from menu bar) -> Preference (or under the "Eclipse" menu on Mac OSX), expend Install/Update on side bar, and select "Available Software Sites",
    then if you are 
    + Eclipse Juno user: Enable the item with **Name**: `Juno` (or `Eclipse Juno repository`) and **Location**: `http://download.eclipse.org/releases/juno`
    + Eclipse Kepler user: Enable the item with **Name**: `Kepler` (or `Eclipse Kepler repository`) and **Location**: `http://download.eclipse.org/releases/kepler`
    + Eclipse Luna user: Enable the item with **Name** `Luna` (or `Eclipse Luna repository`) and **Location**: `http://download.eclipse.org/releases/luna`
    + Eclipse Mars user: Enable the item with **Name** `Mars` (or `Eclipse Mars repository`) and **Location**: `http://download.eclipse.org/releases/mars`
    
    Enable corresponding item, like the following:
    <br>
    <img src="src/main/resources/source.png" width="50%"/>
    <br>
    
2. Click Help -> Install New Software at menu bar. 
3. Check your Eclipse version,  
    + If you are an Eclipse Luna or Mars user, copy the link [http://download.eclipse.org/technology/m2e/releases](http://download.eclipse.org/technology/m2e/releases) 
    + If you are Juno or Kepler user, copy the link [http://download.eclipse.org/technology/m2e/releases/1.4](http://download.eclipse.org/technology/m2e/releases/1.4) 
   
4. Paste the link in previous step into "Work with" input box, then press "Add".
5. It is okay to leave the "Name" input box to be blank, then press "OK", and check "Maven Integration for Eclipse" in the following list view.
6. Check the option "Contact all update sites during install to find required software" at the bottom of the current window.
    At this point the screen should look like the following (again, Eclipse Juno and Kepler users should use http://download.eclipse.org/technology/m2e/releases/1.4).
    <br>
    <img src="src/main/resources/demo.png" width="50%"/>
    <br>
7. Press "Next" -> "Next" -> "Finish".

To install maven-build-helper plugin, you can just repeat the process for installing m2e plugin like the above: 

1. Click Help -> Install New Software at menu bar. 
2. Check your Eclipse version, copy the link http://repo1.maven.org/maven2/.m2e/connectors/m2eclipse-buildhelper/0.15.0/N/0.15.0.201207090124/
3. Paste the link in previous step into "Work with" input box, then press "Add".
4. Press "OK", and check "M2E Buildhelper Connector" in the following list view.
5. Press "Next" -> "Next" -> "Finish".

### How to set up the project into Eclipse

1. From Eclipse top menu bar, click File -> Import.

2. Expand Maven menu, click "Existing Maven Projects", and click next.

3. Browse the location where you have the CliffGuard source code, select the top level root directory, then click next.

4. Eclipse will recognize project and it will show you a list of all possible projects located there.

5. Select the project org.cliffguard.cliffguard, click "Next", then Eclipse shows some build information, then click "Finish".

6. Done! (There might be some automatic setup process conducted by Eclipse at this point, and it is okay to click "Next" as whatever Eclipse asks you to permit)  

## Developer Guide 
(Users should skip these steps)

**WARNING**: This developer guide is only intended for academic researchers. To follow the instructions below you need to be using the academic version (the academic version is different from the public oepn-source version that is available at [http://CliffGuard.org](http://CliffGuard.org)). The academic version is not open-source and is shared on a per-request basis. If you wish to access the academic version, please contact us at mozafari <AT> umich.edu.

#### How to enable Vertica to empty the OS cache:

1. Run the following command as "dbadmin":

```bash
$ TOP_LEVEL_DIRECTORY/scripts/install_stored_procedures.sh
```

2. Run the following as root:

```bash
$ sudo chown root:root restartDB
$ sudo chmod u+s restartDB
```

3. Test it by running the following command in vsql:

```bash
vsql> select barzan_empty_cache();
```

and see if it prints out root! 

```
	INFO 4427:  Procedure reported:
	root

	 barzan_empty_cache 
	--------------------
			  0
	(1 row)
```

#### Installing SWIG and GLPK

You only need to install SWIG and GLPK if you are looking to use some of the advanced features in CliffGuard. Not recommended for beginner users.

1. Make sure "swig" is installed, by trying:

```bash
$ swig
```

To install:

```bash
sudo yum install swig
```

2. Compile and install GLPK:

```bash
$ cd TOP_LEVEL_DIRECTORY/lib/
$ tar xvzf glpk-4.50.tar.gz
$ cd glpk-4.50
$ ./configure 
$ make
$ sudo make install
```
