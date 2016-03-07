# Installation

Table of Contents:
- Requirements
- Installation
- Developer Guide

## Requirements

- Oracle JDK 8+ / OpenJDK 1.8+
- Maven 3.2.0+
- Vertica 6+ / Microsoft SQL Server XX (only necessary for using the designer tool)
- SWIG and GLPK (only for developers)

## Installation

Attention: Before you install cliffguard, make sure $JAVA_HOME is set.

Untar the package, then:

```bash
$ cd TOP_LEVEL_DIRECTORY
$ mvn clean compile package
```

After this step, the binary file (a JAR file) will be created in

```
TOP_LEVEL_DIRECTORY/target/VLogProcessor.jar
```

At this point, CliffGuard has been successfully compiled.
Refer to the README documentation to learn how you can use various tools that are included in the CliffGuard package.

## Developer Guide 
(Users should skip these steps)

*WARNING*: This developer guide is only intended for academic researchers. To follow the instructions below you need to be using the academic version (the academic version is different from the public oepn-source version that is available at [http://CliffGuard.org](http://CliffGuard.org)). The academic version is not open-source and is shared on a per-request basis. If you wish to access the academic version, please contact us at mozafari <AT> umich.edu.

### How to enable Vertica to empty the OS cache:

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

###  Importing into Eclipse

#### Requirements

- Eclipse LUNA+
- m2eclipse plugin for Eclipse

If you are not sure about installation of m2eclipse, check out Help -> Install New Software, then copy the link [http://download.eclipse.org/technology/m2e/releases](http://download.eclipse.org/technology/m2e/releases) into "Work with" input box, then press "Add" -> "OK", and check "Maven Integration for Eclipse" in the following list view. Then press "Next" -> "Next" -> "Finish".

#### How to set up the project into Eclipse

1. From Eclipse, Project Explorer -> Right click -> Select Import Menu and Import, or from top menu bar, File -> Import.

2. Expand Maven menu, click "Existing Maven Projects", and click next.

3. Browse the location where you have the CliffGuard source code, select the top level root directory.

4. Click next, Eclipse will recognize project and it will show you a list of all possible projects located there.

5. Select the project org.cliffguard.cliffguard, click "Finish", then Eclipse shows some build information, then done.

## Installing SWIG and GLPK

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
