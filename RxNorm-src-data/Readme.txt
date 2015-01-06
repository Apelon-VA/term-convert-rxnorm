Steps to deploy new source content:

	1) Place the source files into the native-source folder
	2) Update the version number as appropriate in pom.xml
	3) Run a command like this to deploy - (maestro is a server name that must be defined with credentials in your maven configuration):
		mvn deploy -DaltDeploymentRepository=maestro::default::https://va.maestrodev.com/archiva/repository/data-files/
		
Note - new source content should not be checked into SVN.  When finished, simply empty the native-source folder.

For RxNorm - the loader currently expects a single zip file - typically:
  RxNorm_full_05062013.zip

The date in the file name doesn't matter, it just needs to be a zip file which starts with RxNorm_full.
