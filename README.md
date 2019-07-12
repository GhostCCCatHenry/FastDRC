# FastDRC
Distributed biological sequences Compression

The code in the compress folder is the compression code of FastDRC. 

The author saves the HDFS mapping path of the sequence file into the file and reads the path through file reading. Your path may be different, so you need to modify the path.

Place each path file in a folder with the corresponding serial number, such as the chr1 folder, and compress all chr1 files at runtime using the command {hadoop jar}+ [jar package Path]+ [chr1](your sequence number) .

The file name is string processed in the code, so if you do not use the 1000Project data set, you need to change the string processing rules for the file name.

If you need to change the path, please open the file compress/App.class, and change this parameters.
You may need to set the input fragment size in the "mapreduce.input.fileinputformat.split.minsize" part of App.class; 
set the distributed cache path of HDFS in method addCacheFile(); 
set the input path collection in method addInputPaths(); set the output path in method outPath().

After the setting is completed, the three java files are grouped into jars (you can also set the linux local path to run directly) into the cloud platform such as the server, and use the command {hadoop jar} to run.


# SQLdecompress
Decompression code for FastDRC

Compressed code is for compression of large amounts of genetic data, while decompressed code is only for a single compressed sequence.

Make this folder a jar package and use the command {java jar} +[jar package name]+[Compressed sequence path]+[Reference sequence path]+ [Output path] command runs, and the path should be the local path.


# pom.xml

pom.xml is used to create maven projects from which to import hadoop dependencies and version information. Create a maven project through the IDE, paste this POM file into the corresponding POM of the project, and automatically import all the dependent libraries of the project.
