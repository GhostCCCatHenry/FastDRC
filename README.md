# FastDRC
Distributed biological sequences Compression
The code in the compress folder is the compression code of FastDRC. In use, you need to set the input fragment size in the "mapreduce.input.fileinputformat.split.minsize" part of App.class; 
set the distributed cache path of HDFS in addCacheFile. ; 
set the input path collection in addInputPaths; set the output path in outPath.
The file name is string processed in the code, so if you do not use the 1000Project data set, you need to change the string processing rules for the file name.
After the setting is completed, the three java files are grouped into jars (you can also set the linux local path to run directly) into the cloud platform such as the server, and use the hadoop jar command to run.

The author saves the HDFS mapping path of the sequence file into the file and reads the path through file reading. 
Place each path file in a folder with the corresponding serial number, such as the chr1 folder, and compress all chr1 files at runtime using the hadoop jar jar package name chr1 command.
The path file will also be provided in the input


# SQLdecompress
Decompression code for FastDRC
Compressed code is for compression of large amounts of genetic data, while decompressed code is only for a single compressed sequence.
Make this folder a jar package and use the java jar jar package name. Input path Reference sequence path The output path command runs, and the path should be the local path.
