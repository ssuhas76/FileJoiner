# FileJoiner
git clone https://github.com/ssuhas76/FileJoiner.git
Open the project in eclipse and create a Jar.
to run in hadoop try the below command
copy the jar and the emp-file-join into VM and run the below command
yarn jar FileJoiner-adv.jar com.dataflair.join.basic.FileJoinDriver /emp-file-join/emp-name /emp-file-join/emp-dept /emp-file-join/emp-data-out
check the contents of /emp-file-join/emp-data-out
hdfs dfs -cat /emp-file-join/emp-data-out/part*
