# tikvReader

#### Description
The TiKV reader is a software program that allows users to read data from TiKV without the need for a password. Similar to reading data from Kafka or Redis, this program uses the TikvClient provided by TiDB to access the data stored in the TiKV storage architecture. TiDB stores data in a Key-Value (KV) format, where each row of data has an implicit row_id as its key. By leveraging this feature, users can retrieve data using the row_id.
#### Software Architecture
The Tikv GUI was developed by JavaFX.

#### Installation

The executable file for the application is located in the application directory.
Choose the corresponding table and select "Consume Data" to consume a specified amount of data, similar to how Kafka works. Alternatively, you can choose to consume single data entries by specifying the key, similar to how Redis works.
Click on the consumed data to view it in JSON format.
Right-click on the table name to enter edit mode, allowing you to manually write or modify data within the table.
#### Instructions

1.Enter the TiDB address and select Save to connect to the TiKV database.
2.Double-click on the database name to view the table list under the selected database.
#### Contribution

Fork this repository
Create a new branch named Feat_xxx
Submit code changes
Open a new Pull Request


#### Skills

For further assistance, you can contact me through my personal WeChat public account with the same name.

#### Optimize the user interface.:

Update the metadata retrieval method to improve the user experience.
Change the data reading method to make it faster.
Add functions for adding and modifying data.
Optimize the session connection method to increase running speed.


![tikvReader.gif](tikvReader.gif)