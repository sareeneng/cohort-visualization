# cohort-visualization
This project has two main parts to it

1) Creating a structure to quickly relate SQL-style datafiles to each other, using "graph database"-like structure
2) Using this structure to visualize various aggregations of variables across files

## 1) Relating SQL databases to each other
The file db_structure.py uses a rules system to take CSV files located in a directory within the **"datasets"** directory and find 
relationships between the files.

sample_1 is configured for testing. This can be loaded using the following code:

```
from db_structure import DB

sample_1_db = DB('sample1')
```

sample_1 already has a **.config** file located in its folder that loads the relationship information.

The relationship system looks at all files and determines which files have "many-to-one" and "one-to-one" relationships.
In this manner, a table that is related through another table through multiple foreign-keys and other intermediary tables can be
joined together without forcing the user to determine the order to join the tables.

I refer to two tables with a many-to-one relationship as having a parent-child relationship (the many table is the parent), and 
tables with one-to-one relationship are siblings. In a string of joins, the parent table must always be the left table compared to the
child.

When the DB class loads a directory without a **.config** file, it prompts the users for questions to determine how to relate
tables to each other. The following steps are followed:

1) Identify which column headers are common across files. Prompt the user for identifying which of these common column headers are
used to join tables together whenever they are found in a table. e.g. customerNumber = 3 in one table must mean the same thing in
all files where customerNumber is found.

2) Prompt the user to identify other foreign key links where the column headers are different (e.g. customerNumber in one file 
vs customerNum in another)

3) Using these links, identify which tables are related in a many-to-one or one-to-one fashion. Create parent-child or sibling-sibling
linkes between them.

To demonstrate, looking at sample_1, we can examine three of the files

###### orders.csv

|orderNumber|orderDate|Status|customerNum|
|-----------|---------|------|-----------|
|1|1/1/2001|Shipped|1|
|2|5/2/2002|Pending|1|
|3|3/4/2005|Done|1|
|4|1/4/2006|Done|2|

###### orderdetails.csv
|orderNumber|productCode|quantity|
|-----------|-----------|--------|
|1|1|3|
|2|3|7|
|3|3|1|

###### customers.csv
|customerNumber|LastName|FirstName|salesRepEmployeeNum|
|-------------|---------|---------|-------------------|
|1|Bob|Billy|1|
|2|Jenkins|Sally|1|
|3|Hobbes|Calvin|1|
|4|Jacob|John|2|

**orders** has a one-to-one relationship with **orderdetails** based on column ***orderNumber***. **orders** has a many-to-one
relationship with **customers** based on ***customerNum (orders)*** --> ***customerNumber (customers)***.

If the user wanted to identify the name of the customer associated with an orderdetail, they would need to join **orderdetails** -->
**orders** --> **customers** in that order. They would not be able to start with the **orders** table as the left-most table because
there is no path to get to customers from there.

If the user wanted to identify the opposite relationship (find orderdetail based on customer), it would not be possible. This makes
sense intuitively since customers can have many orders. The DB class would not allow this type of query.

While this example seems trivial, the difficulty associated with this problem becomes more apparent with databases normalized over
many tables. Finding the optimal path from one table to another requires the users to know these relationships, and can lead to
trial-and-error and incomplete capture of all data requested.

The DB structure can be used to create a graphical representation of the relationships between tables using the following code:

```
import networkx
import matplotlib.pyplot as plt

G = nx.DiGraph()
G.add_nodes_from(sample_1_db.tables.keys())
for current_table_name, current_table in sample_1_db.tables.items():
    for sibling in current_table.get_sibling_names():
        G.add_edge(current_table_name, sibling)
        G.add_edge(sibling, current_table_name)
    for child in current_table.get_children_names():
        G.add_edge(current_table_name, child)
plt.figure(figsize=(10,10))

# draw with networkx built-ins
nx.draw_networkx(G, node_shape="None", width=0.2)

#### OR

# draw using pygraphviz (does not need to be separately imported but must be installed using pip)
A = nx.nx_agraph.to_agraph(G)
H = nx.nx_agraph.from_agraph(A)
nx.draw_spring(H, node_shape="None", with_labels=True, width=0.2)
```

In this graphical representation, a single-headed arrow represents a parent-child link (parent points to child), and a double-headed
arrow represents a sibling-sibling link.

sample_2 is a more abstract sample set that is used to show more difficult pathfinding. It is represented as such:

![sample_2 schema](https://imgur.com/a/M9OgQB2)

A has a many-to-one relationship with C/D, and a one-to-one relationship with B
B has a many-to-one relationship with E, and a one-to-one relationship with A
C has a one-to-one relationship with D, and a many-to-one relationship with F
D has a one-to-one relationship with C
E has a many-to-one relationship with F

If you wanted to correlate a variable in A to a variable in F, it would be difficult to find the optimal path without arduous 
examination of how each table relates to each other, and even then there are multiple joining pathways that you could take. Finding
not only a possible pathway but really the optimal pathway is complex.

The DB class sets up these relationships automatically so that pathfinding can be accomplished.

```
from db_structure import DB

db_sample_2 = DB('sample2')
table_start = DB.tables['A']
table_end = DB.tables['F']

paths = DB.find_paths_between_tables(table_start, table_end)
## paths result: [[A, D, C, F], [A, C, F], [A, B, E, F]]
```

To get a column from table F into A, there are three paths you can take:
A JOIN D JOIN C JOIN F
A JOIN C JOIN F
A JOIN B JOIN E JOIN F

Then you can get the resultant dataframes from these three options using:

```
DB.get_joined_df_options_from_paths(paths)
## result: array of three dataframes, with length of 4, 6, and 5 rows
```

Any of these dataframes are valid solutions to the problem at hand, but clearly one is better (the largest one). So an alternative is 
to use: 

```
DB.get_biggest_joined_df_option_from_paths(paths)
```
