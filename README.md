# LSEG_Matching_Engine
Configuration Used:
System: Windows
IDE: IntelliJ IDE
Spark: 3.3.1
Scala: 2.13.10


Approach followed:
1) Process Reads the Input file and split it into two Data Frames by TYPES [SELL/ORDER]
2) Splitted data frames are joined to check if we have any matching orders in the current Iteration
3) If we have found any matches, matching ids will be picked from joined DF to shrink Huge Data and to process things Faster
4) Picked Matching Ids are used to select from initial splitted [Buy/Sell DF's] and ranked based on requirement [Buy-ASC,SELL-DESC] using WINODOW Partitioning
5) Final orders will be matched through quantity as we already identified which order to process first based on rank
6) Order Placed Id's are removed from the input order book which can be utilised for next run if it is streaming


Test Cases Handled:
1) When oders Matched, we match and store the details in updatedOrderBook
2) If there are no matches found in current run, process identifies once it joins between sell and buy dataframes and doesn't proceed further
3) For the Same quantity if we have more than single quote,
      a) for buy order, min-price will be considered
      b) for sell order, max-price will be considered
4) If we have more than one buy order with same price, the buy order which placed first will be considered
5) If we have more than one sell order with same price, the sell order which placed first will be considered
6) If the orders are matched, corresponding id's will be removed from the orderBook

Sample output from local run for given input:
   +--------+----------------+-----------------+-------------+-----+
|order_id|matched_order_id|order_placed_time|sell_quantity|price|
+--------+----------------+-----------------+-------------+-----+
|       5|               1|       1623239774|           72| 1550|
|      12|              10|       1623239781|           89| 7596|
+--------+----------------+-----------------+-------------+-----+
