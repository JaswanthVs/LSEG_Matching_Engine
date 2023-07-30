# LSEG_Matching_Engine
Configuration Used:
System: Windows
IDE: Intellij IDE
Spark: 3.3.1
Scala: 2.13.10


Approach followed:
1) Splitted input data file in to BUY and SELL orders to identify a match
2) Only relevant ids which we require further will be picked from order book and proceed to next step
3) Rank buy and sell data as per the price [buy asc, sell desc]
4) Final orders will be matched through quantity as we already shrinked the data and picked qualified data for join
5) Order Placed Id's are removed from input order book


Test Cases Handled:
1) When oders Matched, we match and store the details in updatedOrderBook
2) If there are no matches found in current run, process identifies once it joins between sell and buy dataframes and doesn't proceed further
3) For the Same quantity if we have more than single quote,
      a) for buy order, min-price will be considered
      b) for sell order, max-price will be considered
4) If we have more than one buy order with same price, the buy order which placed first will be considered
5) If we have more than one sell order with same price, the sell order which placed first will be considered
6) If the orders are matched, corresponding id's will be removed from the orderBook
   
