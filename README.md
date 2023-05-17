# expressions_limit_results_returned

- This code will add observation data points to a List. 
- The List contains n maps, where each map holds a list of datapoints.
- The read method will either return all the data or the data up to and including a dynamically stored index position of the said List.
- The dynamically stored index position is defined by a constant which 
- Schema
  - observations: [{ obs: [a,b,..m] }], totalObs: N, indexOfReqObs: I
