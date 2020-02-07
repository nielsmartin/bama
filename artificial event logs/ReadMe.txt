These artificial event logs form the basis for the evaluation of the Information Systems paper on the algorithm.

To detect batching behavior in the artificial event logs, the following steps are required:
- Remove process instances which are included in the last batch as this batch might be incomplete
- Create a list containing time tolerances within the context of sequential batching detection (using the helper function seq_tolerated_gap_list_generator)
- Create a list of subsequences for which sequential/concurrent case-based batching needs to be checked (using the helper function enumerate_subsequences for enumeration, or identify_frequent_sequences for sequence mining)
- Use function detect_batching_behavior to detect batching behavior with the appropriate parameter values. More details on the parameters can be found in the paper or in the function documentation.

Further information regarding the use of the artificial event logs can be obtained by sending an e-mail to niels.martin@uhasselt.be