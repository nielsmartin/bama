####################################################################
# PART 3: SUPPORTING FUCTIONS FOR BATCH IDENTIFICATION
####################################################################

# GENERATE SEQUENTIAL TOLERATED GAP LIST WHEN A SINGLE NUMERIC VALUE IS GIVEN
# Function that creates a seq_tolerated_gap_list when a single numeric value (in seconds) is given
seq_tolerated_gap_list_generator <- function(activity_log, seq_tolerated_gap_value){
  #If a list is already passed to the function, no further processing is required
  if(is.list(seq_tolerated_gap_value)){
    return(seq_tolerated_gap_value)
  } else{
    activities <- unique(activity_log$activity)
    seq_tolerated_gap_list <- list()
    seq_tolerated_gap_list <- c(seq_tolerated_gap_list, rep(seq_tolerated_gap_value, length(activities)))
    names(seq_tolerated_gap_list) <- activities
    return(seq_tolerated_gap_list)
  }
}

# CHECK IF RESOURCE WAS ACTIVE DURING A PARTICULAR TIME FRAME
# Resource was active when: (i) a start event is recorded in this time frame not including boundaries,
# (ii) a complete event is recorded in this time frame not including boundaries or (iii) an instance
# is recorded for which start and complete exactly match the time frame boundaries AND the time between
# start_tf and end_tf is not zero
resource_active <- function(activity_log, res, start_tf, end_tf, act){
  # Short-circuiting not appropriate here!
  # outcome <- ifelse(TRUE %in% (activity_log$activity != act & activity_log$resource == res &
  #                                 ((activity_log$start > start_tf && activity_log$start < end_tf) ||
  #                                   (activity_log$complete >  start_tf && activity_log$complete < end_tf) ||
  #                                   (activity_log$start == start_tf && activity_log$complete == end_tf))), TRUE, FALSE)

  outcome <- ifelse(TRUE %in% (activity_log$activity != act & activity_log$resource == res &
                                 ((activity_log$start > start_tf & activity_log$start < end_tf) |
                                    (activity_log$complete >  start_tf & activity_log$complete < end_tf) |
                                    (activity_log$start == start_tf & activity_log$complete == end_tf & start_tf != end_tf))),
                                    TRUE, FALSE)

  return(outcome)
}


####################################################################
# PART 4: BOWI - BATCH IDENTIFICATION FUNCTIONS (RESOURCE-ACTIVITY LEVEL)
####################################################################

# BASIC BATCH DETECTION - ADD BATCHING INFORMATION AT RESOURCE-ACTIVITY LEVEL TO ACTIVITY LOG
# Detection of three types of batching behavior in the event log
# Note: seq_tolerated_gap is specified in seconds
# Parameters:
# - activity_log: the activity log
# - seq_tolerated_gap_list: list containing the tolerated time gap for each activity
# - timestamp_format: format in which the timestamps are specified
# - numeric_timestamps: boolean indicating whether timestamps are expressed numerically (instead of in POSIXct format)
# - log_and_model_based: boolean indicating whether a notion of the process model is present (i.e. whether arrival times are contained in the activity log)
add_batching_information <- function(activity_log, seq_tolerated_gap_list,  timestamp_format = "yyyy-mm-dd hh:mm:ss", numeric_timestamps = TRUE, log_and_model_based){

  # Convert timestamp format if required
  if(numeric_timestamps == FALSE){
    if(!lubridate::is.POSIXct(activity_log$arrival)){
      suppressWarnings(activity_log <- convert_activity_log_timestamp_format(activity_log, "arrival", timestamp_format = timestamp_format))
      if(any(is.na(activity_log$arrival))){
        warning("Column with arrival times contains NA values.")
      }
    }
    if(!lubridate::is.POSIXct(activity_log$start)){
      suppressWarnings(activity_log <- convert_activity_log_timestamp_format(activity_log, "start", timestamp_format = timestamp_format))
      if(any(is.na(activity_log$start))){
        warning("Column with start times contains NA values.")
      }
    }
    if(!lubridate::is.POSIXct(activity_log$complete)){
      suppressWarnings(activity_log <- convert_activity_log_timestamp_format(activity_log, "complete", timestamp_format = timestamp_format))
      if(any(is.na(activity_log$complete))){
        warning("Column with complete times contains NA values.")
      }
    }
  }

  # Sort activity log based on resource-activity combinations
  activity_log <- activity_log %>% arrange(activity, resource, start, complete)

  # Add numeric timestamps for calculation purposes
  activity_log$arrival_num <- as.numeric(activity_log$arrival)
  activity_log$start_num <- as.numeric(activity_log$start)
  activity_log$complete_num <- as.numeric(activity_log$complete)

  # When arrival_num is not available, set arrival_num to zero
  index <- is.na(activity_log$arrival)
  activity_log$arrival_num[index] <- 0
  remove(index)

  # Add batch number and batch type columns in activity log
  activity_log$batch_number <- NA
  activity_log$batch_type <- NA

  # Initialize values
  batch_counter <- 1
  activity_log$batch_number[1] <- batch_counter
  start_first_batched_case <- activity_log$start_num[1]
  if(log_and_model_based == TRUE){
    arrival_first_batched_case <- activity_log$arrival_num[1]
  }
  seq_tolerated_gap <- seq_tolerated_gap_list[[activity_log$activity[1]]]

  # Go through activity log and add batch numbers
  pb <- txtProgressBar(min = 2, max = nrow(activity_log), style = 3)
  for(i in 2:nrow(activity_log)){

    # Check is two subsequent instances in the activity log belong to the same resource-activity combination
    if(activity_log$activity[i] == activity_log$activity[i-1] &
       activity_log$resource[i] == activity_log$resource[i-1]){

      # TYPE 1: SIMULTANEOUS BATCH PROCESSING
      if((activity_log$start_num[i] == activity_log$start_num[i-1]) &&
         (activity_log$complete_num[i] == activity_log$complete_num[i-1]) &&
         (is.na(activity_log$batch_type[i-1]) | activity_log$batch_type[i-1] == "simultaneous")){

        activity_log$batch_number[i] <- batch_counter
        activity_log$batch_type[i] <- "simultaneous"

        if(is.na(activity_log$batch_type[i-1])){
          activity_log$batch_type[i-1] <- "simultaneous"
        }

      } else if((activity_log$start_num[i] >= activity_log$start_num[i-1]) &&
                (activity_log$start_num[i] < activity_log$complete_num[i-1]) &&
                (activity_log$complete_num[i] != activity_log$complete_num[i-1]) &&
                (is.na(activity_log$batch_type[i-1]) | activity_log$batch_type[i-1] == "concurrent")){

        # TYPE 2: CONCURRENT BATCH PROCESSING

        activity_log$batch_number[i] <- batch_counter
        activity_log$batch_type[i] <- "concurrent"

        if(is.na(activity_log$batch_type[i-1])){
          activity_log$batch_type[i-1] <- "concurrent"
        }

      } else if((activity_log$start_num[i] >= activity_log$complete_num[i-1]) &&
                (activity_log$start_num[i] <= activity_log$complete_num[i-1] + seq_tolerated_gap) &&
                (is.na(activity_log$batch_type[i-1]) | activity_log$batch_type[i-1] == "sequential") &&
                (resource_active(activity_log, activity_log$resource[i], activity_log$complete_num[i-1],
                                 activity_log$start_num[i], activity_log$activity[i]) == FALSE)){

        # TYPE 3: SEQUENTIAL BATCH PROCESSING

        # Check additional constraints only when arrival events are present
        if(log_and_model_based == FALSE || is.na(activity_log$arrival_num[i])){
          activity_log$batch_number[i] <- batch_counter
          activity_log$batch_type[i] <- "sequential"

          if(is.na(activity_log$batch_type[i-1])){
            activity_log$batch_type[i-1] <- "sequential"
          }

        } else{

          #if((activity_log$arrival_num[i] <= start_first_batched_case) &&
          #   (!(activity_log$arrival_num[i] == arrival_first_batched_case & arrival_first_batched_case == start_first_batched_case))){
          if(activity_log$arrival_num[i] <= start_first_batched_case){
            activity_log$batch_number[i] <- batch_counter
            activity_log$batch_type[i] <- "sequential"

            if(is.na(activity_log$batch_type[i-1])){
              activity_log$batch_type[i-1] <- "sequential"
            }

          } else{
            # Increase batch counter and add this new value to the current instance
            batch_counter <- batch_counter + 1
            activity_log$batch_number[i] <- batch_counter
            # Save the arrival and start time of the first case in a batch
            start_first_batched_case <- activity_log$start_num[i]
            arrival_first_batched_case <- activity_log$arrival_num[i]
          }
        }

      } else{
        # NO BATCHING RELATIONSHIP BETWEEN THESE TWO SUBSEQUENT INSTANCES

        # Increase batch counter and add this new value to the current instance
        batch_counter <- batch_counter + 1
        activity_log$batch_number[i] <- batch_counter
        # Save the arrival and start time of the first case in a batch
        start_first_batched_case <- activity_log$start_num[i]
        if(log_and_model_based == TRUE){
          arrival_first_batched_case <- activity_log$arrival_num[i]
        }
      }

    } else{
      # Subsequent instances in the activity log do not belong to the same resource-activity combination

      # Increase batch counter and add this new value to the current instance
      batch_counter <- batch_counter + 1
      activity_log$batch_number[i] <- batch_counter
      # Save the arrival and start time of the first case in a batch
      start_first_batched_case <- activity_log$start_num[i]
      if(log_and_model_based == TRUE){
        arrival_first_batched_case <- activity_log$arrival_num[i]
      }
      # Change value of the sequential tolerated gap
      seq_tolerated_gap <- seq_tolerated_gap_list[[activity_log$activity[i]]]
    }
    setTxtProgressBar(pb, i)
  }
  close(pb)

  # Remove numeric timestamp columns
  activity_log$arrival_num <- NULL
  activity_log$start_num <- NULL
  activity_log$complete_num <- NULL

  return(activity_log)
}


####################################################################
# PART 5: BOWI EXTENSION - DETECTING BATCH SUBPROCESSES
####################################################################

# DETECT TASK-BASED BATCH SUBPROCESSES
# Detection of task-based batch subprocesses using BOWI batching information as a starting point. Detected batch types:
# - parallel batching (simultaneous batching)
# - task-based sequential batching
# - task-based concurrent batching
# Note: seq_tolerated_gap is specified in seconds
# Parameters:
# - activity_log: the activity log (subset) that needs to be investigated
# - full_activity_log: the full activity log (required to determine whether activities immediately follow each other)
detect_task_based_batch_subprocesses <- function(activity_log, full_activity_log){

  # # Initialize batch_subprocess column
  # activity_log$batch_subprocess <- NA

  # Initialize batch_subprocess_id
  batch_subprocess_id <- 1

  # Add batch composition information
  activity_log <- activity_log %>%
    group_by(batch_number) %>%
    arrange(case_id, start_num) %>%
    mutate(batch_composition = paste(case_id, collapse = " - "), n_cases_in_batch = n())

  # Check if there are multiple batches (consisting of multiple cases) having the same composition
  candidates <- as.data.frame(activity_log %>% filter(n_cases_in_batch > 1) %>% group_by(batch_composition) %>% summarize(n_act = n_distinct(activity)) %>% filter(n_act > 1))$batch_composition

  if(length(candidates) > 0){

    # Check if the activities immediately follow each other
    pb <- txtProgressBar(min = 2, max = length(candidates), style = 3)
    for(i in 1:length(candidates)){
      rows_to_check <- activity_log %>% filter(batch_composition == candidates[i])
      activities_to_check <- as.data.frame(rows_to_check %>% distinct(activity))$activity
      subprocesses <- list()
      subprocess_under_construction <- c()

      for(j in 1:(length(activities_to_check) - 1)){

        rows_to_check_subset <- rows_to_check %>% filter(activity == activities_to_check[j] || activity == activities_to_check[j+1]) %>% arrange(case_id, start_num)

        error_detected <- FALSE
        k <- 1

        while(error_detected == FALSE && k < (nrow(rows_to_check_subset) - 1)){
          check <- activity_started(full_activity_log, rows_to_check_subset$case_id[k], c(activities_to_check[j], activities_to_check[j+1]), rows_to_check_subset$start_num[k], rows_to_check_subset$start_num[k+1])

          if(check == TRUE){
            error_detected <- TRUE
          } else{  # continue to next check
            k <- k + 2
          }
        }

        if(error_detected == FALSE){
          # No error is detected > tasks are part of subprocess
          subprocess_under_construction <- c(subprocess_under_construction, activities_to_check[j], activities_to_check[j+1])
        } else{
          # Error is detected
          if(length(subprocess_under_construction) > 0){
            # If a subprocess is already detected, save this subprocess
            subprocesses[[length(subprocesses) + 1]] <- subprocess_under_construction
            subprocess_under_construction <- c()
          }
        }
      }

      if(length(subprocess_under_construction) > 0){
        # If subprocess is detected at the maximal value of j, ensure that it is still included
        subprocesses[[length(subprocesses) + 1]] <- subprocess_under_construction
        subprocess_under_construction <- c()
      }


      # For activities forming a batch subprocess, mark the appropriate rows in the activity log with the same batch_subprocess_id

      if(length(subprocesses) > 0){ # if one or more subprocesses are detected

        for(l in 1:length(subprocesses)){

          subset_to_mark <- as.data.frame(rows_to_check %>% filter(activity %in% subprocesses[[l]]))

          # Add batch_subprocess_id to the appropriate rows in the activity log
          index <- activity_log$row_id %in% subset_to_mark$row_id
          activity_log$batch_subprocess_number[index] <- batch_subprocess_id
          activity_log$batch_subprocess_type[index] <- "task-based"
          remove(index)

          # Increment batch_subprocess_id
          batch_subprocess_id <- batch_subprocess_id + 1

        }
      }

      setTxtProgressBar(pb, i)
    }
    close(pb)
  }

  # Remove helper columns
  activity_log$batch_composition <- NULL

  return(activity_log)
}


# CHECK IF ACTIVITY HAS STARTED IN A TIME INTERVAL FOR A PARTICULAR CASE
# Check whether, for a particular case, an activity has started in a time interval (interval_end not included)
# Parameters:
# - activity_log: the activity log
# - case: identifier of the case
# - activities_to_exclude: activities that do not have to be taken into account (as they are the potential batch activities)
# - interval_start: start time of the interval (not included in the check)
# - interval_end: end time of the interval (not included in the check)
activity_started <- function(activity_log, case, activities_to_exclude, interval_start, interval_end){

  started <- as.data.frame(activity_log %>% filter(case_id == case, !(activity %in% activities_to_exclude), start_num > interval_start, start_num < interval_end))

  if(nrow(started) > 0){
    return(TRUE) # activity/activities have started in the indicated time interval
  } else{
    return(FALSE) # no activity has started in the indicated time interval
  }
}


# IDENTIFY FREQUENT SEQUENCES
# Function which identifies frequent sequences in traces using the SPADE algoritm (Sequential PAttern Discovery using Equivalence classes)
# Parameters:
# - activity_log: the activity log
# - min_support: the minimal support level that needs to be obtained in order to consider a sequence as frequent
identify_frequent_sequences <- function(activity_log, min_support){

  # Order activity log
  activity_log <- activity_log %>%
    arrange(case_id, start, complete)

  # Add row numbers for sequence
  activity_log$seq_row_number <- seq(1,nrow(activity_log))

  # Create transactions
  activity_log_activities <- data.frame(item = activity_log$activity)
  activity_log_trans <- as(activity_log_activities, "transactions")
  transactionInfo(activity_log_trans)$sequenceID <- activity_log$case_id
  transactionInfo(activity_log_trans)$eventID <- activity_log$seq_row_number

  # Determine frequent sequences
  frequent_sequences <- arulesSequences::cspade(activity_log_trans, parameter = list(support=min_support, maxgap = 1)) #maxgap = 1 ensures that only activities that immediately follow each other are taken into account
  frequent_sequences <- as(frequent_sequences, "data.frame")
  frequent_sequences <- frequent_sequences %>% arrange(desc(support))

  # Create list with frequent patterns
  frequent_sequences$sequence <- gsub("item=", "", frequent_sequences$sequence)
  frequent_sequences$sequence <- gsub ("[^[:alnum:][:blank:],]", "", frequent_sequences$sequence)
  frequent_sequences$sequence_list <- strsplit(frequent_sequences$sequence, ",")

  # Filter out frequent patterns consisting of only 1 activity
  frequent_sequences$n_act <- unlist(rapply(frequent_sequences$sequence_list, length, how="list"))
  frequent_sequences <- frequent_sequences %>% filter(n_act > 1)

  return(frequent_sequences)
}


# ENUMERATE SUBSEQUENCES
# Function which enumerates all subsequences observed within a trace
# Parameters:
# - activity_log: the activity log
# - min_trace_freq: specifies the number of times that a trace needs to occur in the event log before it should be considered
enumerate_subsequences <- function(activity_log, min_trace_freq = 0){

  # Retrieve traces together with trace frequencies
  traces <- activity_log %>% group_by(case_id) %>% summarize(trace = list(activity), trace_string = paste(activity, collapse = "-")) %>% group_by(trace_string) %>% summarize(trace_freq = n(), trace = trace[1], trace_length = length(trace[[1]]))

  # Remove infrequent traces
  if(min_trace_freq > 0){
    traces <- traces %>% filter(trace_freq >= min_trace_freq)
  }

  # Initialize combinations dataframe
  subsequences <- list()

  # Retrieve combinations for each trace
  print("Enumerating subsequences...")

  #pb <- txtProgressBar(min = 1, max = nrow(traces), style = 3)
  for(i in 1:nrow(traces)){
    trace <- traces$trace[[i]]

    for(k in 1:(length(trace)-1)){
      # For each length of combinations
      l <- 1
      while((l+k) <= length(trace)){
        subsequences <- c(subsequences, list(trace[l:(l+k)]))
        l <- l + 1
      }
    }
    #setTxtProgressBar(pb, i)
  }
  #close(pb)

  # Select unique values
  subsequences <- unique(subsequences)

  return(subsequences)

}

# CHECK CASE-BASED BATCHING SUBPROCESSES - ARRIVAL TIME CHECK IN STEP 4
# Function which whether a particular set of activities forms a case-based batching subprocess
# Parameters:
# - activity_log: the activity log
# - activity_set: ordered set of activities for which time relation needs to be checked
# - within_case_seq_tolerated_gap: tolerated time gap to detect sequential batching between activities within a particular case
# - between_cases_seq_tolerated_gap: tolerated time gap to detect sequential batching between (aggregated) activities over several cases
# - batch_subprocess_number_start: first batchprocess identification number (default value of 1)
check_case_batching_relation <- function(activity_log, activity_set, within_case_seq_tolerated_gap = 0,
                                         between_cases_seq_tolerated_gap = 0, batch_subprocess_number_start = 1){

  # Ensure that appropriate input parameters are passed
  if(length(activity_set) == 1){
    stop("Activity set consisting of only a single activity is passed to helper function check_case_batching_relation.")
  } #else if(!(time_relation %in% c("sequential","concurrent"))){
  #stop("Parameter time_relation should be either sequential or concurrent.")
  #}

  # Filter the activities in activity_set
  activity_log_subset <- activity_log %>% filter(activity %in% activity_set) %>% arrange(resource, case_id, start, complete)

  # STEP 1: Remove cases for which not all activities in activity_set are executed or are not performed by the same resource
  cases_to_maintain <- (activity_log_subset %>% group_by(case_id) %>% summarize(n_act = length(unique(activity)), n_res = length(unique(resource))) %>%
                          filter(n_act == length(activity_set), n_res == 1))$case_id
  activity_log_subset <- activity_log_subset %>% filter(case_id %in% cases_to_maintain)

  if(length(cases_to_maintain) > 1){

    # STEP 2: Check if sequential/concurrent relationship exists between activities for a particular case

    # Add batch number and batch type columns in activity_log_subset
    activity_log_subset$case_batch_number <- NA
    activity_log_subset$case_batch_type <- NA

    # Initialize values
    batch_counter <- 1

    # Go through activity_log_subset and add batch numbers
    for(i in 2:nrow(activity_log_subset)){

      # Check is two subsequent instances in the activity log belong to the same case
      if(activity_log_subset$case_id[i] == activity_log_subset$case_id[i-1]){

        if((activity_log_subset$start_num[i] >= activity_log_subset$start_num[i-1]) &&
           (activity_log_subset$start_num[i] < activity_log_subset$complete_num[i-1]) &&
           (activity_log_subset$complete_num[i] != activity_log_subset$complete_num[i-1]) &&
           (is.na(activity_log_subset$case_batch_type[i-1]) | activity_log_subset$case_batch_type[i-1] == "concurrent")){

          # TYPE 1: CONCURRENT BATCH PROCESSING

          activity_log_subset$case_batch_number[i] <- batch_counter
          activity_log_subset$case_batch_type[i] <- "concurrent"

          if(is.na(activity_log_subset$case_batch_number[i-1])){
            activity_log_subset$case_batch_number[i-1] <- batch_counter
          }

          if(is.na(activity_log_subset$case_batch_type[i-1])){
            activity_log_subset$case_batch_type[i-1] <- "concurrent"
          }

        } else if((activity_log_subset$start_num[i] >= activity_log_subset$complete_num[i-1]) &&
                  (activity_log_subset$start_num[i] <= activity_log_subset$complete_num[i-1] + within_case_seq_tolerated_gap) &&
                  (is.na(activity_log_subset$case_batch_type[i-1]) | activity_log_subset$case_batch_type[i-1] == "sequential") &&
                  (resource_active(activity_log, activity_log_subset$resource[i], activity_log_subset$complete_num[i-1],
                                   activity_log_subset$start_num[i], activity_log$activity[i]) == FALSE)){

          # } else if((activity_log_subset$start_num[i] >= activity_log_subset$complete_num[i-1]) &&
          #           (activity_log_subset$start_num[i] <= activity_log_subset$complete_num[i-1] + within_case_seq_tolerated_gap) &&
          #           (is.na(activity_log_subset$case_batch_type[i-1]) | activity_log_subset$case_batch_type[i-1] == "sequential")){
          #
          # TYPE 2: SEQUENTIAL BATCH PROCESSING

          activity_log_subset$case_batch_number[i] <- batch_counter
          activity_log_subset$case_batch_type[i] <- "sequential"

          if(is.na(activity_log_subset$case_batch_number[i-1])){
            activity_log_subset$case_batch_number[i-1] <- batch_counter
          }

          if(is.na(activity_log_subset$case_batch_type[i-1])){
            activity_log_subset$case_batch_type[i-1] <- "sequential"
          }

        } else{ # Subsequent lines are not in a sequential or concurrent relation
          # Increment batch counter
          batch_counter <- batch_counter + 1
        }
      } else{ # Subsequent lines relate to different cases
        # Increment batch counter
        batch_counter <- batch_counter + 1
      }
    }

    # Remove cases for which sequential/concurrent relationship between activities is absent
    cases_to_remove <- (activity_log_subset %>% filter(is.na(case_batch_type)))$case_id
    activity_log_subset <- activity_log_subset %>% filter(!(case_id %in% cases_to_remove))

    # For remaining cases, check whether case batch type is consistent or hybrid
    case_batch_type_correction <- (activity_log_subset %>% group_by(case_id) %>% summarize(n_case_batch_types = length(unique(case_batch_type))) %>%
                                     filter(n_case_batch_types > 1))$case_id
    if(length(case_batch_type_correction) > 0){
      index <- activity_log_subset$case_id %in% case_batch_type_correction
      activity_log_subset$case_batch_type[index] <- "hybrid"
      remove(index)
    }

    # If no within-case batching is detected for a subsequence, all higher order subsequences do not have to be checked
    # (return boolean to indicate this to calling function)
    if(nrow(activity_log_subset) == 0){
      remove_higher_order_subsequences <- TRUE
    } else{
      remove_higher_order_subsequences <- FALSE
    }

    if(length(unique(activity_log_subset$case_id)) > 1){

      # STEP 3: Abstract activities in a sequential/concurrent relationship at the case level
      activity_log_subset <- as.data.frame(activity_log_subset %>%
                                             group_by(case_id) %>%
                                             summarize(resource = resource[1],
                                                       arrival_num = min(arrival_num),
                                                       start_num = min(start_num),
                                                       complete_num = max(complete_num),
                                                       case_batch_type = case_batch_type[1],
                                                       case_row_ids = paste(row_id, collapse = "-")) %>%
                                             arrange(resource, start_num,complete_num))

      # STEP 4: Check if sequential/concurrent relationship exists across cases

      # Add batch number and batch type columns in activity_log_subset
      activity_log_subset$batch_number <- NA
      activity_log_subset$batch_type <- NA

      # Initialize values
      batch_counter <- 1
      start_first_batched_case <- activity_log_subset$start_num[1]

      # Go through activity_log_subset and add batch numbers
      for(i in 2:nrow(activity_log_subset)){

        # Check is two subsequent rows belong to the same resource
        if(activity_log_subset$resource[i] == activity_log_subset$resource[i-1]){

          if((activity_log_subset$start_num[i] >= activity_log_subset$start_num[i-1]) &&
             (activity_log_subset$start_num[i] < activity_log_subset$complete_num[i-1]) &&
             (activity_log_subset$complete_num[i] != activity_log_subset$complete_num[i-1]) &&
             (is.na(activity_log_subset$case_batch_type[i-1]) | activity_log_subset$case_batch_type[i-1] == "concurrent")){

            # TYPE 1: CONCURRENT BATCH PROCESSING

            activity_log_subset$batch_number[i] <- batch_counter
            activity_log_subset$batch_type[i] <- "concurrent"

            if(is.na(activity_log_subset$batch_number[i-1])){
              activity_log_subset$batch_number[i-1] <- batch_counter
            }

            if(is.na(activity_log_subset$batch_type[i-1])){
              activity_log_subset$batch_type[i-1] <- "concurrent"
            }

          } else if((activity_log_subset$start_num[i] >= activity_log_subset$complete_num[i-1]) &&
                    (activity_log_subset$start_num[i] <= activity_log_subset$complete_num[i-1] + between_cases_seq_tolerated_gap) &&
                    (is.na(activity_log_subset$batch_type[i-1]) | activity_log_subset$batch_type[i-1] == "sequential") &&
                    (resource_active(activity_log, activity_log_subset$resource[i], activity_log_subset$complete_num[i-1],
                                     activity_log_subset$start_num[i], activity_log$activity[i]) == FALSE)){

            # TYPE 2: SEQUENTIAL BATCH PROCESSING

            # Check additional constraint on arrival times only when arrival events are present
            if(is.na(activity_log_subset$arrival_num[i])){
              activity_log_subset$batch_number[i] <- batch_counter
              activity_log_subset$batch_type[i] <- "sequential"

              if(is.na(activity_log_subset$batch_number[i-1])){
                activity_log_subset$batch_number[i-1] <- batch_counter
              }

              if(is.na(activity_log_subset$batch_type[i-1])){
                activity_log_subset$batch_type[i-1] <- "sequential"
              }
            } else{
              # Check additional constraint based on arrival times
              if(activity_log_subset$arrival_num[i] <= start_first_batched_case){
                activity_log_subset$batch_number[i] <- batch_counter
                activity_log_subset$batch_type[i] <- "sequential"

                if(is.na(activity_log_subset$batch_number[i-1])){
                  activity_log_subset$batch_number[i-1] <- batch_counter
                }

                if(is.na(activity_log_subset$batch_type[i-1])){
                  activity_log_subset$batch_type[i-1] <- "sequential"
                }
              } else{ # Subsequent lines are not in a sequential  relation
                # Increment batch counter
                batch_counter <- batch_counter + 1
                start_first_batched_case <- activity_log_subset$start_num[i]
              }
            }

          } else{ # Subsequent lines are not in a sequential or concurrent relation
            # Increment batch counter
            batch_counter <- batch_counter + 1
            start_first_batched_case <- activity_log_subset$start_num[i]
          }
        } else{ # Subsequent lines relate to different cases
          # Increment batch counter
          batch_counter <- batch_counter + 1
          start_first_batched_case <- activity_log_subset$start_num[i]
        }
      }

      # Remove cases which do not belong to a batch
      activity_log_subset <- activity_log_subset %>% filter(!(is.na(batch_type)))

      if(nrow(activity_log_subset) > 0){

        activity_log_subset <- activity_log_subset %>% group_by(batch_number) %>%
          summarize(case_ids = paste(case_id, collapse = "-"),
                    row_ids = paste(case_row_ids, collapse = "-"),
                    batch_subprocess_type = paste(case_batch_type[1], "case-based")) #case_batch_type is currently considered dominant


        # Add batch subprocess number
        activity_log_subset$batch_subprocess_number <- seq(batch_subprocess_number_start, batch_subprocess_number_start + nrow(activity_log_subset) - 1)

      }
    }
  } else{
    remove_higher_order_subsequences <- TRUE
  }

  if(("case_id" %in% colnames(activity_log_subset)) && (length(unique(activity_log_subset$case_id)) == 1)){
    activity_log_subset <- activity_log_subset[0,] # empty activity_log_subset as case-based batching behavior is only found for a single case
  }

  output <- list(log = activity_log_subset, remove_higher_order_subsequences = remove_higher_order_subsequences)

  return(output)
}

