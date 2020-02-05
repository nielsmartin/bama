# BATCH ACTIVITY MINING ALGORITHM - FUNCTIONS
# Niels Martin, Hasselt University
# Implementation 1.14
# Last update: 12/04/2019

# IMPORT LIBRARIES
library(reshape)
library(dplyr)
library(lubridate)
library(stringr)
library(tidyr)
library(arulesSequences) # sequence mining for sequential/concurrent case-based batching

####################################################################
# PART 1: SUPPORTING DATA PREPARATION FUNCTIONS FOR EVENT LOG
####################################################################

# RENAME EVENT LOG 
rename_event_log <- function(event_log, timestamp, case_id, activity, resource, event_type, activity_instance_id = NULL, case_attributes = NULL,
                             arrival_events_recorded = FALSE, event_type_arrival = "arrival", event_type_start = "start", 
                             event_type_complete = "complete") {
  colnames(event_log)[match(timestamp, colnames(event_log))] <- "timestamp"
  colnames(event_log)[match(case_id, colnames(event_log))] <- "case_id"
  colnames(event_log)[match(activity, colnames(event_log))] <- "activity"
  colnames(event_log)[match(resource, colnames(event_log))] <- "resource"
  colnames(event_log)[match(event_type, colnames(event_log))] <- "event_type"
  
  if(!is.null(case_attributes)){
    for(i in 1:length(case_attributes)){
      colnames(event_log)[match(case_attributes[i], colnames(event_log))] <- paste("cattr#", case_attributes[i], sep = "")
    }
  }
  
  if(!is.null(activity_instance_id)){
    colnames(event_log)[match(activity_instance_id, colnames(event_log))] <- "activity_instance_id"
  }
  
  if(arrival_events_recorded == TRUE & event_type_arrival != "arrival"){
    index <- event_log$event_type == event_type_arrival
    event_log$event_type[index] <- "arrival"
    remove(index)
  }
  if(event_type_start != "start"){
    index <- event_log$event_type == event_type_start
    event_log$event_type[index] <- "start"
    remove(index)
  }
  if(event_type_complete != "complete"){
    index <- event_log$event_type == event_type_complete
    event_log$event_type[index] <- "complete"
    remove(index)
  }
  
  return(event_log)
}


# ADD ARRIVAL EVENTS TO THE EVENT LOG
add_arrival_events <- function(renamed_event_log, previous_in_trace = TRUE){
  
  if(previous_in_trace == FALSE){
    stop("Sorry, this is not supported yet.")
  }
  
  # If timestamps are given in POSIXct format, convert it to character to avoid
  # copying issues. 
  if(is.POSIXct(renamed_event_log$timestamp)){
    renamed_event_log$timestamp <- as.character(renamed_event_log$timestamp)
  }
  
  # Case arrival is equated to the last complete timestamp recorded in the case's trace
  if(previous_in_trace == TRUE){
    
    # Order event log
    renamed_event_log <- renamed_event_log %>% arrange(timestamp, activity, desc(event_type))
    
    # Add row number
    renamed_event_log$row_id <- seq(1, nrow(renamed_event_log))
    
    # Select start events
    start_events <- renamed_event_log %>% filter(event_type == "start")
    
    # For each start event, add the accompanying arrival event
    for(i in 1:nrow(start_events)){
      
      # Check whether an arrival proxy can be determined
      
      proxy_events <- as.data.frame(renamed_event_log %>%
                                      filter(case_id == start_events$case_id[i], event_type == "complete",
                                             timestamp <= start_events$timestamp[i], row_id < start_events$row_id[i]))
      if(nrow(proxy_events) > 0){
        arrival_proxy <- (proxy_events %>% 
                            summarize(arrival_proxy = max(timestamp)))$arrival_proxy[1]
        
        next_log_line <- nrow(renamed_event_log) + 1
        renamed_event_log[next_log_line,] <- start_events[i,]
        renamed_event_log$event_type[next_log_line] <- "arrival"
        renamed_event_log$timestamp[next_log_line] <- arrival_proxy
      } else{
        # If no arrival proxy can be determined, add arrival event with NA timestamp
        next_log_line <- nrow(renamed_event_log) + 1
        renamed_event_log[next_log_line,] <- start_events[i,]
        renamed_event_log$event_type[next_log_line] <- "arrival"
        renamed_event_log$timestamp[next_log_line] <- NA
      }
    }
    
    # Remove row_id
    renamed_event_log$row_id <- NULL
    
    # Order event log
    renamed_event_log <- renamed_event_log %>% arrange(timestamp)
  }
  
  return(renamed_event_log)
}


# CONVERT EVENT LOG TIMESTAMP FORMAT TO POSIXCT OBJECT
convert_timestamp_format <- function(renamed_event_log, timestamp_format = "yyyy-mm-dd hh:mm:ss"){
  
  if(timestamp_format == "yyyy-mm-dd hh:mm:ss"){
    renamed_event_log$timestamp <- ymd_hms(renamed_event_log$timestamp)
  } else if(timestamp_format == "dd-mm-yyyy hh:mm:ss"){
    renamed_event_log$timestamp <- dmy_hms(renamed_event_log$timestamp)
  } else if(timestamp_format == "dd-mm-yyyy hh:mm"){
    renamed_event_log$timestamp <- dmy_hm(renamed_event_log$timestamp)
  } else{
    stop("Timestamp format not supported. Convert timestamp to POSIXct object manually.")
  }
  
  return(renamed_event_log)
}


# ADD ACTIVITY INSTANCE ID TO EVENT LOG
add_activity_instance_id <- function(renamed_event_log, event_types = c("arrival", "start", "complete"), remove_incomplete_instances = FALSE){
  
  if("activity_instance_id" %in% colnames(renamed_event_log)){
    
    message("Activity instance identifier is already present in the event log")
    
  } else{
    
    n_event_types <- length(event_types)
    instance_id_value <- 0
    
    # order event log
    renamed_event_log <- renamed_event_log %>% arrange(case_id,activity,timestamp)
    
    renamed_event_log$activity_instance_id <- NA
    case_act_res_comb <- unique(renamed_event_log[,c('activity', 'resource', 'case_id')])
    
    for(i in 1:nrow(case_act_res_comb)){
      index <- renamed_event_log$case_id == case_act_res_comb$case_id[i] & renamed_event_log$activity == case_act_res_comb$activity[i] & renamed_event_log$resource == case_act_res_comb$resource[i]
      
      if(as.numeric(table(index)["TRUE"]) < n_event_types){
        # When not all events are recorded for a particular instance, action depends upon value of remove_incomplete_instances
        if(remove_incomplete_instances == TRUE){
          warning(paste("Instance", case_act_res_comb$case_id[i], " - activity", case_act_res_comb$activity[i], " - resource", case_act_res_comb$resource[i], "removed because not all events were recorded."))
          renamed_event_log <- renamed_event_log[-which(index == TRUE),]
        } else{
          stop(paste("Event log does not contain all events for case", case_act_res_comb$case_id[i], " - activity", case_act_res_comb$activity[i], " - resource", case_act_res_comb$resource[i]))
        }
        
      } else if(as.numeric(table(index)["TRUE"]) == n_event_types){
        
        instance_id_value <- instance_id_value + 1
        renamed_event_log$activity_instance_id[index] <- instance_id_value
        
      } else{
        instance_id_value <- instance_id_value + 1
        
        n_of_instances <- as.numeric(table(index)["TRUE"]) / n_event_types
        
        if(n_of_instances %% 1 != 0){
          stop(paste("Event log does not contain all events for case", case_act_res_comb$case_id[i], " - activity", case_act_res_comb$activity[i], " - resource", case_act_res_comb$resource[i]))
        }
        
        instance_id_range <- seq(instance_id_value, instance_id_value + n_of_instances - 1)
        
        for(j in 1:n_event_types){
          index2 <- renamed_event_log$case_id == case_act_res_comb$case_id[i] & 
            renamed_event_log$activity == case_act_res_comb$activity[i] &
            renamed_event_log$resource == case_act_res_comb$resource[i] &
            renamed_event_log$event_type == event_types[j]
          
          renamed_event_log$activity_instance_id[index2] <- instance_id_range
        }
        instance_id_value <- max(instance_id_range) 
      }
    }
    
    # Sort renamed event log
    renamed_event_log <- renamed_event_log %>%
      arrange(timestamp, case_id)
    
  }
  
  return(renamed_event_log)
}


# CONVERT EVENT LOG TO ACTIVITY LOG
convert_to_activity_log <- function(renamed_event_log){
  
  # Create activity log, where the statement structure depends on the presence of (i) activity instance id and (ii) case attributes 
  
  if("activity_instance_id" %in% names(renamed_event_log) & length(names(renamed_event_log)[grep("cattr", names(renamed_event_log))]) > 0){
    # Event log containing: activity instance id, case attributes
    activity_log <- reshape(renamed_event_log, timevar = "event_type", 
                            idvar = c("activity","case_id","resource", colnames(renamed_event_log)[grep("cattr#", colnames(renamed_event_log))],
                                      "activity_instance_id"), 
                            direction = "wide")
  } else if("activity_instance_id" %in% names(renamed_event_log)){
    # Event log containing: activity instance id
    activity_log <- reshape(renamed_event_log, timevar = "event_type", 
                            idvar = c("activity","case_id","resource", "activity_instance_id"), 
                            direction = "wide")
  } else if(length(names(renamed_event_log)[grep("cattr", names(renamed_event_log))]) > 0){
    # Event log containing: case attributes
    activity_log <- reshape(renamed_event_log, timevar = "event_type", 
                            idvar = c("activity","case_id","resource", colnames(renamed_event_log)[grep("cattr#", colnames(renamed_event_log))]), 
                            direction = "wide")
  } else{
    # Event log not containing activity instance id, batch number or case attributes
    activity_log <- reshape(renamed_event_log, timevar = "event_type", 
                            idvar = c("activity","case_id","resource"), 
                            direction = "wide")
  }
  
  colnames(activity_log)[match("timestamp.arrival", colnames(activity_log))] <- "arrival"
  colnames(activity_log)[match("timestamp.start", colnames(activity_log))] <- "start"
  colnames(activity_log)[match("timestamp.complete", colnames(activity_log))] <- "complete"
  
  
  # Filter activity instances for which either start or complete is missing
  activity_log <- as.data.frame(activity_log %>% filter(!is.na(start), !is.na(complete)))
  
  return(activity_log)
}



####################################################################
# PART 2: SUPPORTING DATA PREPARATION FUNCTIONS FOR ACTIVITY LOG
####################################################################

# RENAME ACTIVITY LOG 
rename_activity_log <- function(activity_log, timestamp, case_id, activity, resource, event_type, activity_instance_id = NULL, case_attributes = NULL,
                             arrival_events_recorded = FALSE, arrival_ts = "arrival", start_ts = "start", 
                             complete_ts = "complete") {
  colnames(activity_log)[match(timestamp, colnames(activity_log))] <- "timestamp"
  colnames(activity_log)[match(case_id, colnames(activity_log))] <- "case_id"
  colnames(activity_log)[match(activity, colnames(activity_log))] <- "activity"
  colnames(activity_log)[match(resource, colnames(activity_log))] <- "resource"
  colnames(activity_log)[match(start_ts, colnames(activity_log))] <- "start"
  colnames(activity_log)[match(complete_ts, colnames(activity_log))] <- "complete"
  
  if(!is.null(case_attributes)){
    for(i in 1:length(case_attributes)){
      colnames(activity_log)[match(case_attributes[i], colnames(activity_log))] <- paste("cattr#", case_attributes[i], sep = "")
    }
  }

  if(arrival_times_included == TRUE){
    colnames(activity_log)[match(arrival_ts, colnames(activity_log))] <- "arrival"
  }
  
  if(!is.null(activity_instance_id)){
    colnames(activity_log)[match(activity_instance_id, colnames(activity_log))] <- "activity_instance_id"
  }

  return(activity_log)
}


# ADD ARRIVAL PROXIES TO THE ACTIVITY LOG
add_arrival_proxies <- function(activity_log, previous_in_trace = TRUE){
  
  if(previous_in_trace == FALSE){
    stop("Sorry, this is not supported yet.")
  }
  
  # Create numeric versions of timestamps
  activity_log$start_num <- as.numeric(activity_log$start)
  activity_log$complete_num <- as.numeric(activity_log$complete)
  
  # If timestamps are given in POSIXct format, convert it to character to avoid
  # copying issues.
  if(is.POSIXct(activity_log$start)){
    activity_log$start <- as.character(activity_log$start)
  }
  if(is.POSIXct(activity_log$complete)){
    activity_log$complete <- as.character(activity_log$complete)
  }
  
  # Case arrival is equated to the last complete timestamp recorded in the case's trace
  if(previous_in_trace == TRUE){
    
    # Order activity log
    activity_log <- activity_log %>% arrange(case_id, start_num, activity, complete_num)
    
    # Add arrival time column
    activity_log$arrival <- NA
    
    # Add (temporary) row number column
    activity_log$row_number <- seq(1, nrow(activity_log))
    
    # For each start event, add the accompanying arrival event
    for(i in 1:nrow(activity_log)){
      
      # Check whether an arrival proxy can be determined
      
      prior_activities <- as.data.frame(activity_log %>% filter(case_id == activity_log$case_id[i], complete_num <= activity_log$start_num[i],
                                                                activity != activity_log$activity[i]))
      if(nrow(prior_activities) > 0){
        arrival_proxy <- (prior_activities %>%
                            filter(complete_num == max(complete_num)))$row_number[1]
        
        activity_log$arrival[i] <- activity_log$complete[arrival_proxy]
        
      } 
    }
    
    # Order event log
    activity_log <- activity_log %>% arrange(start)
  }
  
  # Remove added columns
  activity_log$start_num <- NULL
  activity_log$complete_num <- NULL
  activity_log$row_number <- NULL
  
  return(activity_log)
}


# CONVERT ACTIVITY LOG TIMESTAMP FORMAT TO POSIXCT OBJECT
convert_activity_log_timestamp_format <- function(activity_log, timestamp_type, timestamp_format = "yyyy-mm-dd hh:mm:ss"){
  
  if(timestamp_format == "yyyy-mm-dd hh:mm:ss"){
    activity_log[, timestamp_type] <- ymd_hms(activity_log[, timestamp_type])
  } else if(timestamp_format == "dd-mm-yyyy hh:mm:ss"){
    activity_log[, timestamp_type] <- dmy_hms(activity_log[, timestamp_type])
  } else if(timestamp_format == "dd-mm-yyyy hh:mm"){
    activity_log[, timestamp_type] <- dmy_hm(activity_log[, timestamp_type])
  } else{
    stop("Timestamp format not supported. Convert timestamp to POSIXct object manually.")
  }
  
  return(activity_log)
}


# REMOVE SELF-LOOPS FROM ACTIVITY LOG
# If the sequential execution of the same activity (by the same resource) is not considered as batch processing, 
# such self-loops need to be abstracted from in the activity log.This is implemented by replacing these
# self-loops by a single instance of the activity with (i) the start timestamp corresponding to the start timestamp of
# the first instance of the self-loop and (ii) the complete timestamp corresponding to the complete timestamp of
# the last instance of the self-loop.
remove_self_loops <- function(activity_log, seq_tolerated_gap_list){
  activity_log <- activity_log %>%
    arrange(activity, resource, case_id, start, complete)
  
  activity_log$self_loops_removed <- FALSE
  activity_log$to_delete <- FALSE
  first_case_index <- 1
  self_loop_removal <- FALSE
  
  # Combine activity instances and determine rows to delete
  for(i in 2:nrow(activity_log)){
    seq_tolerated_gap <- seq_tolerated_gap_list[[event_log$activity[i]]]
    if((activity_log$activity[i] == activity_log$activity[i-1]) && (activity_log$resource[i] == activity_log$resource[i-1]) &
       (activity_log$case_id[i] == activity_log$case_id[i-1]) &&
       (activity_log$start[i] >= activity_log$complete[i-1]) && (activity_log$start[i] <= activity_log$complete[i-1] + seq_tolerated_gap)){
      self_loop_removal <- TRUE
    } else{
      if(self_loop_removal == TRUE){
        #mark to remove self-loop
        activity_log$complete[first_case_index] <- activity_log$complete[i-1]
        activity_log$self_loops_removed[first_case_index] <- TRUE
        for(j in (first_case_index+1):(i-1)){
          activity_log$to_delete[j] <- TRUE
        }
        self_loop_removal <- FALSE
        first_case_index <- i
        remove(j)
      } else{
        first_case_index <- i
      }
    }
  }
  
  # Remove redundant rows
  activity_log <- activity_log[which(activity_log$to_delete == FALSE),]
  activity_log$to_delete <- NULL
  
  return(activity_log)
}



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
    if(!is.POSIXct(activity_log$arrival)){
      suppressWarnings(activity_log <- convert_activity_log_timestamp_format(activity_log, "arrival", timestamp_format = timestamp_format))
      if(any(is.na(activity_log$arrival))){
        warning("Column with arrival times contains NA values.")
      }
    }
    if(!is.POSIXct(activity_log$start)){
      suppressWarnings(activity_log <- convert_activity_log_timestamp_format(activity_log, "start", timestamp_format = timestamp_format))
      if(any(is.na(activity_log$start))){
        warning("Column with start times contains NA values.")
      }
    }
    if(!is.POSIXct(activity_log$complete)){
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


# # BASIC COMPOSITE BATCHES - ADD INFORMATION ON COMPOSITE BATCHES AT THE RESOURCE-ACTIVITY LEVEL TO THE ACTIVITY LOG
# # Detection of composite batches (i.e. no distinction is made between three types of batching behavior)
# # Note: seq_tolerated_gap is specified in seconds
# add_batching_information_composite_batches <- function(activity_log, seq_tolerated_gap = 0.0,  timestamp_format = "yyyy-mm-dd hh:mm:ss"){
#   
#   # Check if character timestamps are in POSIXct format
#   if(is.character(activity_log$arrival)){
#     activity_log <- convert_activity_log_timestamp_format(activity_log, "arrival", timestamp_format = timestamp_format)
#   }
#   if(is.character(activity_log$start)){
#     activity_log <- convert_activity_log_timestamp_format(activity_log, "start", timestamp_format = timestamp_format)
#   }
#   if(is.character(activity_log$complete)){
#     activity_log <- convert_activity_log_timestamp_format(activity_log, "complete", timestamp_format = timestamp_format)
#   }
#   
#   # Sort activity log based on resource-activity combinations
#   activity_log <- activity_log %>% arrange(activity, resource, start, complete)
#   
#   # Add numeric timestamps for calculation purposes
#   activity_log$arrival_num <- as.numeric(activity_log$arrival)
#   activity_log$start_num <- as.numeric(activity_log$start)
#   activity_log$complete_num <- as.numeric(activity_log$complete)
#   
#   # When arrival_num is not available, set arrival_num to zero
#   index <- is.na(activity_log$arrival)
#   activity_log$arrival_num[index] <- 0
#   remove(index)
#   
#   # Add batch number and batch type column in activity log
#   activity_log$batch_number <- NA
#   activity_log$batch_type <- NA
#   
#   # Initialize values
#   batch_counter <- 1
#   activity_log$batch_number[1] <- batch_counter
#   start_first_batched_case <- activity_log$start_num[1]
#   
#   # Go through activity log and add batch numbers
#   for(i in 2:nrow(activity_log)){
#     
#     # Check batching conditions:
#     # (i) activity of two subsequent activity instances are the same
#     # (ii) resource of two subsequent activity instances are the same
#     # (iii) one of the following applies:
#     # - there is some overlap (complete or partial) between both instances in time
#     # - the start timestamp (almost) equals the complete timestamp of the preeding instance and both
#     # instances were present before processing of the first starts and the resource was not active
#     # during the time gap that might be allowed
#     
#     if(activity_log$activity[i] == activity_log$activity[i-1] &
#        activity_log$resource[i] == activity_log$resource[i-1] &
#        (
#          # Simultaneous or concurrent batch processing
#          (activity_log$start_num[i] >= activity_log$start_num[i-1] & activity_log$start_num[i] < activity_log$complete_num[i-1]) |
#          # Sequential batch processing
#          (activity_log$start_num[i] <= activity_log$complete_num[i-1] + seq_tolerated_gap &
#           activity_log$arrival_num[i] <= start_first_batched_case &
#           resource_active(activity_log, activity_log$resource[i], activity_log$complete_num[i-1], activity_log$start_num[i], act = activity_log$activity[i]) == FALSE) 
#        )
#     ){
#       
#       activity_log$batch_number[i] <- batch_counter
#       activity_log$batch_type[i] <- "composite"
#       
#       if(is.na(activity_log$batch_type[i-1])){
#         activity_log$batch_type[i-1] <- "composite"
#       }
#       
#     } else{
#       # Increase batch counter and add this new value to the current instance
#       batch_counter <- batch_counter + 1
#       activity_log$batch_number[i] <- batch_counter
#       
#       # Save the start time of the first case in a batch
#       start_first_batched_case <- activity_log$start_num[i]
#     }
#   }
#   
#   # Remove numeric timestamp columns
#   activity_log$arrival_num <- NULL
#   activity_log$start_num <- NULL
#   activity_log$complete_num <- NULL
#   
#   return(activity_log)
# }



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

# # DETECT TASK-BASED BATCH SUBPROCESSES - OLD IMPLEMENTATION
# # Detection of task-based batch subprocesses using BOWI batching information as a starting point. Detected batch types:
# # - parallel batching (simultaneous batching)
# # - task-based sequential batching
# # - task-based concurrent batching
# # Note: seq_tolerated_gap is specified in seconds
# # Parameters:
# # - activity_log: the activity log (subset) that needs to be investigated
# # - full_activity_log: the full activity log (required to determine whether activities immediately follow each other)
# detect_task_based_batch_subprocesses <- function(activity_log, full_activity_log){
#   
#   # # Initialize batch_subprocess column
#   # activity_log$batch_subprocess <- NA
# 
#   # Initialize batch_subprocess_id
#   batch_subprocess_id <- 1
#   
#   # Add batch composition information
#   activity_log <- activity_log %>% 
#                     group_by(batch_number) %>% 
#                     arrange(case_id) %>%
#                     mutate(batch_composition = paste(case_id, collapse = " - "), n_cases_in_batch = n())
#   
#   # Check if there are multiple batches (consisting of multiple cases) having the same composition
#   candidates <- as.data.frame(activity_log %>% filter(n_cases_in_batch > 1) %>% group_by(batch_composition) %>% summarize(n_act = n_distinct(activity)) %>% filter(n_act > 1))$batch_composition
#   
#   if(length(candidates) > 0){
#     
#     # Check if the activities immediately follow each other
#     pb <- txtProgressBar(min = 2, max = length(candidates), style = 3)
#     for(i in 1:length(candidates)){
#       rows_to_check <- activity_log %>% filter(batch_composition == candidates[i])
#       activities_to_check <- as.data.frame(rows_to_check %>% distinct(activity))$activity
#       checked_activities <- activities_to_check
#       #case_set <- as.numeric(strsplit(candidates[i], " - ")[[1]])
#       
#       for(j in 1:(length(activities_to_check) - 1)){
#         
#         rows_to_check_subset <- rows_to_check %>% filter(activity == activities_to_check[j] || activity == activities_to_check[j+1]) %>% arrange(case_id)
#         
#         error_detected <- FALSE
#         k <- 1
#         
#         while(error_detected == FALSE && k < (nrow(rows_to_check_subset) - 1)){
#           check <- activity_started(full_activity_log, rows_to_check_subset$case_id[k], c(activities_to_check[j], activities_to_check[j+1]), rows_to_check_subset$start_num[k], rows_to_check_subset$start_num[k+1])
#           
#           if(check == TRUE){
#             error_detected <- TRUE
#           } else{  # continue to next check
#             k <- k + 2
#           }
#         }
#         
#         
#         if(k != (nrow(rows_to_check_subset) - 1)){ # if an error is detected, the two activities are not in a batch subprocess
#           checked_activities <- checked_activities[-1]
#         }
#         
#       }
#       
#       # For activities forming a batch subprocess, mark the appropriate rows in the activity log with the same batch_subprocess_id
#       
#       if(length(checked_activities) > 1){ # if a batch subprocess is detected
#         
#         checked_activities_subset <- as.data.frame(rows_to_check %>% filter(activity %in% checked_activities))
#         
#         # Add batch_subprocess_id to the appropriate rows in the activity log
#         index <- activity_log$row_id %in% checked_activities_subset$row_id
#         activity_log$batch_subprocess_number[index] <- batch_subprocess_id
#         activity_log$batch_subprocess_type[index] <- "task-based"
#         remove(index)
#         
#         # Increment batch_subprocess_id
#         batch_subprocess_id <- batch_subprocess_id + 1
#       }
#       setTxtProgressBar(pb, i)
#     }
#     close(pb)
#   }
#   
#   # Remove helper columns
#   activity_log$batch_composition <- NULL
#   
#   return(activity_log)
# }


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
  
  # # Add row_id if absent
  # if(!("row_id" %in% names(activity_log))){
  #   activity_log$row_id <- seq(1, nrow(activity_log))
  # }
  # 
  # # Order activity_log
  # activity_log <- activity_log %>% arrange(case_id, row_id)
  
  # Order activity log
  activity_log <- activity_log %>% arrange(case_id, start, complete)
  
  # Add row numbers for sequence
  activity_log$seq_row_number <- seq(1,nrow(activity_log))

  # Create transactions
  activity_log_activities <- data.frame(item = activity_log$activity)
  activity_log_trans <- as(activity_log_activities, "transactions")
  transactionInfo(activity_log_trans)$sequenceID <- activity_log$case_id
  #☺transactionInfo(activity_log_trans)$eventID <- activity_log$row_id
  transactionInfo(activity_log_trans)$eventID <- activity_log$seq_row_number
  
  # Determine frequent sequences
  #frequent_sequences <- cspade(activity_log_trans, parameter = list(support=min_support))
  frequent_sequences <- cspade(activity_log_trans, parameter = list(support=min_support, maxgap = 1)) #maxgap = 1 ensures that only activities that immediately follow each other are taken into account
  #summary(frequent_sequences)
  #as.list(frequent_sequences$sequence)
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
        
        # Determine whether case_batch_type is consistent over cases
        n_distinct_case_batch_types <- length(unique(activity_log_subset$case_batch_type))
        
        # Aggregate results and mark with apropriate type of case-based batching
        if(n_distinct_case_batch_types > 1){ # Incosistent case_batch_type over cases
          activity_log_subset <- activity_log_subset %>% group_by(batch_number) %>%
            summarize(case_ids = paste(case_id, collapse = "-"),
                      row_ids = paste(case_row_ids, collapse = "-"),
                      batch_subprocess_type = paste(case_batch_type[1], "case-based")) #case_batch_type is currently considered dominant
        } else{ 
          activity_log_subset <- activity_log_subset %>% group_by(batch_number) %>%
            summarize(case_ids = paste(case_id, collapse = "-"),
                      row_ids = paste(case_row_ids, collapse = "-"),
                      batch_subprocess_type = paste("hybrid", "case-based")) #case_batch_type is currently considered dominant
          
        }
        
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



####################################################################
# PART 6: GENERIC BATCH DETECTION FUNCTION
####################################################################

# GENERIC BATCH DETECTION FUNCTION (builds upon functions defined in other parts)
# Note: seq_tolerated_gap is specified in seconds
# Parameters:
# - activity_log: the activity log
# (1) Parameters to detect batching behavior at the resource-activity level
# - act_seq_tolerated_gap_list: list containing the tolerated time gap for a particular each activity (used for sequential batching detection)
# - timestamp_format: format in which the timestamps are specified
# - numeric_timestamps: boolean indicating whether timestamps are expressed numerically (instead of in POSIXct format)
# - log_and_model_based: boolean indicating whether a notion of the process model is present (i.e. whether arrival times are contained in the activity log)
# (2) Parameters to detect case-based sequential/concurrent batch subprocesses
# - subsequence_list: list of subsequences for which case-based sequential/concurrent batching needs to be checked
# - subsequence_type: reflects the way in which subsequences are generated: by enumeration (enum) or using a sequence mining method (mine)
# - within_case_seq_tolerated_gap: tolerated time gap to detect sequential batching between activities within a particular case
# - between_cases_seq_tolerated_gap: tolerated time gap to detect sequential batching between (aggregated) activities over several cases
detect_batching_behavior <- function(activity_log, act_seq_tolerated_gap_list,  timestamp_format = "yyyy-mm-dd hh:mm:ss", numeric_timestamps = TRUE, log_and_model_based,
                                     subsequence_list, subsequence_type, within_case_seq_tolerated_gap = 0, between_cases_seq_tolerated_gap = 0){
  
  ### ADD BASIC BATCHING INFORMATION (RESOURCE-ACTIVITY LEVEL)
  # After testing, a custom-version of this function can be made in which, e.g., the removal of start_num etc. is removed (as this is used later in the function)
  print("Detecting batching behavior at the resource-activity level (phase 1 of 3)...")
  activity_log <- add_batching_information(activity_log, act_seq_tolerated_gap_list, timestamp_format, numeric_timestamps, log_and_model_based)
  
  ### PREPARATORY STEPS
  
  # Add row number to activity log (will be used later in this function and in helper functions)
  activity_log$row_id <- seq(1, nrow(activity_log))
  
  # Add batch_activity_number and batch_activity_type columns
  activity_log$batch_subprocess_number <- NA
  activity_log$batch_subprocess_type <- NA
  
  # Add numeric timestamps for calculation purposes
  activity_log$arrival_num <- as.numeric(activity_log$arrival)
  activity_log$start_num <- as.numeric(activity_log$start)
  activity_log$complete_num <- as.numeric(activity_log$complete)
  
  # Order activity log
  activity_log <- activity_log %>% arrange(start_num, complete_num)
  
  # Divide activity log into 2 subsets:
  # - subset 1: activity instances which belong to a batch at the resource-activity level
  # - subset 2: activity instances which do not belong to a batch at the resource-activity level
  # EXCEPTIONS: concurrent and sequential batching is included in both subsets (reason: with strong overlap, it can be the case
  # that a concurrent/sequential batch is formed at the resource-activity level even when concurrent case-based batching prevails)
  activity_log_ss1 <- activity_log %>% filter(!is.na(batch_type))
  activity_log_ss2 <- activity_log %>% filter(is.na(batch_type) | batch_type == "concurrent" | batch_type == "sequential")
  
  ### DETECT PARALLEL, TASK-BASED SEQUENTIAL AND TASK-BASED CONCURRENT BATCH SUBPROCESSES
  # Uses activity_log_ss1 because these types of subprocesses require that a parallel/sequential/concurrent
  # batching is present for the individual activities of which the subprocess exists
  print("Detecting parallel, task-based sequential and task-based concurrent batch subprocesses (phase 2 of 3)...")
  if(nrow(activity_log_ss1) > 0){
    activity_log_ss1 <- detect_task_based_batch_subprocesses(activity_log_ss1, activity_log)
  }
  
  # Determine next batch_subprocess_number
  if(length(unique(activity_log_ss1$batch_subprocess_number)) == 1){ # Implies that batch_subprocess_number equals NA for all values
    start_value <- 1
  } else{
    start_value <- max(activity_log_ss1$batch_subprocess_number, na.rm = TRUE) + 1
  }
  
  # Remove rows included in a task-based concurrent/sequential subprocess from activity_log_ss2 (otherwise they are duplicated in the output as batch_type concurrent is included both in activity_log_ss1 and activity_log_ss2)
  row_ids_to_remove <- (activity_log_ss1 %>% filter(batch_type %in% c("concurrent", "sequential"), batch_subprocess_type == "task-based"))$row_id
  activity_log_ss2 <- activity_log_ss2 %>% filter(!(row_id %in% row_ids_to_remove))
  
  # Remove rows which are not included in a task-based concurrent/sequential subprocess from activity_log_ss1 (otherwise they are duplicated in the output as batch_type concurrent is included both in activity_log_ss1 and activity_log_ss2)
  row_ids_to_remove <- (activity_log_ss1 %>% filter(batch_type %in% c("concurrent", "sequential"), is.na(batch_subprocess_type)))$row_id
  activity_log_ss1 <- activity_log_ss1 %>% filter(!(row_id %in% row_ids_to_remove))
  
  
  ### DETECT CASE-BASED SEQUENTIAL AND CASE-BASED CONCURRENT BATCH SUBPROCESSES
  # Uses activity_log_ss2 because for these types of subprocesses no batching is present for the individual activities
  # of which the subprocess exists
  print("Detecting case-based sequential and case-based concurrent batch subprocesses (phase 3 of 3)...")
  
  if(nrow(activity_log_ss2) > 0){
    
    activity_log_ss2 <- activity_log_ss2 %>% arrange(row_id)
    
    if(subsequence_type == "enum"){
      # Subsequence_list is determined by means of enumeration
      
      subsequence_list <- tibble(subsequence = subsequence_list)
      subsequence_list <- subsequence_list %>% rowwise() %>% mutate(subseq_string = paste(subsequence, collapse = "-"),
                                                                    length = length(subsequence))
      subsequence_list <- subsequence_list %>% filter(length > 1) %>% arrange(length)
      subsequence_list$to_check <- TRUE
      
      # Detect case-based sequential/concurrent batching
      pb <- txtProgressBar(min = 1, max = nrow(subsequence_list), style = 3)
      for(i in 1:nrow(subsequence_list)){
        
        if(subsequence_list$to_check[i] == TRUE){
          # If this subsequence needs to be checked
          
          if(length(unique(activity_log_ss2$batch_subprocess_number)) > 1){ # Implies that another value than NA has been recorded
            start_value <- max(activity_log_ss2$batch_subprocess_number, na.rm = TRUE) + 1
          }

          case_batching_output <- check_case_batching_relation(activity_log = activity_log_ss2, 
                                                                activity_set = subsequence_list$subsequence[[i]], 
                                                                within_case_seq_tolerated_gap = within_case_seq_tolerated_gap, 
                                                                between_cases_seq_tolerated_gap = between_cases_seq_tolerated_gap, 
                                                                batch_subprocess_number_start = start_value)
          
          detected_case_based_batches <- as.data.frame(case_batching_output[[1]])
          remove_higher_order_subsequences <- case_batching_output[[2]]

          # Add detected batches to activity_log_ss2
          if(nrow(detected_case_based_batches) > 0){
            
            detected_case_based_batches <- detected_case_based_batches %>%
              mutate(row_ids = strsplit(as.character(row_ids), "-")) %>%
              unnest() %>% arrange(row_ids)
            
            activity_log_ss2 <- activity_log_ss2 %>% arrange(row_id)
            
            index <- activity_log_ss2$row_id %in% detected_case_based_batches$row_ids
            activity_log_ss2$batch_subprocess_number[index] <- detected_case_based_batches$batch_subprocess_number
            activity_log_ss2$batch_subprocess_type[index] <- detected_case_based_batches$batch_subprocess_type
            remove(index)
          } #else{  # Old, less stringent removel criterion
          #   # If no batches are detected, all "higher-order" subsequences are removed from subsequence_to_test
          #   index <- str_detect(subsequence_list$subseq_string, subsequence_list$subseq_string[i])
          #   subsequence_list$to_check[index] <- FALSE
          #   remove(index)
          # }
          
          # Remove "higher-order" subsequences from subsequence_to_test when applicable
          if(remove_higher_order_subsequences == TRUE){
            index <- str_detect(subsequence_list$subseq_string, subsequence_list$subseq_string[i])
            subsequence_list$to_check[index] <- FALSE
            remove(index)
          }
          
        }
        setTxtProgressBar(pb, i)
      }
      close(pb)
      
    } else{
      # Subsequence_list is determined using sequence mining method
      
      # Detect whether case-based sequential/concurrent batching is present for each element of the subsequence_list
      pb <- txtProgressBar(min = 1, max = length(subsequence_list), style = 3)
      for(i in 1:nrow(subsequence_list)){
        subsequence_to_test <- subsequence_list$sequence_list[[i]]
        
        # Determine first possible identification number of batch subprocesses (to use as input in the following function call)
        if(length(unique(activity_log_ss2$batch_subprocess_number)) > 1){ # Implies that another value than NA has been recorded
          start_value <- max(activity_log_ss2$batch_subprocess_number, na.rm = TRUE) + 1
        }
        
        batching_output <- check_case_batching_relation(activity_log = activity_log_ss2, 
                                                        activity_set = subsequence_to_test, 
                                                        within_case_seq_tolerated_gap = within_case_seq_tolerated_gap, 
                                                        between_cases_seq_tolerated_gap = between_cases_seq_tolerated_gap, 
                                                        batch_subprocess_number_start = start_value)
        detected_case_based_batches <- as.data.frame(batching_output[[1]])
        
        # Add detected batches to activity_log_ss2
        if(nrow(detected_case_based_batches) > 0){
          
          detected_case_based_batches <- detected_case_based_batches %>%
            mutate(row_ids = strsplit(as.character(row_ids), "-")) %>%
            unnest() %>% arrange(row_ids)
          
          activity_log_ss2 <- activity_log_ss2 %>% arrange(row_id)
          
          index <- activity_log_ss2$row_id %in% detected_case_based_batches$row_ids
          activity_log_ss2$batch_subprocess_number[index] <- detected_case_based_batches$batch_subprocess_number
          activity_log_ss2$batch_subprocess_type[index] <- detected_case_based_batches$batch_subprocess_type
          remove(index)
        }
        
        setTxtProgressBar(pb, i)
      }
      close(pb)
      
    }
  }

  ### COMBINE ACTIVITY_LOG_SS1 AND ACTIVITY_LOG_SS2
  print("Combining results...")
  
  # Remove n_cases_in_batch column from activity_log_ss1
  activity_log_ss1$n_cases_in_batch <- NULL
  
  # Combine activity_log_ss1 and activity_log_ss2
  activity_log_ss1 <- as.data.frame(activity_log_ss1)
  activity_log_ss2 <- as.data.frame(activity_log_ss2)
  activity_log <- rbind(activity_log_ss1, activity_log_ss2)

  
  ### CLEANING UP
  activity_log$arrival_num <- NULL
  activity_log$start_num <- NULL
  activity_log$complete_num <- NULL
  activity_log$row_id <- NULL
  
  ### PRINT FINAL MESSAGE
  print("Batch detection finalized...")
  
  return(activity_log)
  
}



