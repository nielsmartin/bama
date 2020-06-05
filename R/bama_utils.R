#
# TODO check those functions and export them if useful
#

# ADD ARRIVAL EVENTS TO THE EVENT LOG
add_arrival_events <- function(renamed_event_log, previous_in_trace = TRUE){

  if(previous_in_trace == FALSE){
    stop("Sorry, this is not supported yet.")
  }

  # If timestamps are given in POSIXct format, convert it to character to avoid
  # copying issues.
  if(lubridate::is.POSIXct(renamed_event_log$timestamp)){
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
  if(lubridate::is.POSIXct(activity_log$start)){
    activity_log$start <- as.character(activity_log$start)
  }
  if(lubridate::is.POSIXct(activity_log$complete)){
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

# REMOVE SELF-LOOPS FROM ACTIVITY LOG
# If the sequential execution of the same activity (by the same resource) is not considered as batch processing,
# such self-loops need to be abstracted from in the activity log. This is implemented by replacing these
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

