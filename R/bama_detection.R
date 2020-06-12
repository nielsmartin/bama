
####################################################################
# PART 6: GENERIC BATCH DETECTION FUNCTION
####################################################################

#' @title Detect Batch Behavior in a Task Log
#'
#' @description Detect batch behavior using the Bama algorithm based on a plain task log.
#'
#' @param task_log The task log containing task or activity instances.
#' @param act_seq_tolerated_gap_list List containing the tolerated time gap for a particular each activity (used for sequential batching detection)
#' @param timestamp_format Format in which the timestamps are specified
#' @param numeric_timestamps Boolean indicating whether timestamps are expressed numerically (instead of in POSIXct format)
#' @param log_and_model_based Boolean indicating whether a notion of the process model is present (i.e. whether arrival times are contained in the activity log)
#' @param subsequence_list List of subsequences for which case-based sequential/concurrent batching needs to be checked
#' @param subsequence_type Reflects the way in which subsequences are generated: by enumeration (enum) or using a sequence mining method (mine)
#' @param within_case_seq_tolerated_gap Tolerated time gap to detect sequential batching between activities within a particular case
#' @param between_cases_seq_tolerated_gap Tolerated time gap to detect sequential batching between (aggregated) activities over several cases
#' @param show_progress Whether to show a progress bar in the console.
#'
#' @export
detect_batching <- function(task_log,
                           act_seq_tolerated_gap_list,
                           timestamp_format = "yyyy-mm-dd hh:mm:ss",
                           numeric_timestamps = TRUE,
                           log_and_model_based,
                           subsequence_list,
                           subsequence_type,
                           within_case_seq_tolerated_gap = 0,
                           between_cases_seq_tolerated_gap = 0,
                           show_progress = T){

  activity_log <- task_log

  ### ADD BASIC BATCHING INFORMATION (RESOURCE-ACTIVITY LEVEL)
  # After testing, a custom-version of this function can be made in which, e.g., the removal of start_num etc. is removed (as this is used later in the function)
  print("Detecting batching behavior at the resource-activity level (phase 1 of 3)...")
  activity_log <- add_batching_information(activity_log, act_seq_tolerated_gap_list, timestamp_format, numeric_timestamps, log_and_model_based, show_progress)

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
    activity_log_ss1 <- detect_task_based_batch_subprocesses(activity_log_ss1, activity_log, show_progress)
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
            index <- stringr::str_detect(subsequence_list$subseq_string, subsequence_list$subseq_string[i])
            subsequence_list$to_check[index] <- FALSE
            remove(index)
          }

        }
        if (show_progress) {
          setTxtProgressBar(pb, i)
        }
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

        if (show_progress) {
          setTxtProgressBar(pb, i)
        }
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



