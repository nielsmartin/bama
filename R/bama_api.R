
#' @title Detect Batch Behavior in an Event Log
#'
#' @description TODO
#'
#' @param log The `bupaR` event log object that should be animated
#'
#'
#' @export
detect_batching <- function(log,
                            subsequence_type = c("mine", "enum"),
                            subsequence_min_support = 0.01,
                            seq_tolerated_gap = 180,
                            within_case_seq_tolerated_gap = seq_tolerated_gap,
                            between_cases_seq_tolerated_gap = seq_tolerated_gap,
                            arrival_event_gap = NULL) {

  subsequence_type <- match.arg(subsequence_type)
  has_arrival <- ("arrival" %in% colnames(log))

  if (!has_arrival && is.null(arrival_event_gap)) {
    # bama needs arrival column
    arrival_event_gap <- 0
  }

  # From bupaR log to activity log
  log_info <- convert_from_bupar(log, arrival_event_gap)

  activity_log <- log_info$log
  case_map <- log_info$case_map


  # GENERATE SEQUENTIAL TOLERATED GAP LIST (expressed in seconds)
  # The sequential tolerated gap list outlines, for each activity, how many seconds instances of this activity
  # can be apart for it to be compose a sequential batch of cases. The function that is called here will
  # take a strict perspective and apply a sequential tolerated gap of 0 seconds for each activity (hence: only when
  # activity instances follow each other immediately, they will form a sequential batch). In real-life, this value is
  # likely to be too rigid.
  seq_tolerated_gap_list <- seq_tolerated_gap_list_generator(activity_log = activity_log,
                                                             seq_tolerated_gap_value = seq_tolerated_gap)

  # CREATE SUBSEQUENCE LIST
  # The subsequence list contains all sets of combinations for which case-based batching behavior will be detected
  # by the algorithm. It can be created in two ways:
  # (i) By means of enumeration: all (relevant) subsets of activities are considered
  # (ii) By identifying frequent sequences in traces using the SPADE algoritm
  # Only (i) or (ii) should be executed, with the use of the corresponding subsequence_type

  if (subsequence_type == "enum") {
    # (i) Create subsequence list using enumeration without applying any threshold on the number of times a trace occurs
    subsequence_list <- enumerate_subsequences(activity_log, 0)
  } else {
    # (ii) Create subsequence list using the SPADE algorithm without putting threshold on the minimum support
    subsequence_list <- identify_frequent_sequences(activity_log, subsequence_min_support)
  }

  # DETECT BATCHING BEHAVIOR
  # This function detects batching behavior
  # Parameters:
  # - activity_log: the activity log
  # (1) Parameters to detect batching behavior at the resource-activity level
  # - act_seq_tolerated_gap_list: list containing the tolerated time gap for a particular each activity (used for sequential batching detection)
  # - timestamp_format: format in which the timestamps are specified
  # - numeric_timestamps: boolean indicating whether timestamps are expressed numerically (instead of in POSIXct format)
  # - log_and_model_based: boolean indicating whether arrival times are contained in the activity log
  # (2) Parameters to detect case-based sequential/concurrent batch subprocesses
  # - subsequence_list: list of subsequences for which case-based sequential/concurrent batching needs to be checked
  # - subsequence_type: reflects the way in which subsequences are generated: by enumeration (enum) or using a sequence mining method (mine)
  # - within_case_seq_tolerated_gap: tolerated time gap to detect sequential batching between activities within a particular case
  # - between_cases_seq_tolerated_gap: tolerated time gap to detect sequential batching between (aggregated) activities over several cases
  activity_log <- detect_batching_behavior(activity_log = activity_log,
                                           act_seq_tolerated_gap_list = seq_tolerated_gap_list,
                                           timestamp_format = "yyyy-mm-dd hh:mm:ss",
                                           numeric_timestamps = FALSE,
                                           log_and_model_based = has_arrival,
                                           subsequence_list = subsequence_list,
                                           subsequence_type = subsequence_type,
                                           within_case_seq_tolerated_gap = within_case_seq_tolerated_gap,
                                           between_cases_seq_tolerated_gap = between_cases_seq_tolerated_gap)

  col_case <- bupaR::case_id(log)
  col_act <- bupaR::activity_id(log)
  col_act_id <- bupaR::activity_instance_id(log)
  suppressWarnings( # factor/charvec coercing
    log %>%
      as_tibble() %>%
      left_join(case_map, by = setNames(c("case_id_org"), c(col_case))) %>%
      left_join(activity_log, by = setNames(c("case_id", "activity", "activity_instance"), c("case_id", col_act, col_act_id))) %>%
      select(-start, -complete, -resource, -case_id, -case_id_org) %>%
      bupaR::re_map(., bupaR::mapping(log)) %>%
      return()
  )
}

convert_from_bupar <- function(log, arrival_event_gap) {

  log %>%
      as_tibble() %>%
      dplyr::rename(case_id_org = case_id_(log),
                    activity = activity_id_(log),
                    activity_instance = activity_instance_id_(log),
                    timestamp = timestamp_(log),
                    resource = resource_id_(log)) %>%
      mutate(activity = as.character(activity),
             resource = as.character(resource)) %>%
      group_by(case_id_org) %>%
      mutate(case_id = group_indices()) %>%
      group_by(case_id_org, case_id, activity, activity_instance) %>%
      summarise(start = min(timestamp),
                complete = max(timestamp),
                resource = first(resource)) %>%
      group_by(case_id) %>%
      arrange(start) %>%
      ungroup() -> bama_log

  if (!is.null(arrival_event_gap)) {
    mutate(bama_log, arrival = start - (arrival_event_gap)) -> bama_log
  }

  if (any(is.na(bama_log$resource))) {
    stop("Bama requires the event log to specify a resource for each event. At least one event has a NA value as resource identifier.")
  }

  list(log = bama_log, case_map = bama_log %>% select(case_id, case_id_org))
}

# Utility functions
# https://github.com/gertjanssenswillen/processmapR/blob/master/R/utils.R
case_id_ <- function(eventlog) rlang::sym(bupaR::case_id(eventlog))
timestamp_ <- function(eventlog) rlang::sym(bupaR::timestamp(eventlog))
activity_id_ <- function(eventlog) rlang::sym(bupaR::activity_id(eventlog))
activity_instance_id_ <- function(eventlog) rlang::sym(bupaR::activity_instance_id(eventlog))
resource_id_ <- function(eventlog) rlang::sym(bupaR::resource_id(eventlog))
