test_that("batch detect patients example", {
  expect_is(detect_batching_log(eventdataR::patients), "eventlog")
})
