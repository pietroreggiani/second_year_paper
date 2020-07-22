#' ---
#' title: "R scripts can be rendered!"
#' author: "Jenny Bryan"
#' date: "April 1, 2014"
#' output: github_document
#' ---
#'
#' Here's some prose in a very special comment. Let's summarize the built-in
#' dataset `VADeaths`.

## here is a regular code comment, that will remain as such

#' Here's some more prose. I can use usual markdown syntax to make things
#' **bold** or *italics*. Let's use an example from the `dotchart()` help to
#' make a Cleveland dot plot from the `VADeaths` data. I even bother to name
#' this chunk, so the resulting PNG has a decent name.
#+ dotchart


res <- dbSendQuery(wrds, "select * from crsp.dsf")
