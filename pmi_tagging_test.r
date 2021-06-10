#-- test that tags in PMI match result of source data queries
rm(list = objects())
options(stringsAsFactors = FALSE,
    scipen = 200)
library(wrangleR)
library(tidyverse)
library(DBI)
p <- getprofile(c("dams_con", "aws_pmi"))
dams_con <- dbConnect(RPostgres::Postgres(),
             dbname = "labkey",
             host     = p$dams_con$host,
             port     = p$dams_con$port,
             user     = p$dams_con$user,
             password = p$dams_con$password)
pmi_con <- dbConnect(RPostgres::Postgres(),
             dbname = "trifacta",
             host     = p$aws_pmi$host,
             port     = p$aws_pmi$port,
             user     = p$aws_pmi$user,
             password = p$aws_pmi$password)

run_query <- function(conn, f) {
    # run the contents of sql file - f- on a given db connection - conn
    require(DBI)
    q <- trimws(readLines(f))
    # remove lines starting with --
    q <- gsub("^--.*$", "", q)
    d <- dbGetQuery(conn, paste(q, collapse = " "))
    cat(paste0(f, ' - ', nrow(d), 'Rx', ncol(d), 'C\n'))
    return(d)
}

get_pmi_tag <- function(tag) {
    # get the participant_ids that currently have the tag
    # where tag is the concept code of the tag
    bq <- "select sp.identifier_value as participant_id
    from pmi.tag_membership tm
    join pmi.study_participant sp
        on tm.study_participant_uid = sp.uid
    join pmi.concept tc
        on tc.uid = tm.tag_cid
    where tc.concept_code = '$$tag$$'
    ;"
    q <- gsub("$$tag$$", tag, bq, fixed = TRUE)
    d <- dbGetQuery(pmi_con, q)
    return(d$participant_id)
}

get_source_tag <- function(tag, src) {
    # get the participant_ids from the source data that should be given
    # the tag. Where tag is the concept_code for the tag, and src refers to
    # dams or ....
    conn <- eval(parse(text = paste0(src, "_con")))
    fn <- paste0("./source_queries/", src, "/", tag, ".sql")
    d <- run_query(conn, fn)
    return(d$participant_id)
}

prepare_comparison_table <- function(d1, d2, label_d1, label_d2) {
    # generate a dataframe that lists all participants in d1 and d2 and then
    # gives columns for in_d1 and in_d2
    d <- data.frame("participant_id" =
                          unique(c(d1, d2)))
    d[[paste0("in_", label_d1)]] <- d$participant_id %in% d1
    d[[paste0("in_", label_d2)]] <- d$participant_id %in% d2
    return(d)
}

compare_tag_results <- function(tag, src) {
    # print out some comparison stuff and then return a comparison table for 
    # a given tag against the src
    cat(paste("Comparing results for", tag, "...\n"))
    pmi_ids <- unique(get_pmi_tag(tag))
    src_ids <- unique(get_source_tag(tag, src))
    cat(paste("Got", length(pmi_ids), "participants from pmi\n"))
    cat(paste("Got", length(src_ids), "participants from", src, "\n"))
    n_missing_pmi <- sum(!src_ids %in% pmi_ids)
    n_missing_src <- sum(!pmi_ids %in% src_ids)
    cat(paste(n_missing_pmi, "missing from PMI,", n_missing_src, "missing from", src, "\n\n"))
    return(prepare_comparison_table(pmi_ids, src_ids, "pmi", src))
}

# now loop through each of the sources in source_queries folder and then
# each of the tags in those folders and compare the data
srcs <- list.dirs("./source_queries/", full.names = F, recursive = F)
d <- list()
for(src in srcs) {
    d[[src]] <- list()
    tags <- gsub(".sql", "",
                 list.files(paste0("./source_queries/", src),
                       pattern = "*.sql", full.names = F),
                 fixed = TRUE)
    for(tag in tags) {
        d[[src]][[tag]] <- compare_tag_results(tag, src)
    }
}
