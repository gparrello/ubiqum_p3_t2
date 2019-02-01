pacman::p_load(
  "data.table",
  "RMySQL",
  "lubridate",
  "dplyr",
  "imputeTS",
  "rbokeh",
  "padr",
  "forecast",
  "ggplot2"
)
set.seed(123)

fQuery <- function(){
  years <- c('2006', '2007', '2008', '2009', '2010')
  conn = dbConnect(
    MySQL(),
    user='deepAnalytics',
    password='Sqltask1234!',
    dbname='dataanalytics2018',
    host='data-analytics-2018.cbrosir2cswx.us-east-1.rds.amazonaws.com'
  )
  query <- ""
  for(yr in years){
    if(!yr == head(years,1)){
      query <- paste(query, "UNION ALL")
    }
    query <- paste(query, " SELECT * FROM yr_", yr, sep='')
  }
  df <- dbGetQuery(
    conn,
    query
  )
  df <- as.data.table(df)
  return(df)
}

fAggregate <- function(input){
  output <- input %>%
    group_by(date = cut(Datetime, g)) %>%  # hour grouping not working!
    summarize(
      obs = n(),
      sub1 = sum(Sub_metering_1),
      sub2 = sum(Sub_metering_2),
      sub3 = sum(Sub_metering_3),
      nosub = sum(Global_active_power*1000/60 - Sub_metering_1 - Sub_metering_2 - Sub_metering_3),
      active_sum = sum(Global_active_power),
      active_mean = mean(Global_active_power),
      active_min = mean(Global_active_power) - sd(Global_active_power),
      active_max = mean(Global_active_power) + sd(Global_active_power),
      reactive_sum = sum(Global_reactive_power),
      reactive_mean = mean(Global_reactive_power),
      reactive_min = mean(Global_reactive_power) - sd(Global_reactive_power),
      reactive_max = mean(Global_reactive_power) + sd(Global_reactive_power),
      intensity_sum = sum(Global_intensity),
      intensity_mean = mean(Global_intensity),
      intensity_min = mean(Global_intensity) - sd(Global_intensity),
      intensity_max = mean(Global_intensity) + sd(Global_intensity),
      voltage_sum = sum(Voltage),
      voltage_mean = mean(Voltage),
      voltage_min = mean(Voltage) - sd(Voltage),
      voltage_max = mean(Voltage) + sd(Voltage)
    )
  
  output$obs <- as.integer(output$obs)
  
  return(output)
}



original_file <- "./data/original.csv"
processed_file <- "./data/processed.csv"
if(file.exists(processed_file)){
  print("loading processed data from file")
  # df <- read.csv(processed_file)
  df <- fread(processed_file)
  df$Datetime <- as.POSIXct(df$Datetime, origin="1970-01-01", tz="GMT")
} else {
  if(!file.exists(original_file)){
    print("querying original data")
    df <- fQuery()
    print("writing original data to file")
    # write.csv(df, file=original_file)
    fwrite(df, file = original_file, dateTimeAs = "epoch")
  } else {
    print("loading original data from file")
    # df <- read.csv(original_file)
    df <- fread(original_file)
  }

  # some transformations
  df$Datetime <- paste(df$Date, '', df$Time)
  df$Datetime <- as.POSIXct(df$Datetime, "%Y/%m/%d %H:%M:%S", tz="GMT")
  # attr(df$Datetime, "tzone") <- "GMT" # better than "Europe/Paris"
  df[,c("Date","Time","id")] <- NULL
  # original_df <- df
  df <- pad(df, break_above = 3e6)
  df <- na.kalman(df)
  # try interpolation, kalman, spline and random

  print("writing processed data to file")
  # write.csv(df, file=processed_file)
  fwrite(df, file = processed_file, dateTimeAs = "epoch")

}


print("calculating granularity")

aggregated_df <- c()
aggregated_ts <- c()
decomposed_ts <- c()

granularity <- c("hour", "day", "week", "month")
frequency <- c(365.25*24, 365, 52, 12)
start <- c(
  1,
  yday(df$Datetime[1]),  # calculate the day of the year for the first row
  week(df$Datetime[1]),
  month(df$Datetime[1])
)
names(frequency) <- granularity
names(start) <- granularity
for(g in granularity){
  aggregated_df[[g]] <- fAggregate(df)
  
  aggregated_ts[[g]] <- ts(
    aggregated_df[[g]],
    frequency = frequency[[g]],
    start=c(2006,start[[g]])
  )
  
  my_vector <- c()
  for(c in colnames(aggregated_ts[[g]])[!colnames(aggregated_ts[[g]]) %in% c("date", "obs")]){
    my_vector[[c]] <- stl(
      aggregated_ts[[g]][,c],
      s.window = "periodic"
      # s.window = frequency[[g]]
    )
    
  }
  decomposed_ts[[g]] <- my_vector
  
}


## Forecasting

train_month <- window(aggregated_ts[["month"]][,"active_sum"], start=c(2006,12), end=c(2009,12))
# test_month <- 
