pacman::p_load(
  "RMySQL",
  "lubridate",
  "dplyr",
  "imputeTS",
  "rbokeh",
  "padr"
)

years <- c('2006', '2007', '2008', '2009', '2010')
my_file <- "./data/original.csv"
if(!file.exists(my_file)){
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
  write.csv(df, file=my_file)
} else {
  df <- read.csv(my_file)
}

# some transformations
df$Datetime <- paste(df$Date, '', df$Time)
df$Datetime <- as.POSIXct(df$Datetime, "%Y/%m/%d %H:%M:%S")
# attr(df$Datetime, "tzone") <- "Europe/Paris"
df$X <- NULL
original_df <- df
df <- pad(df, break_above = 3*10^6)

missing <- which(is.na(df$id))
set.seed(123)
# my_density <- data.frame()
for(m in head(missing,1000)){
  # print(paste("row number", m))
  my_posix <- as.POSIXlt(df[m,]$Datetime)
  my_minute <- my_posix$min
  my_hour <- my_posix$hour
  my_weekday <- my_posix$wday
  # print(paste("this are details", my_posix, my_minute, my_hour, my_weekday))
  my_data_slice <- original_df[which(
    as.POSIXlt(original_df$Datetime)$min == my_minute &
    as.POSIXlt(original_df$Datetime)$hour == my_hour &
    as.POSIXlt(original_df$Datetime)$wday == my_weekday 
  ),]
  # print(paste("na rows in slice", length(which(is.na(my_data_slice)))))
  my_random <- runif(1)
  # print(my_random)
  for(c in colnames(df[, -which(names(df) %in% c("id", "Date", "Time", "Datetime"))])){
    df[m,c] <- quantile(my_data_slice[,c], my_random)
    # print(paste("this is column", c, "and I will impute", my_impute))
  }
}

aggregated_df <- c()
# aggregated_ts <- c()
granularity <- c("hour", "day", "week", "month", "year")
for(g in granularity){
  aggregated_df[[g]] <- df %>%
    group_by(date = as.Date(cut(Datetime, unit = g))) %>%  # hour grouping not working!
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
  
  aggregated_df[[g]]$obs <- as.integer(aggregated_df[[g]]$obs)
  #aggregated_df[[g]] <- pad(aggregated_df[[g]])
  #if(nrow(aggregated_df[[g]][is.na(aggregated_df[[g]]$obs),]) != 0){
  #  aggregated_df[[g]][is.na(aggregated_df[[g]]$obs),]$obs <- 0
  #}
}

# daily_ts <- ts(aggregated_df[[g]], start=c(2006,12,16), frequency = 365)
# daily_ts_imputed <- na.kalman(daily_ts) # use different for short and long missing
# try interpolation, kalman, spline and random

# p <- figure() %>%
  # ly_points(daily_ts, x = date, y = obs, color='purple') #%>%
  #ly_lines(aggregated_df[[g]], x = date, y = sub1, color='green') %>%
  #ly_lines(aggregated_df[[g]], x = date, y = sub2, color='red') %>%
  #ly_lines(aggregated_df[[g]], x = date, y = sub3, color='blue')
