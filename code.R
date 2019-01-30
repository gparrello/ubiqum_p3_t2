pacman::p_load(
  "RMySQL",
  "lubridate",
  "dplyr",
  "imputeTS",
  "rbokeh",
  "padr",
  "forecast"
)
set.seed(123)

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
attr(df$Datetime, "tzone") <- "GMT" # better than "Europe/Paris"
df[,c("X","Date","Time","id")] <- NULL
original_df <- df
df <- pad(df, break_above = 3e6)
df <- na.kalman(df)


aggregated_df <- c()
aggregated_ts <- c()
decomposed_ts <- c()
decomposed_ts2 <- c()
granularity <- c("day", "week", "month")
frequency <- c(365, 52, 12)
names(frequency) <- granularity
for(g in granularity){
  aggregated_df[[g]] <- df %>%
    group_by(date = as.Date(floor_date(Datetime, unit = g))) %>%  # hour grouping not working!
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
  
  aggregated_ts[[g]] <- ts(
    aggregated_df[[g]],
    frequency = frequency[[g]]
  )
  
  my_vector <- c()
  my_vector2 <- c()
  for(c in colnames(aggregated_ts[[g]])[!colnames(aggregated_ts[[g]]) %in% c("date", "obs")]){
    print(paste(g, c))
    my_vector[[c]] <- stl(
      aggregated_ts[[g]][,c],
      s.window = "periodic"
    )
    my_vector2[[c]] <- stl(
      aggregated_ts[[g]][,c],
      s.window = frequency[[g]]
    )
  }
  decomposed_ts[[g]] <- my_vector
  decomposed_ts2[[g]] <- my_vector2
}

# daily_ts <- ts(aggregated_df[[g]], start=c(2006,12,16), frequency = 365)
# daily_ts_imputed <- na.kalman(daily_ts) # use different for short and long missing
# try interpolation, kalman, spline and random

# p <- figure() %>%
  # ly_points(daily_ts, x = date, y = obs, color='purple') #%>%
  #ly_lines(aggregated_df[[g]], x = date, y = sub1, color='green') %>%
  #ly_lines(aggregated_df[[g]], x = date, y = sub2, color='red') %>%
  #ly_lines(aggregated_df[[g]], x = date, y = sub3, color='blue')
