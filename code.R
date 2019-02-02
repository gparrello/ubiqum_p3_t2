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
      active = sum(Global_active_power),
      # active_mean = mean(Global_active_power),
      # active_min = mean(Global_active_power) - sd(Global_active_power),
      # active_max = mean(Global_active_power) + sd(Global_active_power),
      reactive = sum(Global_reactive_power),
      # reactive_mean = mean(Global_reactive_power),
      # reactive_min = mean(Global_reactive_power) - sd(Global_reactive_power),
      # reactive_max = mean(Global_reactive_power) + sd(Global_reactive_power),
      intensity = sum(Global_intensity),
      # intensity_mean = mean(Global_intensity),
      # intensity_min = mean(Global_intensity) - sd(Global_intensity),
      # intensity_max = mean(Global_intensity) + sd(Global_intensity),
      voltage = sum(Voltage)#,
      # voltage_mean = mean(Voltage),
      # voltage_min = mean(Voltage) - sd(Voltage),
      # voltage_max = mean(Voltage) + sd(Voltage)
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
  print("chivato")
  df <- na.kalman(df)
  # try interpolation, kalman, spline and random

  print("writing processed data to file")
  # write.csv(df, file=processed_file)
  fwrite(df, file = processed_file, dateTimeAs = "epoch")

}


print("calculating granularity")

aggdf <- c()
tseries <- c()
tseriesDecomp <- c()
relRemainder <- data.frame()
absRemainder <- data.frame()

granularity <- c("hour", "day", "week", "month")
frequency <- c(365*24, 365, 52, 12)
start <- c(
  (yday(df$Datetime[1]) - 1)*24 + hour(df$Datetime[1]),
  yday(df$Datetime[1]),  # calculate the day of the year for the first row
  week(df$Datetime[1]),
  month(df$Datetime[1])
)
n <- nrow(df)
end <- c(
  (yday(df$Datetime[n]) - 1)*24 + hour(df$Datetime[1]),
  yday(df$Datetime[n]),  # calculate the day of the year for the first row
  week(df$Datetime[n]),
  month(df$Datetime[n])
)
names(frequency) <- granularity
names(start) <- granularity
names(end) <- granularity

for(g in granularity){
  aggdf[[g]] <- fAggregate(df)
  
  tseries[[g]] <- ts(
    aggdf[[g]],
    frequency = frequency[[g]],
    start=c(2006,start[[g]]),
    end=c(2010,end[[g]])
  )
  
  vDecomp <- c()
  excluded_columns <- colnames(tseries[[g]]) %in% c("date", "obs")
  for(c in colnames(tseries[[g]])[!excluded_columns]){
    vDecomp[[c]] <- stl(
      tseries[[g]][,c],
      s.window = "periodic"
      # s.window = frequency[[g]]
    )
    absRemainder[g,c] <- mean(abs(remainder(vDecomp[[c]])))
    relRemainder[g,c] <- mean(abs(remainder(vDecomp[[c]])))/mean(tseries[[g]][,c])
    
  }
  tseriesDecomp[[g]] <- vDecomp
  
}


## Modeling
full_set <- tseries[["month"]][,"active"]
train_set <- window(full_set, start=c(2006,12), end=c(2009,12))
test_set <- window(full_set, start=c(2010,1), end=c(2010,11))

# Linear model
models <- c()
models[["linear"]] <- tslm(train_set ~ trend + season)
models[["arima"]] <- arima(train_set, order = c(0,0,1), seasonal = c(1,1,0))
models[["hw"]] <- HoltWinters(train_set)

## Forecasting
forecasts <- c()
accuracies <- c()
for(m in names(models)){
  forecasts[[m]] <- forecast(models[[m]], h=10, level=c(80,90))
  accuracies[[m]] <- accuracy(forecasts[[m]], test_set)
}

plots <- c()
for(f in names(forecasts)){
  plots[[f]] <- autoplot(full_set, series="Real") +
    autolayer(forecasts[[f]], series = "Forecasted")
}
