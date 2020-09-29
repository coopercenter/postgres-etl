pjm_today <- function() {
  # Retrieve today's observations from the gen_by_fuel series
  library(data.table); library(httr); library(lubridate)
  r <- GET("https://api.pjm.com/api/v1/gen_by_fuel", add_headers("Ocp-Apim-Subscription-Key" = "625845c6fabc4639ab91428486d8d2e2"),
           query = list(rowCount = "40000", startRow = "1", datetime_beginning_ept = 'Today'))
  temp = httr::content(r,"parsed")
  gen_data_today = data.table::rbindlist(temp[['items']])
  # Check for zero-length return record (i.e. no data)
  if (length(gen_data_today) == 0) {
    print("No PJM data available.")
    return(gen_data_today)
  }
  # Eliminate any duplicate records, believe me, it happens...
  data.table::setkey(gen_data_today,"datetime_beginning_ept")
  gen_data_today = unique(gen_data_today)
  # Create date variables
  gen_data_today[,date_time := parse_date_time(datetime_beginning_ept,"Ymd HMS",tz="America/New_York")]   #+hours(5)
  gen_data_today[,`:=`(date = date(date_time), 
                       hour = hour(date_time), year = year(date_time), 
                       month = month(date_time),
                      day = day(date_time))]
  # PJM data is full of noise. Oftern it reports substantial solar generation when it's dark
  # Find the maximum solar generation in the dark (data error)
  # and subtract this nonsense off of generation amounts
  # 
  base = gen_data_today[hour %in% c(0,1,2,3) & fuel_type=='Solar', max(mw)]
  if (length(base) == 0) return(NULL)
  gen_data_today[fuel_type=="Solar",mw := mw - base]
  gen_data_today[fuel_type=='Solar',mw := ifelse(mw<0,0,mw)]
  return(gen_data_today)
}

pjm_generation_by_fuel <- function(gen_data,file,last_date_retreived) {
  library(httr); library(lubridate)
  gen_data  <- data.table(read_feather(file))
# The data are full of noise. Here are some things I have fixed.
# This is almost certainly not an exhaustive list
#  gen_data[order(date),last(date)]
#  gen_data[fuel_type =='Solar' & date == '2017-10-16' & hour >= 18, mw := 0.0]
#  gen_data[fuel_type =='Solar' & date == '2018-10-23' & hour >= 20, mw := 0.0]
#  gen_data[fuel_type =='Solar' & date == '2019-08-29' & hour >= 20, mw := 0.0]
#  gen_data[fuel_type =='Solar' & date == '2019-12-19' & hour == 15, mw := 611.0]
#  gen_data[fuel_type =='Solar' & date == '2019-12-19' & hour == 16, mw := 90.0]
#  gen_data[fuel_type =='Solar' & date == '2019-12-19' & hour > 16, mw := 0.0]
#  gen_data[fuel_type =='Solar' & date == '2019-12-19' & hour <15, mw := mw_solar]
#    gen_data[fuel_type =='Solar' & date == '2019-12-20' & hour <9, mw := 0.0]
#    gen_data[fuel_type =='Solar' & date == '2019-12-20' & hour >= 9, mw := mw_solar]
  yesterday = date(now()) - 1
  last_date_retrieved = gen_data[order(date),last(date)]
#  last_date_retrieved = as.Date('2017-02-16')
#  yesterday = as.Date('2017-02-20')
# Find the dates you need to retrieve
  if (yesterday - last_date_retrieved >1) {
    retrieve_date_range = paste0(last_date_retrieved + 1, "T00:00:00 to ",yesterday,"T23:00:00")
  } else if (yesterday - last_date_retrieved ==1) {
    retrieve_date_range = paste0(yesterday)
  } else return(gen_data)
  print(retrieve_date_range)
  r <- GET("https://api.pjm.com/api/v1/gen_by_fuel", add_headers("Ocp-Apim-Subscription-Key" = "625845c6fabc4639ab91428486d8d2e2"),
           query = list(rowCount = "10000", startRow = "1", datetime_beginning_ept = retrieve_date_range)
  )
  temp = httr::content(r,"parsed")
  gen_data_part = data.table::rbindlist(temp[['items']])
  gen_data_part[,date_time := parse_date_time(datetime_beginning_ept,"Ymd HMS",tz="America/New_York")]   #+hours(5)
  gen_data_part[,`:=`(date = date(date_time), hour = hour(date_time), year = year(date_time), month = month(date_time),
                      day = day(date_time))]
  gen_data_part[,yearmo:= year*100 + month]
  # Fixed some goofy data in earlier data
  # if (as.Date('2018-04-28') %in% unique(gd$date)) {
  #   gen_data_part[fuel_type =='Solar' & date == '2018-04-28' & hour == 18, mw := 250.0]
  #   gen_data_part[fuel_type =='Solar' & date == '2018-04-28' & hour == 19, mw := 70.0]
  #   gen_data_part[fuel_type =='Solar' & date == '2018-04-28' & hour > 19, mw := 0.0]
  #   gen_data_part[fuel_type =='Solar' & date == '2018-04-28' & hour < 18, mw := mw - 19.4]
  # } else if (as.Date('2018-04-29') %in% unique(gd$date)) {
  #   gen_data_part[fuel_type =='Solar' & date == '2018-04-29' & hour <= 1, mw := 24.2]
  # }
  gen_data_part[fuel_type=="Solar",mw_solar := mw]
#  gen_data_part[fuel_type=="Solar",solar_generation_mw := NA]
  #
  # The next code adjusts for nonsense data reporting solar generation in the dark
  # Subtract off the average of dark generation reported in the morning and evening
  #
  for (i in seq_along(unique(gen_data_part$date))) {
    this_date = unique(gen_data_part$date)[i]
    #  cat(format(unique(gen_data_part$date)[i], format="%B %d %Y"),'\n')
    # I have not written code to account for the transition to and from daylight savings time
    # I just punt and treat days with 264 observations as normal.
    # Transition days are not accounted for correctly
    # This needs to be fixed.
    if (nrow(gen_data_part[date == this_date] )==264) {
      early_morning_solar = gen_data_part[date == this_date & 
                                            fuel_type=="Solar" & hour %in% c(0,1,2,3),max(mw_solar)]
      #   cat(early_morning_solar,'\n')
      late_evening_solar = gen_data_part[date == this_date & 
                          fuel_type=="Solar" & hour %in% c(22,23),max(mw_solar)]
          cat(early_morning_solar,'    ',late_evening_solar,'\n')
      if (early_morning_solar>0 | late_evening_solar>0) {
        if (abs(early_morning_solar - late_evening_solar)<0.2) {
          gen_data_part[date == this_date & 
                          fuel_type=="Solar", mw := mw_solar -mean(early_morning_solar,late_evening_solar)]
          gen_data_part[mw < 0 & fuel_type=="Solar", mw := 0.0]
        } else {
          # gen_data_part[date == this_date & 
          #                 fuel_type=="Solar" & mw %in% c(early_morning_solar-0.1,early_morning_solar,early_morning_solar+0.1), 
          #          mw := mw_solar - early_morning_solar]
          # gen_data_part[date == this_date & 
          #                 fuel_type=="Solar" & mw %in% c(late_evening_solar-0.1,late_evening_solar,late_evening_solar+0.1),
          #          mw := mw_solar - late_evening_solar]
          # gen_data_part[mw < 0 & fuel_type=="Solar", mw := 0.0]
          gen_data_part[date == this_date & fuel_type=="Solar",
                        mw := ifelse(mw_solar %in% c(early_morning_solar-0.1,early_morning_solar,early_morning_solar+0.1),
                                     mw_solar - early_morning_solar,
                                     ifelse(mw_solar %in% c(late_evening_solar-0.1,late_evening_solar,late_evening_solar+0.1),
                                            mw_solar - late_evening_solar,
                                            mw_solar - mean(c(early_morning_solar,late_evening_solar))))
                        ]
          gen_data_part[mw < 0 & fuel_type=="Solar", mw := 0.0]
        }
      }
    } else {
      print('Wrong number of rows. Check data.')
    }
    print(gen_data_part[date == unique(gen_data_part$date)[i] & fuel_type=="Solar",.(daily_total = sum(mw))])
  }
  #
  gen_data_part[,solar_generation_mw:=NA]
  gen_data_all = rbindlist(list(gen_data,gen_data_part),use.names=TRUE,fill=FALSE)
  data.table::setkey(gen_data_all,"datetime_beginning_ept")
  gen_data_all <- unique(gen_data_all)
  # Like storing data in feather files for fast reading and writing and Python compatibility
  feather::write_feather(gen_data_all, file)
  return(gen_data_all)
}


solar_hourly <- function() {
  library(httr); library(lubridate)
  # retrieve existing data from local file
  solar_data_local = data.table(read_feather("Data/PJM/solar_gen.ftr"))
# This was to fix an old data error. This function needs more robust error checking.
#  solar_data_local[date=='2020-04-09' & hour==12 & area%in%c("RTO","SOUTH","RFC","MIDATL"),
#               solar_generation_mw:=1575]
  solar_data_local[solar_generation_mw==max(solar_generation_mw),
                 .(date,hour,max(solar_generation_mw))]
  last_date_recorded = solar_data_local[order(date),last(date)]
  date(last_date_recorded)+1
  retrieve_date_range = paste0(date(last_date_recorded+1), "T00:00:00 to ",
                               date(now())-1,"T23:00:00")
  print(retrieve_date_range)
  r <- GET("https://api.pjm.com/api/v1/solar_gen", 
           add_headers("Ocp-Apim-Subscription-Key" = "625845c6fabc4639ab91428486d8d2e2"),
           query = list(rowCount = "50000", startRow = "1", 
                        datetime_beginning_ept = retrieve_date_range))
  temp = content(r,"parsed")
  solar_data_new = rbindlist(temp[['items']])
  solar_data_new[,date_time := parse_date_time(datetime_beginning_ept,"Ymd HMS",
                                              tz="America/New_York")]   #+hours(5)
  solar_data_new[,`:=`(date = date(date_time), hour = hour(date_time),
                      year = year(date_time), month = month(date_time),
                      day = day(date_time))]
solar_data_new[,yearmo:= year*100 + month]
# This data can take a few days to be updated. Weekend values don't appear until Tuesday.
bad_dates = solar_data_new[hour==12&area=="RTO"&solar_generation_mw<=0,date]
print(bad_dates)
solar_gen_data = rbindlist(list(solar_data_local,solar_data_new[date%!in%bad_dates]))
solar_gen_data[solar_generation_mw==max(solar_generation_mw),
               .(date,hour,max(solar_generation_mw))]
solar_gen_data[order(date),.(last_day = last(date))]
solar_gen_data[,solar_generation_mw:=ifelse(solar_generation_mw<0,0,solar_generation_mw)] 
write_feather(solar_gen_data,"Data/PJM/solar_gen.ftr")
return solar_gen_data
}