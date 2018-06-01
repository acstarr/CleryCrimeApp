#Adam Starr
#Code to turn CSV of Clery Crime data and weather data into tables to load into Database. 

library(readr)
#library(dplyr)
library(plyr)
library(RJSONIO)

log <- read_csv("~/Dropbox/Senior Year/Semester 2/CSCI133/CecilSafe/data/log.csv", 
                col_types = cols(dateEnded = col_date(format = "%Y-%m-%d"), 
                                 dateReported = col_date(format = "%Y-%m-%d"), 
                                 dateStarted = col_date(format = "%Y-%m-%d"), 
                                 timeEnded = col_time(format = "%H:%M:%S"), 
                                 timeReported = col_time(format = "%H:%M:%S"), 
                                 timeStarted = col_time(format = "%H:%M:%S")))
log$incidentID <- seq.int(nrow(log))


#clean dispositions
log$disposition2 <- NA
for (i in 1:length(log$disposition)){
  disps <- append(strsplit(log$disposition[i],",")[[1]],c(NA))
  log$disposition[i] <- disps[1]
  log$disposition2[i] <- disps[2]
}

#get dispostions
disposition <- log$disposition
disposition2 <- log$disposition2
disposition <- c(disposition,disposition2)
disposition <- unique(disposition)
disposition <- disposition[!is.na(disposition)]
disposition <- as.data.frame(disposition)
disposition$dispID <- seq.int(nrow(disposition))

disposition2 <- disposition$disposition
disposition2 <- data.frame(disposition2)
disposition2$disp2ID <- disposition$dispID


#join on dispositons
log <-join(log,disposition)
log <-join(log,disposition2)

#make dispositions for export
disposition1 <- data.frame(incidentID=log$incidentID, dispID=log$dispID)
disposition2 <- data.frame(incidentID=log$incidentID, dispID=log$disp2ID)
status <- rbind(disposition1,disposition2)
status <- na.omit(status)

#get locs merge addresses and add ID
locs <- unique(log[,c("college","location","address")])
locs <- ddply(locs, .(college, location), summarize, address = address[xor(!is.na(address),all(is.na(address)))])
locs$locID <- seq.int(nrow(locs))

#lat lon 
geocodeAdddress <- function(address) {
  require(RJSONIO)
  url <- "http://maps.google.com/maps/api/geocode/json?address="
  url <- URLencode(paste(url, address," 91711", "&sensor=false", sep = ""))
  x <- fromJSON(url, simplify = FALSE)
  if (x$status == "OK") {
    out <- c(x$results[[1]]$geometry$location$lng,
             x$results[[1]]$geometry$location$lat)
  } else {
    out <- NA
  }
  Sys.sleep(0.2)  # API only allows 5 requests per second
  out
}

#add lat lon
locs$latitude <- NA
locs$longitude <- NA
for (i in 1:length(locs$address)){
    if(!is.na(locs[i,"address"])){
    ll <- geocodeAdddress(locs[i,"address"])
    locs[i,"longitude"] <- ll[1]
    locs[i,"latitude"] <- ll[2]
  }
}

#join id numbers and lat lng to full data
log <- log[ , !(names(log) %in% c("address"))]
log <-join(log,locs, by=c("college","location"),type="inner")


#get campus bulding and is on
campus <- locs[locs$location %in% c("POM", "PTZ", "HMC", "SCR", "CMC", "CGU", "KGI", "CUC"), c("locID","college","location")]
campus <- rename(campus, replace = c( locID="campusID"))
building <- locs[! (locs$location %in% c("POM", "PTZs", "HMC", "SCR", "CMC", "CGU", "KGI", "CUC")), c("locID","college","location")]
building <- rename(building, replace = c(locID="buildingID"))
isOn <- join(building,campus, by="college",type="inner")[,c("buildingID","campusID")]

#format campus
campus$isGrad <- NA #TODO KGI CGU
campus <- campus[,c("campusID","college","location","isGrad")]
campus$isUndergrad <- NA #DOTO NOT KGI CGU CUC
campus$numStudents <- NA
campus$acreage <- NA


#format building
building$isDorm <- NA
building <- building[,c("buildingID","college","location","isDorm")]
building$isAccedemic <- NA 
building$isDining <- NA
building$isSports <- NA

#final format locations
locations <- locs[,c("locID", "location", "address", "latitude", "longitude")]


#get info on codes
violations <- unique(log[,c("code","crime","description")])
violations$violationID <- seq.int(nrow(crime))

log <-join(log,violations, by=c("crime","description"))

#extract incident attributes
incidents <-log[,c("incidentID", "violationID", "caseNumber","dateReported", "timeReported","dateStarted", "timeStarted", "dateEnded", "timeEnded","locID")]


days <- unique(c(log$dateEnded,log$dateReported,log$dateStarted))
days <- data.frame(DATE=days[! is.na(days)])

weather_data_temp <- read_csv("~/Dropbox/Senior Year/Semester 2/CSCI133/CecilSafe/data/weather_data_temp.csv", 
                              col_types = cols(DATE = col_date(format = "%Y-%m-%d")))
weather_data_precip <- read_csv("~/Dropbox/Senior Year/Semester 2/CSCI133/CecilSafe/data/weather_data_precip.csv", 
                              col_types = cols(DATE = col_date(format = "%Y-%m-%d")))
days <- join(days,weather_data_precip)
days <- join(days,weather_data_temp)






write.csv(days, "/Users/Adam/Dropbox/Senior Year/Semester 2/CSCI133/CecilSafe/data/processsed/Days.csv", na = "Null", row.names=FALSE)
write.csv(campus, "/Users/Adam/Dropbox/Senior Year/Semester 2/CSCI133/CecilSafe/data/processsed/Campus.csv", na = "Null", row.names=FALSE)
write.csv(building, "/Users/Adam/Dropbox/Senior Year/Semester 2/CSCI133/CecilSafe/data/processsed/Building.csv", na = "Null", row.names=FALSE)
write.csv(isOn, "/Users/Adam/Dropbox/Senior Year/Semester 2/CSCI133/CecilSafe/data/processsed/IsOn.csv", na = "Null", row.names=FALSE)
write.csv(incidents, "/Users/Adam/Dropbox/Senior Year/Semester 2/CSCI133/CecilSafe/data/processsed/Incidents.csv", na = "Null", row.names=FALSE)
write.csv(locations, "/Users/Adam/Dropbox/Senior Year/Semester 2/CSCI133/CecilSafe/data/processsed/Locations.csv", na = "Null", row.names=FALSE)
write.csv(disposition, "/Users/Adam/Dropbox/Senior Year/Semester 2/CSCI133/CecilSafe/data/processsed/Dispositions.csv", na = "Null", row.names=FALSE)
write.csv(status, "/Users/Adam/Dropbox/Senior Year/Semester 2/CSCI133/CecilSafe/data/processsed/Status.csv", na = "Null", row.names=FALSE)
write.csv(violations, "/Users/Adam/Dropbox/Senior Year/Semester 2/CSCI133/CecilSafe/data/processsed/Violations.csv", na = "Null", row.names=FALSE)
write.csv(log, "/Users/Adam/Dropbox/Senior Year/Semester 2/CSCI133/CecilSafe/data/processsed/Log.csv", na = "Null", row.names=FALSE)
