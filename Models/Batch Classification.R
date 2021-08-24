
library(jsonlite)
library(dplyr)


is_outlier <- function(x) {
  iqr <- IQR(x)
  q <- quantile(x, c(.05, .95))
  x < q[1]-1.5*iqr | x > q[2]+1.5*iqr
}


Flag_lump_events <- function(output){
  
  GTS_List <- output %>% 
    distinct(GTS_Category) 
  
  Init_flag <- 0
  
  for(i in 1:nrow(GTS_List)){
    partition <- output %>% filter(GTS_Category == GTS_List[i,])
    partition$lump <- is_outlier(partition$.value)
    
    
    
    if(Init_flag == 0){
      Init_flag <- 1
      base <- partition
    } else {
      base <- rbind(base, partition)
    }
  }
  
  return(base)
}

Analyze_Lump <- function(Dataset, Model, Category){
  
  Lump <- Model %>% 
    filter(GTS_Category == Category) %>%
    filter(lump == "TRUE") %>% select(GTS_Category, .index, .value)
  
  dates <- Lump %>% select(.index)
  
  #Double click on batch dates from full data (This is hardcoded)
  lump_double_click <- Dataset %>% 
    filter(CreatedDateKey %in% as.Date(dates$.index)) %>% 
    rename(GTS_Category = TicketType) %>% 
    filter(GTS_Category == Category)

  
  
  base <- list()
  for(i in 1:nrow(dates)){
    Results <- list() 
    
    partition <- lump_double_click %>% 
      filter(CreatedDateKey == dates[i,])
    
    Results[[1]] <- partition %>% 
      group_by(CreatedDateKey, ProductCategory) %>% 
      summarize(n = n()) %>% arrange(desc(n)) %>% mutate(Key = "ProductCategory") %>%
      rename(Value = ProductCategory)
    
    Results[[2]] <- partition %>% 
      group_by(CreatedDateKey, ProductName) %>% 
      summarize(n = n()) %>% arrange(desc(n)) %>% mutate(Key = "ProductName") %>%
      rename(Value = ProductName)
    
    Results[[3]] <- partition %>% 
      group_by(CreatedDateKey, RegionName) %>%
      summarize(n = n()) %>% arrange(desc(n)) %>% mutate(Key = "RegionName") %>%
      rename(Value = RegionName)
    
    Results[[4]] <- partition %>% 
      group_by(CreatedDateKey, RegionType) %>%
      summarize(n = n()) %>% arrange(desc(n)) %>% mutate(Key = "RegionType") %>%
      rename(Value = RegionType)
    
    Results[[5]] <- partition %>% 
      group_by(CreatedDateKey, CustomerSegment) %>%
      summarize(n = n()) %>% arrange(desc(n)) %>% mutate(Key = "CustomerSegment")  %>%
      rename(Value = CustomerSegment)
    
    Results[[6]] <- partition %>% 
      group_by(CreatedDateKey, CustomerSubscriptionId) %>%
      summarize(n = n()) %>% arrange(desc(n)) %>% mutate(Key = "CustomerSubscriptionId")  %>%
      rename(Value = CustomerSubscriptionId)
    
    base[[i]] <- Results
      
    
  }
  
  return(base)}


# Model outputs
QuotaType_output <- read.csv("time_series_QuotaType.csv")
TicketType_output <- read.csv("time_series_TicketType.csv")

# * Tickets Data ----
quota_tickets <- read.csv("01_RDQuota/00_quota_tickets_6_mo.csv") %>% 
  mutate(CreatedDateKey = as.Date(paste0(substr(CreatedDateKey, 1, 4), "-", 
                                         substr(CreatedDateKey, 5, 6), "-", 
                                         substr(CreatedDateKey, 7, 8))))

#Flag Model outputs for batch
TicketType_output_flagged <- Flag_lump_events(TicketType_output)


GTS_List <- TicketType_output_flagged %>% distinct(GTS_Category)


full_df <- list()

for(j in GTS_List$GTS_Category){
  print(j)
  #Lump Analysis
  Results <- Analyze_Lump(quota_tickets, TicketType_output_flagged, j)
  
  
  part <- list()
  for(i in 1:length(Results)){
    print(i)
    
    part[[i]] <- do.call(rbind, Results[[i]])
    
  }
  Results_df <- do.call(rbind, part)
  Results_df  <- Results_df %>% mutate(GTS_Category = j)
  full_df[[j]] <- Results_df 
}

Final_Analytics_Output <- do.call(rbind, full_df)



write.csv(Final_Analytics_Output, "Final_Analytics_Output.csv")
write.csv(Final_Analytics_Output %>% ungroup %>% distinct(GTS_Category), "GTS_List.csv")
write.csv(TicketType_output_flagged, "TicketType_output_flagged.csv", row.names = FALSE)



