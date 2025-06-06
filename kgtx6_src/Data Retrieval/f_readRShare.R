require(foreign)
require(data.table)
require(dplyr)


#' Title f_readRShare
#'
#' @param datadir full name of the directory where the unzipped SHARE data are stored
#' @param waves the waves from which retrieve data
#' @param modules the modules (within each wave) from which retrieve data
#' @param variables.in.modules the names of the variables (from each module) to keep from each module
#' @param modules.wX the across-wave modules to retrieve
#' @param countries the countries to include, if not specified, all the countries are retrieved
#' @param keep.wX.included.only logical, if TRUE only the data from participants included in the retrieved data (waves/modules) are kept in the in the cross-wave modules
#' @param load.xt logical indicating if the xt module should be retrieved
#' @param load.iv logical indicating if the interviewers' data (iv module) should be retrieved
#' @param keep.xt.included.only logical, if TRUE only the data from participants included in the retrieved data (waves/modules) are kept in the in the xt module
#' @param remove_sl_w3 logical, if TRUE the sl_ prefix in removed from variables in wave 3
#' @param keep.module.name.in.vars logical, if TRUE, the names of the modules from which the variables were exported appears at the end of the variable name, preceeded by three dots
#' @param verbose logical, if TRUE the progress of the data import is written on screen
#' @return
#' @export
#'
#' @examples
#' 
f_readRShare <-
  function(datadir,
           waves = c(1),
           modules = c("dn"),
           variables.in.modules = NULL,
           modules.wX = NULL,
           countries = NULL,
           keep.wX.included.only = TRUE,
           load.xt = FALSE,
           load.iv = FALSE,
           #transform to load.int -: for interviewers form
           keep.xt.included.only = TRUE,
           remove_sl_w3 = TRUE, 
           keep.module.name.in.vars = TRUE, 
           verbose = TRUE) {
    #remove_sl_w3 = TRUE removes the sl prefix in wave 3 for variable matching
    
    
    #initialization of the data frames that will can be the output of the function
    df.w <- df.xt <- df.iv <- df.wX <- NULL
    #initialization of the vectors with the names of the modueles/variables
    names.modules.df.w <- names.var.modules <- NULL
    
    
    #initialization
    #ifelse(length(waves)>0 & length(modules)>0, names.var.modules <- vector("list", num.waves), NULL)
    
    
    if (!file.exists(datadir))
      stop("The directory that you specified does not exist.")
    
    
    #    if (any(c("xt", "cv") %in% modules))
    if (any(c("xt", "int") %in% modules))
      stop(
        "xt and int modules can be uploaded setting the load.xt or load.int parameters to TRUE, remove them from the list of modules."
      )
    
    
    num.waves <- length(waves) #number of selected waves
    num.modules <-
      length(modules) #number of selected modules within waves
    num.modules.wX <-
      length(modules.wX) #number of selected cross-wave modules
    
    if (length(num.waves) == 0 &
        length(num.modules) > 0)
      stop("You should specify at least one wave from which the modules will be extracted.")
    
    if (length(num.waves) > 0 &
        length(num.modules) == 0)
      stop("You should specify at least one module to extract from each wave.")
    
    
    #list of the directories included in the datadir directory
    my.dirlist <- list.dirs(path = datadir)
    if (length(my.dirlist) == 0)
      stop("There are no subdirectories in the directory that you selected.")
    
    #initialization of the vectors/lists that will contain the names of the files/directories
    name.dir.wave <-
      character(num.waves) #names of the directories of the waves
    name.file.module  <-
      vector("list", num.waves) #a list, each element refers to a wave, each element of the list to a module file
    name.file.module.wX <-
      character(num.modules.wX) #stores the names of the wX modules
    
    
    my.data <- NULL
    
    #retrieve the names of the files to read for the modulues within waves
    if (num.waves > 0) {
      #retrieve the names of the directories and of the files that will be read, for the waves
      for (i.wave in 1:num.waves) {
        name.dir.wave[i.wave] <-
          my.dirlist[grep(paste0("w", waves[i.wave], "_"), my.dirlist)]
        my.filelist <- list.files(path = name.dir.wave[i.wave])
        for (i.mod in 1:num.modules) {
          tmp <-
            my.filelist[grep(paste0("_", modules[i.mod], ".dta"), my.filelist)]
          name.file.module[[i.wave]][i.mod] <-
            ifelse(length(tmp) > 0, tmp, NA) #sets to NA the name of the files that are not present in the wave
        
        }#end for i.mod
        
        #check if for some wave all the modules are not available: stop in this case
        if(all(is.na(name.file.module[[i.wave]]))) stop("None of the selected modules are present in wave ", i.wave, ". Remove the wave or change the list of selected modules.")
          
      }#end for i.wave
      
      
      df <- vector("list", num.waves)
      #names of the available variable, by wave/module
      var.names.wave.module <-
        vector("list", num.waves) #not used yet
      
    }#end if num.waves
    
    
    #retrieve the names of the files and the load the data from the wX modules
    my.filelist.all <- list.files(datadir, recursive = TRUE)
    
    
    if (num.modules.wX > 0) {
      for (i.mod in 1:num.modules.wX) {
        tmp <-
          my.filelist.all[grep(paste0(modules.wX[i.mod], ".dta"), my.filelist.all)]
        
        if(length(tmp)==0) stop("The cross-wave module ", modules.wX[i.mod], " specified in the modules.wX variable cannot be found among your files. Check the name.")
        
        name.file.module.wX[i.mod] <-
          ifelse(length(tmp) > 0, paste(datadir, tmp, sep = "/"), NA) #sets to NA the name of the files that are not present in the wave
      }#end for i.mod
    }#end if num.modules.wX
    
    
    #load the data for the waves
    
    
    
    #retrieve the data
    if (num.waves > 0) {
      #creates a list with the names of the variables included for each module
      names.var.modules <- vector("list", num.waves)
      names(names.var.modules) <- paste("Wave", waves)
      
      
      
      if(verbose) cat("Reading data from the selected modules within the waves. \n")
      
      #if the variables to export from each module are specified, add mergeid (and country, if country is used for subsetting) to all modules
      if (!is.null(variables.in.modules)) {
        if (!is.null(countries))
          variables.in.modules <-
            lapply(variables.in.modules, function(x)
              unique(c("mergeid", "country", x)))
        else
          variables.in.modules <-
            lapply(variables.in.modules, function(x)
              unique(c("mergeid", x)))
      }
      
      
      for (i.wave in 1:num.waves) {
        #Wave = paste("Wave", waves[i.wave])
        #df[[i.wave]] <- data.frame(Wave= paste("Wave", waves[i.wave]))
        if(verbose) cat("\t Reading data from wave ",
            waves[i.wave],
            "\n")
        
      
        #initialize the list with the names of the variables -
        #needed to have the correct number of elements also if the modules are not found within the wave
        names.var.modules[[i.wave]] <- vector("list", num.modules)
        
        k <- 0  #counter of the number of read modules in the wave
        for (i.mod in 1:num.modules) {
          if (is.na(name.file.module[[i.wave]][i.mod]))
            next  #skip the reading of the file if the module is not present in the wave
          k <- k + 1
          file.name <-
            paste(name.dir.wave[i.wave], name.file.module[[i.wave]][i.mod], sep = "/")
          
          
          #read data
          new.data <- read.dta(file.name)
          #adding a suffix with the name of the module at the end of the variable names,
          #maintaining only the mergeid as in its original form for matching
          
          #filter data from countries not included in the list of countries, if provided
          if (!is.null(countries)) {
            new.data <- filter(new.data, country %in% countries)
            new.data <- droplevels(new.data)
          } #end is.null(countries)
          
          
          #remove the sl prefix from the names of the variables in wave 3 if selected by the user
          if (remove_sl_w3 == TRUE & waves[i.wave] == 3) {
            which.start.sl <- which(substr(names(new.data), 1, 3) == "sl_")
            
            
            if (length(which.start.sl) > 0) {
              warning(
                paste(
                  "\n Note that the variables ",
                  names(new.data)[which.start.sl],
                  " from module ",
                  modules[i.mod],
                  " in wave 3 were renamed omitting the sl_ prefix and merged with the variables with the same name from the non-SHARELIFE interviews.
                          Make sure that their content is comparable with those for the non-SHARELIFE interviews. If not use remove_sl_w3 = FALSE. \n\n"
                )
              )
              
              
              
              names(new.data)[which.start.sl] <-
                sub("...", "", names(new.data)[which.start.sl])
            }
            #gsub("sl_", replacement, x, ignore.case = FALSE, perl = FALSE,
            #    fixed = FALSE, useBytes = FALSE)
            
            
          }# and if(remove_sl_w3)
          
          
          #if variables.in.modules[[i.mod]] includes mergeid/country only than retrieve all the data from the module, as the variables were not selected by the user
          #if(!is.null(variables.in.modules)) { 
          if( length(variables.in.modules[[i.mod]])>ifelse(!is.null(countries), 2, 1)   ) { 
          
            #check which of the specified variables are actually present in the downloaded data
            which.var.ok <-
              variables.in.modules[[i.mod]] %in% names(new.data)
            
            if (any(!which.var.ok))
              warning(
                "Some of the variables that you selected from module ",
                modules[i.mod],
                " in wave ",
                waves[i.wave],
                " are not included in the data set. \n The variables that could not be imported were: ",
                variables.in.modules[[i.mod]][!which.var.ok],
                ". Please check these variables, the other variables were exported. \n"
              )
            new.data <-
              select(new.data, variables.in.modules[[i.mod]][which.var.ok])
          }
          
          names(new.data)[-1] <-
            paste(names(new.data)[-1], modules[i.mod], sep = "...")
          
          #names.var.modules[[i.wave]][[i.mod]] <- names(new.data)
          
          
          #not saving mergeid and (if selected) country in the names of the variables downloaded from each module, the [-1] removes the mergeid
         if (!is.null(countries)) names.var.modules[[i.wave]][[i.mod]] <- names(select(new.data, !starts_with(c("country") )))[-1] else
            names.var.modules[[i.wave]][[i.mod]] <- names(new.data)[-1]
          
          
         #changed May10
           if(keep.module.name.in.vars==FALSE) {
            names.var.modules[[i.wave]][[i.mod]] <-  sapply(strsplit(  names.var.modules[[i.wave]][[i.mod]], split = "\\.\\.\\."), function(x) x[1])
           }
          
          if (k == 1)
            
            df[[i.wave]] <- new.data
          else{
            if (modules[i.mod] != "cv_r")
              
              df[[i.wave]] <-
                #cbind.data.frame(df[[i.wave]], read.dta(file.name)) #assuming that the order is always the same!
                full_join(df[[i.wave]],
                          new.data,
                          suffix = c("", paste0(".", modules[i.mod])),
                          by = "mergeid")
            else
              
              df[[i.wave]] <-
                #cbind.data.frame(df[[i.wave]], read.dta(file.name)) #assuming that the order is always the same!
                left_join(df[[i.wave]],
                          new.data,
                          suffix = c("", paste0(".", modules[i.mod])),
                          by = "mergeid")
            
          }#end else
        }#end for i.mod
        #adding the wave information
        df[[i.wave]] <-
          cbind.data.frame(Wave = paste("Wave", waves[i.wave]), df[[i.wave]])
        
        
        #check the length of the list with the modules, otherwise add NULL lists at the end
        #if(length(names.var.modules[[i.wave]]) < num.modules )
        # for(ii in (length(names.var.modules[[i.wave]])+1) : num.modules)
        #   names.var.modules[[i.wave]][[ii]] <- NULL
        
        
        
        names(names.var.modules[[i.wave]]) <- modules
        
      }#end for i.wave
      
      
      #names
      
      df.w <- as.data.frame(data.table::rbindlist(df, fill = TRUE))
      #https://stackoverflow.com/questions/3402371/combine-two-data-frames-by-rows-rbind-when-they-have-different-sets-of-columns
      
      #derive the module from which the variables are retrieved, note that the variables that are not common to all the
      #waves are set at the end of the data frame when the rbindlist is called (fill=TRUE)
      which.module <-
        factor(sapply(strsplit(names(df.w)[-c(1:2)], split = "\\.\\.\\."), tail, 1), levels =
                 modules)
      
      df.w <- df.w[, c(1, 2, order(which.module) + 2)]
      
      
      non.module <- 2 #number of non-module specific variables
      
      #remove the multiple entries about countries for the various modules, keep the variable as appearing in the first downloaded module
      
      if (!is.null(countries)) {
        which.first.country <- grep("country", names(df.w))[1]
        
        df.w$country <- df.w[, which.first.country]
        df.w <- select(df.w,!starts_with("country..."))
        
        df.w <- df.w[, c(1:2, ncol(df.w), 3:(ncol(df.w) - 1))]
        
        non.module <- non.module + 1 
      }
      
      
      #names.var.modules.df.w <- c(rep("general", non.module), as.character(which.module)) # names of the modules from which the data in df.w were exported
      names.modules.df.w <- c(rep("general", non.module), sapply(strsplit(names(df.w)[-c(1:non.module)], split = "\\.\\.\\."), tail, 1))
      
      
      
      
      #remove the indication of the module from the variable names, if requested
      if(keep.module.name.in.vars==FALSE) {
        names(df.w) <-   sapply(strsplit(names(df.w), split = "\\.\\.\\."), function(x) x[1])
      #  sapply(lapply( names.var.modules.df.w.bywave, function(x) lapply(x, gsub("\\.\\.\\..*","","asdas...dn")), function(x) x[1])
      }
      
       
      
      if(verbose) 
        cat(
        "Data from",
        nrow(df.w),
        "interviews from ",
        length(unique(df.w$mergeid)), 
       " unique participants and ",
        ncol(df.w),
        "variables were imported and are stored in the df.w data frame. \n"
      )
      
    }#end if num.waves>0
    
    #load the selected wX modules
    
    
    if (num.modules.wX > 0) {
      #load the data for the waves
      df.wX <- vector("list", num.modules.wX)
      
     if(verbose) cat("\nReading the across wave modules (wX) that will be stored as data frame elements in the df.wX list. \n")
      
      
      for (i.mod in 1:num.modules.wX) {
        
        if (is.na(name.file.module.wX[i.mod]))
          next  #skip the reading of the file if the module is not present in the wave
        #k <- k + 1
        #file.name <-
        #  paste(name.dir.wave[i.wave], name.file.module[[i.wave]][i.mod], sep = "/")
        
        #if (k == 1)
        
        df.wX[[i.mod]] <- read.dta(name.file.module.wX[i.mod])
        
        
        
        
        #filter data from countries not included in the list of countries, if provided
        if (!is.null(countries)) {
          #check if the module contains the variable name country, otherwise skip
          if ("country" %in% names(df.wX[[i.mod]])) {
            df.wX[[i.mod]] <- filter(df.wX[[i.mod]], country %in% countries)
            df.wX[[i.mod]] <- droplevels(df.wX[[i.mod]])
          }
          else
            (warning(
              paste(
                "The module",
                modules.wX,
                "does not include a country variable, therefore the participants were not filtered by country in this module.\n"
              )
            ))
          
          
        }#end if (!is.null(countries))
        
        #else
        #  df.wX[[i.mod]] <-
        #  cbind.data.frame(df[[i.mod]], read.dta(file.name))
        
        
        
        
        #if the user selected to keep only the participants that are included in the selected waves/modules
        if (keep.wX.included.only & num.waves > 0){
          df.wX[[i.mod]] <-
            filter( df.wX[[i.mod]] , is.element( df.wX[[i.mod]]$mergeid, df.w$mergeid))
        }
        
      
        if(verbose) cat(
          "\t Data from the cross-wave module",  modules.wX[i.mod], "include",
          ncol(df.wX[[i.mod]]),
          "variables and ", nrow(df.wX[[i.mod]]), "interviews \n")
        
        
        
      }#end for i.mod
      #adding the wave information
      
      names(df.wX) <- modules.wX
      
    }#end if num.modules.wX
    
    
    
    #vector with the list of unique names of the dataframes retrieve from different waves
    #allNms <- unique(unlist(lapply(df, names)))
    
    
    #df.w <- do.call(rbind,
    #        c(lapply(df,
    #                 function(x) data.frame(c(x, sapply(setdiff(allNms, names(x)),
    #                                                    function(y) NA)))),
    #         make.row.names=FALSE))
    
    
    
    
    
    if (load.xt) {
     if(verbose) cat("\n Reading the exit of life questionnaire data\n")
      name.file <-
        paste(datadir, my.filelist.all[grep("_xt.dta", my.filelist.all)], sep =
                "/")
      
      
      df.xt <- vector("list", length(name.file))
      
      for (i.wave in 1:length(name.file)) {
        df.xt[[i.wave]] <- read.dta(name.file[i.wave])
        
        if (!is.null(countries)) {
          #check if the module contains the variable name country, otherwise skip
          if ("country" %in% names(df.xt[[i.wave]])) {
            df.xt[[i.wave]] <- filter(df.xt[[i.wave]], country %in% countries)
            df.xt[[i.wave]] <- droplevels(df.xt[[i.wave]])
          }
        }#end if countries
        
        #adding the wave information
        df.xt[[i.wave]] <-
          cbind.data.frame(Wave = paste("Wave", unlist(strsplit(
            names(select(df.xt[[i.wave]], starts_with("hhid")))[1], "hhid"
          ))[2]), df.xt[[i.wave]])
        
      }
      
      
      df.xt <- data.table::rbindlist(df.xt, fill = TRUE)
      
      if (keep.xt.included.only & num.waves > 0){
        df.xt <-
        filter(df.xt, is.element(df.xt$mergeid, df.w$mergeid))
        }#end if (keep.xt.included.only & num.waves > 0)
      
      
      if(verbose) cat(
        "\t Data from the exit questionnaries include",
        nrow(df.xt),
        "interviews from ",  
        length(unique(df.xt$mergeid)),
        "unique participants and",
        ncol(df.xt),
        "variables and are stored in the df.xt data frame. \n"
      )
      
      
      
    }#end if(load.xt)
    
    #output: df.xt: data frame with the information about exit questionnaires
    
    
    
    if (load.iv) {
     if(verbose) cat("\n Reading the interviewers' data\n")
      name.file <-
        paste(datadir, my.filelist.all[grep("_iv.dta", my.filelist.all)], sep =
                "/")
      
      
      df.iv <- vector("list", length(name.file))
      
      for (i.wave in 1:length(name.file)) {
        df.iv[[i.wave]] <- read.dta(name.file[i.wave])
        
        if (!is.null(countries)) {
          #check if the module contains the variable name country, otherwise skip
          if ("country" %in% names(df.iv[[i.wave]])) {
            df.iv[[i.wave]] <- filter(df.iv[[i.wave]], country %in% countries)
            df.iv[[i.wave]] <- droplevels(df.iv[[i.wave]])
          }
        }#end if countries
        
        
        #adding the wave information
        df.iv[[i.wave]] <-
          cbind.data.frame(Wave = paste("Wave", unlist(strsplit(
            names(select(df.iv[[i.wave]], starts_with("hhid")))[1], "hhid"
          ))[2]), df.iv[[i.wave]])
        
      }#end for i.wave
      
      
      df.iv <- data.table::rbindlist(df.iv, fill = TRUE)
      
      #keep only the records that appear in the outputted data
      #if (keep.iv.included.only)
      #  df.iv <- filter(df.iv, is.element(df.iv$mergeid, df.w$mergeid))
      
      
      
    if(verbose)  cat(
        "\t Data from the interviewers observations questionnaries include",
        ncol(df.iv),
        "variables and are stored in the df.iv data frame. Note that only the countries/waves that you selected are included in this dataset.\n"
      )
      
      
      
    }#end if(load.cv)
    
    #output: df.iv: data frame with the information about exit questionnaires
    
    
    #outputs:
    #df.w: data frame (data.table) including the data exported from selected modules from separate waves
    #names.modules.df: vector with the names of the modules from which the variables included in the df.w data were exported
    #df.xt: data frame including the data exported from exit questionnaires
    #df.iv: data frame including the data exported from cover screens
    #df.wX: list of data frames including the data exported from modules that included across-wave information (wX)
    #names.var.modules.df.w.bywave: list with names of the variables exported from each of the module; each element of the list refers to a wave
    #params: a list with the parameters used to subset the SHARE data
    
    return(
      list(
        df.w = df.w,
        names.modules.df.w = names.modules.df.w, 
        df.wX = df.wX,
        df.xt = df.xt,
        df.iv = df.iv,
        names.var.modules.df.w.bywave = names.var.modules, 
        params = list(waves = waves,
                      modules = modules,
                      variables.in.modules = variables.in.modules,
                      modules.wX = modules.wX,
                      countries = countries,
                      keep.wX.included.only = keep.wX.included.only,
                      load.xt = load.xt,
                      load.iv = load.iv,
                      keep.xt.included.only = keep.xt.included.only,
                      remove_sl_w3 = remove_sl_w3, 
                      keep.module.name.in.vars = keep.module.name.in.vars)
      )
    )
    
    
  }# end function




#' Title f_get_df_variable_by_wave
#'
#' @param share.data an object obtained with the  f_readRShare function with non-null df.w data frame
#' @return df.var.by.wave - data.frame with variables from df.w by row, selected waves by column; the first column indicates the module from which the variables were selected, the other columns if the variable was present in each of the waves (TRUE/FALSE)
#' @export
#'
#' @examples


f_get_df_variable_by_wave <- function(share.data){

#from which wave do the variables come from

  if (!is.null(share.data$df.w) == 0) stop("This function can be used if some modules from different waves were downloaded (and the df.w matrix is present))")
    
  
  
  names.var.modules.merged <-
    lapply(1:length(share.data$params$modules), function(i)
      unique(unlist(
        lapply(share.data$names.var.modules.df.w.bywave, function(x)
          x[[i]][-1])
      )))
  names(names.var.modules.merged) <- modules
  
  
  df.var.by.wave <-
    matrix(NA,
           ncol = length(share.data$params$waves),
           nrow = ncol(share.data$df.w))
  dimnames(df.var.by.wave)[[1]] <- names(share.data$df.w)
  dimnames(df.var.by.wave)[[2]] <-
    paste("Wave", share.data$params$waves)
  
  
  #number of general variables - 3 or 2 depending on whether the country was used as a subsetting variable or not
  num.general <- ifelse(!is.null(share.data$params$countries), 3, 2)
  
  
  for (i.wave in 1:length(share.data$names.var.modules.df.w.bywave)) {
    #  for(i.mod in 1:length(names.var.modules.merged[[i.wave]])){
    #T>F if it comes from that specific wave or not
    
    df.var.by.wave[, i.wave] <-
      is.element(names(share.data$df.w),
                 unlist(share.data$names.var.modules.df.w.bywave[[i.wave]]))
    
    
  }#end for

df.var.by.wave[1:num.general,] <- TRUE
df.var.by.wave <- cbind.data.frame(Module=share.data$names.modules.df.w, df.var.by.wave)

return(df.var.by.wave)


}#end f_obtain_matrix_variable_by_wave


#names.var.modules.merged <- lapply(1:num.modules, function(i) unique(unlist(lapply(names.var.modules, function(x) x[i]))))