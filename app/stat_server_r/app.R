# rserver
# uses plumber to create a simple REST API
# and fhircrackr to query a FHIR server and return the result
library(fhircrackr)


#* @apiTitle rserver API

#* Return a greeting message.
#* @get /
function(){
  list(message = "Hello from rserver!")
}

#* Health check endpoint
#* @get /health
#* @serializer json
function(){
  # Service is healthy if it can start and respond (regardless of dependencies)
  service_status <- "healthy"
  
  # Test HAPI FHIR server connection as a dependency check
  hapi_connected <- FALSE
  hapi_url <- "http://hapi:8080/fhir"
  hapi_error <- NULL
  
  tryCatch({
    # Try to connect to HAPI server
    request <- fhir_url(url = hapi_url, resource = "metadata")
    response <- httr::GET(request)
    hapi_connected <- httr::status_code(response) == 200
  }, error = function(e) {
    hapi_error <- as.character(e)
  })
  
  return(list(
    status = service_status,
    service = "stat_server_r",
    dependencies = list(
      hapi_fhir = list(
        connected = hapi_connected,
        url = hapi_url,
        error = if(!hapi_connected && !is.null(hapi_error)) hapi_error else NULL
      )
    )
  ))
}


# let's use fhircrackr to do a basic return of a dataframe of patients
#* Return a dataframe of patients.
#* @get /patients
#* @serializer json
function(){    
  ## Example from https://github.com/POLAR-fhiR/fhircrackr
  ## More vignettes in https://cran.r-project.org/web/packages/fhircrackr/vignettes
  request <- fhir_url(url = "http://hapi:8080/fhir", resource = "Patient")
  patient_bundles <- fhir_search(request = request, max_bundles = 2, verbose = 0)

  #define table_description
  table_description <- fhir_table_description(
      resource = "Patient",
      cols     = c(
          id        = "id",
          use_name    = "name/use",
          given_name  = "name/given",
          family_name = "name/family",
          gender      = "gender",
          birthday    = "birthDate"
      ),
      sep           = " ~ ",
      brackets      = c("<<", ">>"),
      rm_empty_cols = FALSE,
      format        = 'compact',
      keep_attr     = FALSE
  )

  patients <- fhir_crack(bundles = patient_bundles, design = table_description, verbose = 0)
  
  print(str(patients))

  # Return the dataframe as JSON
  return(patients)
}