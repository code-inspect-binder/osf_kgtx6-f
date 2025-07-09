message(".Rprofile loaded: Starting initialization...")

if (interactive() && Sys.getenv("RSTUDIO") == "1") {
  message("Step 1: Confirmed interactive RStudio session. Waiting before next step...")

  later::later(function() {
    message("Step 2: Installing flowR addin...")
    try(rstudioaddinflowr:::install_node_addin(), silent = FALSE)

    message("Step 3: Waiting before setting working directory...")

    later::later(function() {
      try({
        setwd("kgtx6_src/Data Retrieval")
        message("Working directory set to: ", getwd())
      }, silent = FALSE)
    }, delay = 2)

  }, delay = 2)

} else {
  message("interactive() is FALSE. Retrying in 3 seconds...")

  later::later(function() {
    if (interactive() && Sys.getenv("RSTUDIO") == "1") {
      message("Retried and now in RStudio interactive mode.")
      message("Installing flowR addin...")
      try(rstudioaddinflowr:::install_node_addin(), silent = FALSE)

      later::later(function() {
        try({
          setwd("kgtx6_src/Data Retrieval")
          message("Working directory set to: ", getwd())
        }, silent = FALSE)
      }, delay = 2)

    } else {
      message("Still not in interactive RStudio session after retry.")
    }
  }, delay = 3)
}
