pacman::p_load(
  "shinydashboard",
  "highcharter"
)

ui <- dashboardPage(
  dashboardHeader(),
  dashboardSidebar(
    sidebarMenu(
      menuItem("Datasets", tabName = "datasets"),
      menuItem("Graphs", tabName = "graphs", icon = icon("chart-bar")),
      menuItem("Text", tabName = "text", icon = icon("chart-line")),
      
      menuItem(
        selectInput(
          "select_gran",
          label = "Select granularity",
          choices = c("Day" = "day", "Week" = "week", "Month" = "month"), 
          selected = 1)
      ),
      menuItem(
        checkboxGroupInput(
          "select_var",
          label = "Select variable",
          choices = c(
            "Observations" = "obs",
            "Submeter 1" = "sub1",
            "Submeter 2" = "sub2",
            "Submeter 3" = "sub3",
            "Not submetered" = "nosub",
            "Active power" = "active_sum",
            "Reactive power" = "reactive_sum",
            "Intensity" = "intensity_sum",
            "Voltage" = "voltage_sum"
          ),
          selected = c("obs", "active_sum"))
      )
    )
  ),
  dashboardBody(
    tabItems(
      tabItem(tabName = "datasets", dataTableOutput("df")),
      tabItem(tabName = "graphs", highchartOutput("plot1"))
    )
  )
)

server <- function(input, output){
  source("./dashdata.R")
  
  get.granularity <- reactive({
    aggregated_df[[input$select_gran]]
  })
  
  filtered.data <- reactive({
    get.granularity() %>% select(c("date", input$select_var))
  })
  
  output$df <- renderDataTable({
    # aggregated_df[[input$select_gran]][,input$select_var]
    final.data <- filtered.data()
  })
  
  output$plot1 <- renderHighchart({
    hchart(filtered.data(), "line", hcaes(x="date", y="active_sum"))
  })
}

shinyApp(ui, server)
