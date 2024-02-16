# Test

```{.mermaid loc=img format=svg theme=neutral caption=gantt}
---
title: Gantt Chart of the activities defined for this project.
---
journey
    title Data Journey
    section Manual
      Download Data: 1: Data, Business
      Paste into Excel: 1: Data, Business
      Create Pivot: 2: Business
    section Semi-Automated
      Refresh Data: 2: Data, Business
      Analyse: 3: Data, Business
    section Fully-Automated
```

```{.mermaid loc=img format=svg theme=neutral caption=gantt}
---
title: Gantt Chart of the activities defined for this project.
---
journey
    title Data Journey
      Unavailable: 0
    section Manual
      Downloaded manually: 1: Analyst
      Extracted with script: 2: Scientist
    section Automated
      Extracted with tools: 3: Engineer
      On schedule: 4: Engineer
      Cost Optimised: 5: Cost Engineer
```

```{.mermaid loc=img format=svg theme=neutral caption=gantt}
---
title: Data Journey
---
journey
    section Manual
      Downloaded: 0: Business User
      Extracted with script: 2: Data Scientist
    section Automated
      Extracted with ETL tools: 4: Data Engineer
      On schedule: 6: Data Engineer
      Cost Optimised: 8: Cost Engineer
```
