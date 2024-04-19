```mermaid
---
title: SQS Message Queues and SQS Messages
---

classDiagram
    class ReportArchiveMessage {
        +String: Id
        +String: UserId
        +String: ReportName
        +String: JobPath
        +String: JobUploadPath
    }
    
    note for ReportArchiveJobMessage "Report Archive Queue
    
    (message schema)"
```

```mermaid
flowchart LR

ReportArchiveService_Flow_Chart

Report_Archive_Queue -->|Report Archive Message| Report_Archive_Service
Report_Archive_Service <--->|1. Download Report CSV Files| S3
Report_Archive_Service -->|2. Create Archive File & Upload| S3
Report_Archive_Service -->|3. Delete Report Files - CSVs & Zip| S3
```
