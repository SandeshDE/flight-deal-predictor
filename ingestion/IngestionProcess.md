## 🔄 Ingestion Process

- **Retrieve Secrets** → from Key Vault  
- **Generate Access Token** → via Web activity  
- **Loop over Dates** → using a `ForEach` loop  
- **Set Dynamic Date** → via `SetVariable` activity  
- **Call Amadeus API** → using `Copy` activity with headers and query params  
- **Write to ADLS** → in JSON format, partitioned by month and year  

![Ingestion Workflow](https://github.com/user-attachments/assets/9e52dad4-7815-4169-9bb5-0576afc0c4a6)


