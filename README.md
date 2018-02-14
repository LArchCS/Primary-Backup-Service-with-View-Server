# Primary-Backup-Service-with-View-Server
1. Primary and Backup servers provide key-value services to client.
2. Primary and Backup servers send heart-beats to view server.
3. View server can promote backup server to primary or any idle server to backup, and updates views.
4. Primary handles direct request from client, and forward put() request to backup.
5. If a new backup is promoted from idle server, primary should forward complete key-value state to backup.
6. Clients keep trying put() and get() requests until success.
7. Both view server and primary backup servers handle network partition and server crash.
8. The system can be scalled up to handle n - 1 faults.
